package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cyverse-de/esutils/v3"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

type ElasticsearchTag struct {
	DocType      string `json:"doc_type"`
	ID           string `json:"id"`
	Value        string `json:"value"`
	Description  string `json:"description"`
	Creator      string `json:"creator"`
	FileType     string `json:"fileType"`
	DateCreated  int64  `json:"dateCreated"`
	DateModified int64  `json:"dateModified"`
}

func logTagTime(prefixlog *logrus.Entry, start time.Time, rows *rowMetadata) {
	prefixlog.Infof("Processed %d entries (%d rows, %d documents, %d tags indexed, %d tags removed) in %s", rows.processed, rows.rows, rows.documents, rows.tags, rows.tagsRemoved, time.Since(start).String())
}

func getIndexedTags(context context.Context, es *ESConnection) (int64, map[string]ElasticsearchTag, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "getIndexedTags")
	defer span.End()

	docs := make(map[string]ElasticsearchTag)

	query := elastic.NewBoolQuery().
		Must(elastic.NewTermQuery("doc_type", "tag"))

	searchService := es.es.Search(es.index).Query(query).Sort("id", true)
	search, err := searchService.Do(ctx)
	if err != nil {
		return 0, nil, err
	}

	for _, hit := range search.Hits.Hits {
		var doc ElasticsearchTag
		b, _ := hit.Source.MarshalJSON()
		err := json.Unmarshal(b, &doc)
		if err != nil {
			// if it broke, just reindex the thing
			continue
		}

		docs[hit.Id] = doc
	}
	return search.TotalHits(), docs, nil
}

func processTags(context context.Context, log *logrus.Entry, rows *rowMetadata, seenDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection, tx *DEDBTx, irodsZone string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "processTags")
	defer span.End()

	tags, err := tx.GetTags(ctx, irodsZone)
	if err != nil {
		return errors.Wrap(err, "Error fetching tags")
	}
	defer logIfErr(tags.Close, "closing tags rows")
	for tags.Next() {
		var id, selectedJSON string
		if err = tags.Scan(&id, &selectedJSON); err != nil {
			return errors.Wrap(err, "Error scanning row")
		}

		seenDocs[id] = true
		if err = index(indexer, es.index, id, selectedJSON); err != nil {
			return err
		}

		rows.processed++
		rows.tags++
	}
	return nil
}

func processTagDeletions(context context.Context, log *logrus.Entry, rows *rowMetadata, esDocs map[string]ElasticsearchTag, seenDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection) error {
	_, span := otel.Tracer(otelName).Start(context, "processTagDeletions")
	defer span.End()

	for id := range esDocs {
		if !seenDocs[id] {
			rows.tagsRemoved++
			req := elastic.NewBulkDeleteRequest().Index(es.index).Id(id)
			err := indexer.Add(req)
			if err != nil {
				return errors.Wrap(err, "Got error adding delete to indexer")
			}
		}
	}

	return nil
}

// ReindexTags attempts to reindex tags given a DB and ES connection
func ReindexTags(context context.Context, db *DEDBConnection, es *ESConnection, irodsZone string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "ReindexTags")
	defer span.End()

	var rows rowMetadata

	taglog := log.WithFields(logrus.Fields{
		"operation": "indexTags",
	})
	taglog.Debug("Indexing tags")

	start := time.Now()
	defer logTagTime(taglog, start, &rows)

	// Get existing stuff from ES
	seenDocs := make(map[string]bool)
	docs, esDocs, err := getIndexedTags(ctx, es)
	rows.documents = docs
	if err != nil {
		return errors.Wrap(err, "Error fetching indexed tags")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "Error starting transaction")
	}
	rb := func() {
		err := tx.tx.Rollback()
		if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
			taglog.Debugf("Failed rolling back transaction: %s", err.Error())
		}
	}
	defer rb()

	// Index tags that exist (just do them all, don't worry about classifying updates/deletes or skipping unchanged)
	indexer := es.NewBulkIndexer(ctx, 1000)
	defer logIfErr(indexer.Flush, "flushing tags bulk indexer (deferred)")

	if err = processTags(ctx, taglog, &rows, seenDocs, indexer, es, tx, irodsZone); err != nil {
		return errors.Wrap(err, "Error processing tags")
	}

	rb() // roll back before doing deletions to release DB locks

	// Delete tags that didn't appear in the database query
	if err = processTagDeletions(ctx, taglog, &rows, esDocs, seenDocs, indexer, es); err != nil {
		return errors.Wrap(err, "Error deleting tags")
	}

	if indexer.CanFlush() {
		err = indexer.Flush()
		if err != nil {
			return errors.Wrap(err, "Got error flushing bulk indexer")
		}
	}

	return nil
}
