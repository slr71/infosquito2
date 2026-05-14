package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"

	"github.com/cyverse-de/esutils/v3"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

var (
	// ErrTooManyResults indicates too many results
	ErrTooManyResults = errors.New("Too many results in prefix")
)

// DocumentClassification specifies whether a given document should be updated, reindexed, or nothing
type DocumentClassification int

type CyverseMetadata struct {
	Cyverse []Metadatum `json:"cyverse"`
}

const (
	// NoAction : Take no action
	NoAction DocumentClassification = iota
	// IndexDocument : Index the document
	IndexDocument
	// UpdateDocument : Update the document (probably by reindexing, but still different)
	UpdateDocument
)

type rowMetadata struct {
	rows               int64
	documents          int64
	processed          int64
	dataobjects        int64
	dataobjectsAdded   int64
	dataobjectsUpdated int64
	dataobjectsRemoved int64
	colls              int64
	collsAdded         int64
	collsUpdated       int64
	collsRemoved       int64
	tags               int64
	tagsRemoved        int64
}

func logTime(prefixlog *logrus.Entry, start time.Time, rows *rowMetadata) {
	prefixlog.Infof("Processed %d entries (%d rows, %d documents, processed %d data objects (+%d,U%d,-%d), %d colls (+%d,U%d,-%d)) in %s", rows.processed, rows.rows, rows.documents, rows.dataobjects, rows.dataobjectsAdded, rows.dataobjectsUpdated, rows.dataobjectsRemoved, rows.colls, rows.collsAdded, rows.collsUpdated, rows.collsRemoved, time.Since(start).String())
}

func createBaseUuidsTable(context context.Context, log *logrus.Entry, prefix string, tx *ICATTx) (int64, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "createBaseUuidsTable")
	defer span.End()

	r, err := tx.CreateTemporaryTable(ctx, "base_object_uuids", "SELECT meta.meta_id, lower(meta.meta_attr_value) as id FROM r_meta_main meta WHERE meta.meta_attr_name = 'ipc_UUID' AND meta.meta_attr_value LIKE $1 || '%'", prefix)
	if err != nil {
		return 0, err
	}

	if r > int64(maxInPrefix) {
		return r, ErrTooManyResults
	}

	log.Debugf("Got %d rows for prefix %s (note that this may include stale unused metadata)", r, prefix)
	return r, nil
}

func createUuidsTable(context context.Context, log *logrus.Entry, prefix string, tx *ICATTx) (int64, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "createUuidsTable")
	defer span.End()

	r, err := createBaseUuidsTable(ctx, log, prefix, tx)
	if err != nil {
		return 0, err
	}

	_, err = tx.CreateTemporaryTable(ctx, "object_uuids", "SELECT map.object_id as object_id, meta.id FROM r_objt_metamap map JOIN base_object_uuids meta ON map.meta_id = meta.meta_id")
	if err != nil {
		return 0, err
	}

	return r, nil
}

func createPermsTable(context context.Context, log *logrus.Entry, tx *ICATTx) error {
	ctx, span := otel.Tracer(otelName).Start(context, "createPermsTable")
	defer span.End()

	r, err := tx.CreateTemporaryTable(ctx, "object_perms", `select object_id, json_agg(format('{"user": %s, "permission": %s}', to_json(u.user_name || '#' || u.zone_name), (
                                 CASE a.access_type_id
                                   WHEN 1050 THEN to_json('read'::text)
                                   WHEN 1120 THEN to_json('write'::text)
                                   WHEN 1200 THEN to_json('own'::text)
                                   ELSE 'null'::json
                                 END))::json ORDER BY u.user_name, u.zone_name) AS "userPermissions" from r_objt_access a join r_user_main u on (a.user_id = u.user_id) where a.object_id IN (select object_id from object_uuids) group by object_id`)
	if err != nil {
		return err
	}

	log.Debugf("Got %d rows for perms", r)
	return nil
}

func createMetadataTable(context context.Context, log *logrus.Entry, tx *ICATTx) error {
	ctx, span := otel.Tracer(otelName).Start(context, "createMetadataTable")
	defer span.End()

	r, err := tx.CreateTemporaryTable(ctx, "object_metadata", `select object_id, json_build_object('irods', json_agg(format('{"attribute": %s, "value": %s, "unit": %s}',
                        coalesce(to_json(m2.meta_attr_name), 'null'::json),
                        coalesce(to_json(m2.meta_attr_value), 'null'::json),
                        coalesce(to_json(m2.meta_attr_unit), 'null'::json))::json ORDER BY meta_attr_name, meta_attr_value, meta_attr_unit))
                       AS "metadata" from r_objt_metamap map2 left join r_meta_main m2 on map2.meta_id = m2.meta_id where m2.meta_attr_name <> 'ipc_UUID' and object_id IN (select object_id from object_uuids) group by object_id`)
	if err != nil {
		return err
	}

	log.Debugf("Got %d rows for metadata", r)
	return nil
}

func getSearchResults(context context.Context, log *logrus.Entry, prefix string, es *ESConnection) (int64, map[string]ElasticsearchDocument, map[string]string, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "getSearchResults")
	defer span.End()

	esDocs := make(map[string]ElasticsearchDocument)
	esDocTypes := make(map[string]string)

	prefixQuery := elastic.NewBoolQuery().
		MinimumNumberShouldMatch(1).
		Must(elastic.NewBoolQuery().
			Should(elastic.NewTermQuery("doc_type", "file"),
				elastic.NewTermQuery("doc_type", "folder"))).
		Should(elastic.NewPrefixQuery("id", strings.ToUpper(prefix)),
			elastic.NewPrefixQuery("id", strings.ToLower(prefix)))

	searchService := es.es.Search(es.index).Query(prefixQuery).Sort("id", true).Size(maxInPrefix)
	search, err := searchService.Do(ctx)
	if err != nil {
		return 0, nil, nil, err
	}

	log.Debugf("Got %d documents for prefix %s (ES)", search.TotalHits(), prefix)

	if search.TotalHits() > int64(maxInPrefix) {
		return search.TotalHits(), nil, nil, ErrTooManyResults
	}

	for _, hit := range search.Hits.Hits {
		var doc ElasticsearchDocument

		// json.RawMessage's MarshalJSON can't actually throw an error,
		// it's just matching a function signature
		b, _ := hit.Source.MarshalJSON()
		err := json.Unmarshal(b, &doc)
		if err != nil {
			// if it can't unmarshal the elasticsearch response,
			// may as well just let it reindex the thing as though
			// it's not in ES
			continue
		}

		esDocs[hit.Id] = doc
		esDocTypes[hit.Id] = hit.Type
	}
	return search.TotalHits(), esDocs, esDocTypes, nil
}

func classify(id string, doc ElasticsearchDocument, esDocs map[string]ElasticsearchDocument) DocumentClassification {
	_, ok := esDocs[id]
	if !ok {
		return IndexDocument
	}

	if !doc.Equal(esDocs[id]) {
		return UpdateDocument
	}

	return NoAction
}

func index(indexer *esutils.BulkIndexer, index, id, json string) error {
	req := elastic.NewBulkIndexRequest().Index(index).Id(id).Doc(json)
	// No need to check this error since we're returning
	return indexer.Add(req)
}

// preprocessMetadata takes in the sql.Rows from the DE database and turns it into a map.
func preprocessMetadata(rows *sql.Rows) (map[string]string, error) {
	var err error
	var ret = make(map[string]string)
	for rows.Next() {
		var id, selectedJSON string
		if err = rows.Scan(&id, &selectedJSON); err != nil {
			return ret, err
		}

		ret[id] = selectedJSON
	}
	return ret, nil
}

func processDataobjects(context context.Context, log *logrus.Entry, rows *rowMetadata, avus map[string]string, esDocs map[string]ElasticsearchDocument, seenEsDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection, tx *ICATTx, irodsZone string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "processDataobjects")
	defer span.End()

	dataobjects, err := tx.GetDataObjects(ctx, "object_uuids", "object_perms", "object_metadata", irodsZone)
	if err != nil {
		return err
	}
	defer logIfErr(dataobjects.Close, "closing data-objects rows")
	for dataobjects.Next() {
		var id, selectedJSON string
		if err = dataobjects.Scan(&id, &selectedJSON); err != nil {
			return err
		}

		seenEsDocs[id] = true
		var doc ElasticsearchDocument
		var classification DocumentClassification
		err := json.Unmarshal([]byte(selectedJSON), &doc)
		if err != nil {
			return err
		}

		_, ok := avus[id]
		if ok {
			var cymeta CyverseMetadata
			err := json.Unmarshal([]byte(avus[id]), &cymeta)
			if err != nil {
				return err
			}

			doc.Metadata.Cyverse = cymeta.Cyverse
			log.Debugf("Integrated CyVerse metadata: %+v", doc)
		}

		classification = classify(id, doc, esDocs)

		if classification == UpdateDocument {
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.dataobjectsUpdated++
		} else if classification == IndexDocument {
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.dataobjectsAdded++
		}

		if classification == UpdateDocument || classification == IndexDocument {
			reencode, err := json.Marshal(doc)
			if err != nil {
				return err
			}
			processedJSON := string(reencode)

			if err = index(indexer, es.index, id, processedJSON); err != nil {
				return err
			}
		}

		rows.processed++
		rows.dataobjects++
	}

	log.Debugf("%d data-objects missing, %d data-objects to update", rows.dataobjectsAdded, rows.dataobjectsUpdated)
	return nil
}

func processCollections(context context.Context, log *logrus.Entry, rows *rowMetadata, avus map[string]string, esDocs map[string]ElasticsearchDocument, seenEsDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection, tx *ICATTx, irodsZone string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "processCollections")
	defer span.End()

	colls, err := tx.GetCollections(ctx, "object_uuids", "object_perms", "object_metadata", irodsZone)
	if err != nil {
		return err
	}
	defer logIfErr(colls.Close, "closing collections rows")
	for colls.Next() {
		var id, selectedJSON string
		if err = colls.Scan(&id, &selectedJSON); err != nil {
			return err
		}

		seenEsDocs[id] = true
		var doc ElasticsearchDocument
		var classification DocumentClassification
		err := json.Unmarshal([]byte(selectedJSON), &doc)
		if err != nil {
			return err
		}

		classification = classify(id, doc, esDocs)

		_, ok := avus[id]
		if ok {
			var cymeta CyverseMetadata
			err := json.Unmarshal([]byte(avus[id]), &cymeta)
			if err != nil {
				return err
			}

			doc.Metadata.Cyverse = cymeta.Cyverse
			log.Debugf("Integrated CyVerse metadata: %+v", doc)
		}

		if classification == UpdateDocument {
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.collsUpdated++
		} else if classification == IndexDocument {
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.collsAdded++
		}

		if classification == UpdateDocument || classification == IndexDocument {
			reencode, err := json.Marshal(doc)
			if err != nil {
				return err
			}
			processedJSON := string(reencode)

			if err = index(indexer, es.index, id, processedJSON); err != nil {
				return err
			}
		}

		rows.processed++
		rows.colls++
	}

	log.Debugf("%d collections missing, %d collections to update", rows.collsAdded, rows.collsUpdated)
	return nil
}

func processDeletions(context context.Context, log *logrus.Entry, rows *rowMetadata, esDocs map[string]ElasticsearchDocument, esDocTypes map[string]string, seenEsDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection) error {
	//ctx, span := otel.Tracer(otelName).Start(context, "processDeletions")
	_, span := otel.Tracer(otelName).Start(context, "processDeletions")
	defer span.End()

	for id := range esDocs {
		if !seenEsDocs[id] {
			docType, ok := esDocTypes[id]
			if !ok {
				log.Errorf("Could not find type for document %s, making rash assumptions", id)
				docType = "file"
			}
			if docType == "file" {
				log.Debugf("data-object %s not seen in ICAT, deleting", id)
				rows.dataobjectsRemoved++
			} else if docType == "folder" {
				log.Debugf("collection %s not seen in ICAT, deleting", id)
				rows.collsRemoved++
			}
			req := elastic.NewBulkDeleteRequest().Index(es.index).Id(id)
			err := indexer.Add(req)
			if err != nil {
				return errors.Wrap(err, "Got error adding delete to indexer")
			}
		}
	}

	log.Debugf("%d data-objects to delete, %d collections to delete", rows.dataobjectsRemoved, rows.collsRemoved)
	return nil
}

// ReindexPrefix attempts to reindex a given prefix given a DB and ES connection
func ReindexPrefix(context context.Context, icat *ICATConnection, dedb *DEDBConnection, es *ESConnection, prefix, irodsZone string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "ReindexPrefix")
	defer span.End()

	// SETUP
	var rows rowMetadata

	prefixlog := log.WithFields(logrus.Fields{
		"prefix": prefix,
	})
	prefixlog.Debugf("Indexing prefix %s", prefix)

	start := time.Now()
	defer logTime(prefixlog, start, &rows)

	seenEsDocs := make(map[string]bool)
	docs, esDocs, esDocTypes, err := getSearchResults(ctx, prefixlog, prefix, es)
	rows.documents = docs
	if err != nil {
		return err
	}

	deTx, err := dedb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	deRollback := func() {
		err := deTx.tx.Rollback()
		if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
			prefixlog.Debugf("Failed rolling back DE transaction: %s", err.Error())
		}
	}
	defer deRollback()

	avusRows, err := deTx.GetAVUs(ctx, prefix)
	if err != nil {
		return err
	}
	defer logIfErr(avusRows.Close, "closing AVUs rows (deferred)")

	avus, err := preprocessMetadata(avusRows)
	if err != nil {
		return err
	}
	logIfErr(avusRows.Close, "closing AVUs rows")
	deRollback()

	icatTx, err := icat.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	icatRollback := func() {
		err := icatTx.tx.Rollback()
		if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
			prefixlog.Debugf("Failed rolling back ICAT transaction: %s", err.Error())
		}
	}
	defer icatRollback()

	// COLLECT PREREQUISITES
	r, err := createUuidsTable(ctx, prefixlog, prefix, icatTx)
	rows.rows = r
	if err != nil {
		return err
	}

	if err = createPermsTable(ctx, prefixlog, icatTx); err != nil {
		return err
	}

	if err = createMetadataTable(ctx, prefixlog, icatTx); err != nil {
		return err
	}

	// PROCESS
	indexer := es.NewBulkIndexer(ctx, 1000)
	defer logIfErr(indexer.Flush, "flushing bulk indexer (deferred)")

	if err = processDataobjects(ctx, prefixlog, &rows, avus, esDocs, seenEsDocs, indexer, es, icatTx, irodsZone); err != nil {
		return err
	}

	if err = processCollections(ctx, prefixlog, &rows, avus, esDocs, seenEsDocs, indexer, es, icatTx, irodsZone); err != nil {
		return err
	}

	// Roll back transactions as early as possible
	icatRollback()

	if err = processDeletions(ctx, prefixlog, &rows, esDocs, esDocTypes, seenEsDocs, indexer, es); err != nil {
		return err
	}

	// FINISH UP
	if indexer.CanFlush() {
		err = indexer.Flush()
		if err != nil {
			return errors.Wrap(err, "Got error flushing bulk indexer")
		}
	}

	return nil
}
