package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cyverse-de/esutils"
	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

var (
	ErrTooManyResults = errors.New("Too many results in prefix")
)

type DocumentClassification int

const (
	NoAction DocumentClassification = iota
	IndexDocument
	UpdateDocument
)

type rowMetadata struct {
	rows                int64
	documents           int64
	processed           int64
	dataobjects         int64
	dataobjects_added   int64
	dataobjects_updated int64
	dataobjects_removed int64
	colls               int64
	colls_added         int64
	colls_updated       int64
	colls_removed       int64
}

func logTime(prefixlog *logrus.Entry, start time.Time, rows *rowMetadata) {
	prefixlog.Infof("Processed %d entries (%d rows, %d documents, processed %d data objects (+%d,U%d,-%d), %d colls (+%d,U%d,-%d)) in %s", rows.processed, rows.rows, rows.documents, rows.dataobjects, rows.dataobjects_added, rows.dataobjects_updated, rows.dataobjects_removed, rows.colls, rows.colls_added, rows.colls_updated, rows.colls_removed, time.Since(start).String())
}

func createUuidsTable(log *logrus.Entry, prefix string, tx *ICATTx) (int64, error) {
	r, err := tx.CreateTemporaryTable("object_uuids", "SELECT map.object_id as object_id, lower(meta.meta_attr_value) as id FROM r_objt_metamap map JOIN r_meta_main meta ON map.meta_id = meta.meta_id WHERE meta.meta_attr_name = 'ipc_UUID' AND meta.meta_attr_value LIKE $1 || '%'", prefix)
	if err != nil {
		return 0, err
	}

	if r > int64(maxInPrefix) {
		return r, ErrTooManyResults
	}

	log.Debugf("Got %d rows for prefix %s (note that this may include stale unused metadata)", r, prefix)
	return r, nil
}

func createPermsTable(log *logrus.Entry, tx *ICATTx) error {
	r, err := tx.CreateTemporaryTable("object_perms", `select object_id, json_agg(format('{"user": %s, "permission": %s}', to_json(u.user_name || '#' || u.zone_name), (
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

func createMetadataTable(log *logrus.Entry, tx *ICATTx) error {
	r, err := tx.CreateTemporaryTable("object_metadata", `select object_id, json_agg(format('{"attribute": %s, "value": %s, "unit": %s}',
                        coalesce(to_json(m2.meta_attr_name), 'null'::json),
                        coalesce(to_json(m2.meta_attr_value), 'null'::json),
                        coalesce(to_json(m2.meta_attr_unit), 'null'::json))::json ORDER BY meta_attr_name, meta_attr_value, meta_attr_unit)
                       AS "metadata" from r_objt_metamap map2 left join r_meta_main m2 on map2.meta_id = m2.meta_id where m2.meta_attr_name <> 'ipc_UUID' and object_id IN (select object_id from object_uuids) group by object_id`)
	if err != nil {
		return err
	}

	log.Debugf("Got %d rows for metadata", r)
	return nil
}

func getSearchResults(log *logrus.Entry, prefix string, es *ESConnection) (int64, map[string]ElasticsearchDocument, map[string]string, error) {
	esDocs := make(map[string]ElasticsearchDocument)
	esDocTypes := make(map[string]string)

	prefixQuery := elastic.NewBoolQuery().MinimumNumberShouldMatch(1).Should(elastic.NewPrefixQuery("id", strings.ToUpper(prefix)), elastic.NewPrefixQuery("id", strings.ToLower(prefix)))

	searchService := es.es.Search(es.index).Type("file", "folder").Query(prefixQuery).Sort("id", true).Size(maxInPrefix)
	search, err := searchService.Do(context.TODO())
	if err != nil {
		return 0, nil, nil, err
	}

	log.Debugf("Got %d documents for prefix %s (ES)", search.Hits.TotalHits, prefix)

	if search.Hits.TotalHits > int64(maxInPrefix) {
		return search.Hits.TotalHits, nil, nil, ErrTooManyResults
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
	return search.Hits.TotalHits, esDocs, esDocTypes, nil
}

func classify(id, jsonstr string, esDocs map[string]ElasticsearchDocument) (DocumentClassification, error) {
	_, ok := esDocs[id]
	if !ok {
		return IndexDocument, nil
	} else {
		var doc ElasticsearchDocument
		if err := json.Unmarshal([]byte(jsonstr), &doc); err != nil {
			return NoAction, err
		}

		if !doc.Equal(esDocs[id]) {
			return UpdateDocument, nil
		}
	}

	return NoAction, nil
}

func index(indexer *esutils.BulkIndexer, index, id, t, json string) error {
	req := elastic.NewBulkIndexRequest().Index(index).Type(t).Id(id).Doc(json)
	if err := indexer.Add(req); err != nil {
		return err
	}
	return nil
}

func processDataobjects(log *logrus.Entry, rows *rowMetadata, esDocs map[string]ElasticsearchDocument, seenEsDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection, tx *ICATTx) error {
	dataobjects, err := tx.GetDataObjects("object_uuids", "object_perms", "object_metadata")
	if err != nil {
		return err
	}
	defer dataobjects.Close()
	for dataobjects.Next() {
		var id, selectedJson string
		if err = dataobjects.Scan(&id, &selectedJson); err != nil {
			return err
		}

		seenEsDocs[id] = true
		classification, err := classify(id, selectedJson, esDocs)
		if err != nil {
			return err
		}

		if classification == UpdateDocument {
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.dataobjects_updated++
		} else if classification == IndexDocument {
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.dataobjects_added++
		}

		if classification == UpdateDocument || classification == IndexDocument {
			if err = index(indexer, es.index, id, "file", selectedJson); err != nil {
				return err
			}
		}

		rows.processed++
		rows.dataobjects++
	}

	log.Infof("%d data-objects missing, %d data-objects to update", rows.dataobjects_added, rows.dataobjects_updated)
	return nil
}

func processCollections(log *logrus.Entry, rows *rowMetadata, esDocs map[string]ElasticsearchDocument, seenEsDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection, tx *ICATTx) error {
	colls, err := tx.GetCollections("object_uuids", "object_perms", "object_metadata")
	if err != nil {
		return err
	}
	defer colls.Close()
	for colls.Next() {
		var id, selectedJson string
		if err = colls.Scan(&id, &selectedJson); err != nil {
			return err
		}

		seenEsDocs[id] = true
		classification, err := classify(id, selectedJson, esDocs)
		if err != nil {
			return err
		}

		if classification == UpdateDocument {
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.colls_updated++
		} else if classification == IndexDocument {
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.colls_added++
		}

		if classification == UpdateDocument || classification == IndexDocument {
			if err = index(indexer, es.index, id, "folder", selectedJson); err != nil {
				return err
			}
		}

		rows.processed++
		rows.colls++
	}

	log.Infof("%d collections missing, %d collections to update", rows.colls_added, rows.colls_updated)
	return nil
}

func processDeletions(log *logrus.Entry, rows *rowMetadata, esDocs map[string]ElasticsearchDocument, esDocTypes map[string]string, seenEsDocs map[string]bool, indexer *esutils.BulkIndexer, es *ESConnection) error {
	for id, _ := range esDocs {
		if !seenEsDocs[id] {
			docType, ok := esDocTypes[id]
			if !ok {
				log.Errorf("Could not find type for document %s, making rash assumptions", id)
				docType = "file"
			}
			if docType == "file" {
				log.Debugf("data-object %s not seen in ICAT, deleting", id)
				rows.dataobjects_removed++
			} else if docType == "folder" {
				log.Debugf("collection %s not seen in ICAT, deleting", id)
				rows.colls_removed++
			}
			req := elastic.NewBulkDeleteRequest().Index(es.index).Type(docType).Id(id)
			err := indexer.Add(req)
			if err != nil {
				return errors.Wrap(err, "Got error adding delete to indexer")
			}
		}
	}

	log.Infof("%d data-objects to delete, %d collections to delete", rows.dataobjects_removed, rows.colls_removed)
	return nil
}
func ReindexPrefix(db *ICATConnection, es *ESConnection, prefix string) error {
	// SETUP
	var rows rowMetadata

	prefixlog := log.WithFields(logrus.Fields{
		"prefix": prefix,
	})
	prefixlog.Infof("Indexing prefix %s", prefix)

	start := time.Now()
	defer logTime(prefixlog, start, &rows)

	tx, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		return err
	}
	defer tx.tx.Rollback()

	// COLLECT PREREQUISITES
	r, err := createUuidsTable(prefixlog, prefix, tx)
	rows.rows = r
	if err != nil {
		return err
	}

	seenEsDocs := make(map[string]bool)
	docs, esDocs, esDocTypes, err := getSearchResults(prefixlog, prefix, es)
	rows.documents = docs
	if err != nil {
		return err
	}

	if err = createPermsTable(prefixlog, tx); err != nil {
		return err
	}

	if err = createMetadataTable(prefixlog, tx); err != nil {
		return err
	}

	// PROCESS
	indexer := es.NewBulkIndexer(1000)
	defer indexer.Flush()

	if err = processDataobjects(prefixlog, &rows, esDocs, seenEsDocs, indexer, es, tx); err != nil {
		return err
	}

	if err = processCollections(prefixlog, &rows, esDocs, seenEsDocs, indexer, es, tx); err != nil {
		return err
	}

	if err = processDeletions(prefixlog, &rows, esDocs, esDocTypes, seenEsDocs, indexer, es); err != nil {
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