package main

import (
	"github.com/cyverse-de/esutils"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"
)

type ESConnection struct {
	es    *elastic.Client
	index string
}

func SetupES(base, user, password, index string) (*ESConnection, error) {
	c, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(base), elastic.SetBasicAuth(user, password))

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create elastic client")
	}

	wait := "10s"
	err = c.WaitForYellowStatus(wait)

	if err != nil {
		return nil, errors.Wrapf(err, "Cluster did not report yellow or better status within %s", wait)
	}

	return &ESConnection{es: c, index: index}, nil
}

func (es *ESConnection) NewBulkIndexer(bulkSize int) *esutils.BulkIndexer {
	return esutils.NewBulkIndexer(es.es, bulkSize)
}

func (es *ESConnection) Close() {
	es.es.Stop()
}
