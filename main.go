package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/spf13/viper"

	"github.com/cyverse-de/messaging/v12"
	amqp "github.com/rabbitmq/amqp091-go"

	"go.opentelemetry.io/otel"
)

const otelName = "github.com/cyverse-de/infosquito2"
const serviceName = "infosquito2"
const defaultConfig = `
amqp:
  uri: amqp://guest:guest@rabbit:5672/
  queue_prefix: ""
  dewey_uri: amqp://guest:guest@rabbit:5672/
  dewey_queue: "dewey.indexing"

irods:
  zone: iplant

infosquito:
  maximum_in_prefix: 10000
  base_prefix_length: 3

elasticsearch:
  base: http://elasticsearch:9200
  index: data

icat:
  uri: postgres://ICAT:fakepassword@icat-db:5432/ICAT?sslmode=disable

db:
  uri: postgres://de:fakepassword@de-db:5432/metadata?sslmode=disable
  schema: public
`

const prefixRoutingKey string = "index.data.prefix"
const prefixRoutingKeyLen int = len(prefixRoutingKey)

var log = logrus.WithFields(logrus.Fields{
	"service": serviceName,
	"art-id":  serviceName,
	"group":   "org.cyverse",
})

var (
	cfgPath = flag.String("config", "", "Path to the configuration file.")
	mode    = flag.String("mode", "", "One of 'periodic' or 'full'.")
	debug   = flag.Bool("debug", false, "Set to true to enable debug logging")
	cfg     *viper.Viper

	amqpURI          string
	amqpDeweyURI     string
	amqpExchangeName string
	amqpExchangeType string
	amqpQueuePrefix  string
	amqpDeweyQueue   string

	elasticsearchBase     string
	elasticsearchUser     string
	elasticsearchPassword string
	elasticsearchIndex    string

	irodsZone string

	ICATURI  string
	dbURI    string
	dbSchema string

	maxInPrefix      int
	basePrefixLength int
)

func initFlags() {
	flag.Parse()
	logrus.SetFormatter(&logrus.JSONFormatter{})
	if !(*debug) {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.DebugLevel)
	}
}

func spin() {
	spinner := make(chan int)
	<-spinner
}

func checkMode() {
	if *mode != "periodic" && *mode != "full" {
		fmt.Printf("Invalid mode: %s\n", *mode)
		flag.PrintDefaults()
		os.Exit(-1)
	}
}

func initConfig(cfgPath string) {
	var err error
	cfg, err = configurate.InitDefaults(cfgPath, defaultConfig)
	if err != nil {
		log.Fatalf("Unable to initialize the default configuration settings: %s", err)
	}

	ICATURI = cfg.GetString("icat.uri")
	dbURI = cfg.GetString("db.uri")
	dbSchema = cfg.GetString("db.schema")

	elasticsearchBase = cfg.GetString("elasticsearch.base")
	elasticsearchUser = cfg.GetString("elasticsearch.user")
	elasticsearchPassword = cfg.GetString("elasticsearch.password")
	elasticsearchIndex = cfg.GetString("elasticsearch.index")
	irodsZone = cfg.GetString("irods.zone")
	max, err := strconv.Atoi(cfg.GetString("infosquito.maximum_in_prefix"))
	if err != nil {
		log.Fatal("Couldn't parse integer out of infosquito.maximum_in_prefix")
	}
	maxInPrefix = max

	base, err := strconv.Atoi(cfg.GetString("infosquito.base_prefix_length"))
	if err != nil {
		log.Fatal("Couldn't parse integer out of infosquito.base_prefix_length")
	}
	basePrefixLength = base
}

func loadAMQPConfig() {
	amqpURI = cfg.GetString("amqp.uri")
	amqpExchangeName = cfg.GetString("amqp.exchange.name")
	amqpExchangeType = cfg.GetString("amqp.exchange.type")
	amqpQueuePrefix = cfg.GetString("amqp.queue_prefix")

	amqpDeweyURI = cfg.GetString("amqp.dewey_uri")
	amqpDeweyQueue = cfg.GetString("amqp.dewey_queue")
}

func getQueueName(prefix string) string {
	if len(prefix) > 0 {
		return fmt.Sprintf("%s.%s", prefix, serviceName)
	}
	return serviceName
}

func generatePrefixes(length int) []string {
	prefixes := int(math.Pow(16, float64(length)))
	res := make([]string, prefixes)
	for i := 0; i < prefixes; i++ {
		res[i] = fmt.Sprintf("%0"+strconv.Itoa(length)+"x", i)
	}
	return res
}

func splitPrefix(prefix string) []string {
	res := make([]string, 16)
	for i := 0; i < 16; i++ {
		res[i] = fmt.Sprintf("%s%x", prefix, i)
	}
	return res
}

func tryReindexPrefix(context context.Context, icat *ICATConnection, dedb *DEDBConnection, es *ESConnection, prefix, irodsZone string) error {
	err := ReindexPrefix(context, icat, dedb, es, prefix, irodsZone)
	if err == ErrTooManyResults {
		for _, newprefix := range splitPrefix(prefix) {
			err = tryReindexPrefix(context, icat, dedb, es, newprefix, irodsZone)
			if err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}
	return nil
}

func publishPrefixMessages(context context.Context, prefixes []string, client *messaging.Client, del amqp.Delivery) error {
	log.Infof("Publishing %d prefix messages", len(prefixes))
	for _, prefix := range prefixes {
		err := client.PublishContext(context, fmt.Sprintf("%s.%s", prefixRoutingKey, prefix), []byte{})
		if err != nil {
			rejectErr := del.Reject(!del.Redelivered)
			if rejectErr != nil {
				log.Error(errors.Wrap(rejectErr, "Failed rejecting the index message after failing to publish prefix messages"))
			}
			return err
		}
	}
	return nil
}

func handleIndex(context context.Context, del amqp.Delivery, publishClient *messaging.Client, deweyClient *messaging.Client) error {
	ctx, span := otel.Tracer(otelName).Start(context, "handleIndex")
	defer span.End()

	// reindex tags
	err := publishClient.PublishContext(context, "index.tags", []byte{})
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to send tag index message"))
	}

	log.Infof("Purging dewey queue %s", amqpDeweyQueue)
	err = deweyClient.PurgeQueue(amqpDeweyQueue)
	if err != nil {
		log.Error(errors.Wrap(err, "Failed purging dewey queue"))
	}
	return publishPrefixMessages(ctx, generatePrefixes(basePrefixLength), publishClient, del)
}

func handlePrefix(context context.Context, del amqp.Delivery, icat *ICATConnection, dedb *DEDBConnection, es *ESConnection, publishClient *messaging.Client) error {
	ctx, span := otel.Tracer(otelName).Start(context, "handlePrefix")
	defer span.End()

	prefix := del.RoutingKey[prefixRoutingKeyLen+1:]
	log.Debugf("Triggered reindexing prefix %s", prefix)
	err := ReindexPrefix(ctx, icat, dedb, es, prefix, irodsZone)
	if err == ErrTooManyResults {
		log.Infof("Prefix %s too large, splitting", prefix)
		return publishPrefixMessages(ctx, splitPrefix(prefix), publishClient, del)
	} else if err != nil {
		log.Errorf("Error reindexing prefix %s: %s", prefix, err)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(errors.Wrap(rejectErr, "Failed rejecting message after failing to reindex prefix"))
		}
		return err
	}

	return nil
}

func handleTags(context context.Context, del amqp.Delivery, db *DEDBConnection, es *ESConnection) error {
	ctx, span := otel.Tracer(otelName).Start(context, "handleTags")
	defer span.End()

	// XXX: reject messages on error
	return ReindexTags(ctx, db, es, irodsZone)
}

func main() {
	initFlags()

	checkMode()
	initConfig(*cfgPath)

	var tracerCtx, cancel = context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	icat, err := SetupICAT(ICATURI)
	if err != nil {
		log.Fatalf("Unable to set up the ICAT database: %s", err)
	}

	db, err := SetupDEDB(dbURI, dbSchema)
	if err != nil {
		log.Fatalf("Unable to set up the DE database: %s", err)
	}

	es, err := SetupES(elasticsearchBase, elasticsearchUser, elasticsearchPassword, elasticsearchIndex)
	if err != nil {
		log.Fatalf("Unable to set up the ElasticSearch connection: %s", err)
	}

	if *mode == "full" {
		log.Info("Full indexing mode selected.")
		// do full mode
		err = ReindexTags(context.Background(), db, es, irodsZone)
		if err != nil {
			log.Fatalf("Full indexing (tags) failed: %s", err)
		}
		for _, prefix := range generatePrefixes(basePrefixLength) {
			log.Infof("Reindexing prefix %s", prefix)
			err = tryReindexPrefix(context.Background(), icat, db, es, prefix, irodsZone)
			if err != nil {
				log.Fatalf("Full reindexing failed: %s", err)
			}
		}
		return
	}

	// periodic mode
	log.Info("Periodic indexing mode selected.")
	loadAMQPConfig()

	listenClient, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatalf("Unable to create the messaging listen client: %s", err)
	}
	defer listenClient.Close()

	publishClient, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatalf("Unable to create the messaging publish client: %s", err)
	}
	defer publishClient.Close()

	err = publishClient.SetupPublishing(amqpExchangeName)
	if err != nil {
		log.Fatalf("Unable to set up message publishing: %s", err)
	}

	deweyClient, err := messaging.NewClient(amqpDeweyURI, true)
	if err != nil {
		log.Fatalf("Unable to create the messaging dewey client: %s", err)
	}
	defer deweyClient.Close()

	go listenClient.Listen()

	queueName := getQueueName(amqpQueuePrefix)
	listenClient.AddConsumerMulti(
		amqpExchangeName,
		amqpExchangeType,
		queueName,
		[]string{"index.all", "index.data", "index.tags", fmt.Sprintf("%s.#", prefixRoutingKey)},
		func(context context.Context, del amqp.Delivery) {
			var err error
			log.Debugf("Got message %s", del.RoutingKey)
			if del.RoutingKey == "index.all" || del.RoutingKey == "index.data" {
				// send prefix messages and an index.tags message
				// this means index.data will also index tags but that's probably fine
				err = handleIndex(context, del, publishClient, deweyClient)
			} else if del.RoutingKey == "index.tags" {
				err = handleTags(context, del, db, es)
			} else if strings.HasPrefix(del.RoutingKey, prefixRoutingKey) {
				err = handlePrefix(context, del, icat, db, es, publishClient)
			} else {
				log.Errorf("Got unknown routing key %s", del.RoutingKey)
			}
			if err != nil {
				return
			}
			err = del.Ack(false)
			if err != nil {
				log.Error(errors.Wrap(err, "Failed acknowledging message"))
			}
		},
		1)

	spin()
}
