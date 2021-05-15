package subscriber

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	pb "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	gravity_transmitter "github.com/BrobridgeOrg/gravity-sdk/transmitter"
	"github.com/BrobridgeOrg/gravity-transmitter-postgres/pkg/app"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Subscriber struct {
	app        app.App
	subscriber *gravity_transmitter.Subscriber
	ruleConfig *RuleConfig
}

func NewSubscriber(a app.App) *Subscriber {
	return &Subscriber{
		app: a,
	}
}

func (subscriber *Subscriber) LoadConfigFile(filename string) (*RuleConfig, error) {

	// Open and read config file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Parse config
	var config RuleConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (subscriber *Subscriber) Init() error {

	// Load rules
	ruleFile := viper.GetString("rules.subscription")

	log.WithFields(log.Fields{
		"ruleFile": ruleFile,
	}).Info("Loading rules...")

	ruleConfig, err := subscriber.LoadConfigFile(ruleFile)
	if err != nil {
		return err
	}

	subscriber.ruleConfig = ruleConfig

	host := viper.GetString("gravity.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing gravity subscriber")

	// Initializing gravity subscriber and connecting to server
	options := gravity_transmitter.NewOptions()
	options.Verbose = true
	subscriber.subscriber = gravity_transmitter.NewSubscriber(options)
	opts := core.NewOptions()
	err = subscriber.subscriber.Connect(host, opts)
	if err != nil {
		return err
	}

	// Register subscriber
	log.Info("Registering subscriber")
	subscriberID := viper.GetString("gravity.subscriber_id")
	err = subscriber.subscriber.Register(subscriberID)
	if err != nil {
		return err
	}

	// Subscribe to collections
	collections := make([]string, 0, len(subscriber.ruleConfig.Subscriptions))
	for col, _ := range subscriber.ruleConfig.Subscriptions {
		log.WithFields(log.Fields{
			"collection": col,
		}).Info("Subscribe to collection")
		collections = append(collections, col)
	}

	if len(collections) > 0 {
		err = subscriber.subscriber.AddCollections(collections)
		if err != nil {
			return err
		}
	}

	// Table mapping
	subscriber.subscriber.SetCollectionMap(subscriber.ruleConfig.Subscriptions)

	return nil
}

func (subscriber *Subscriber) Run() error {

	log.WithFields(log.Fields{}).Info("Subscribing to gravity pipelines...")
	err := subscriber.subscriber.AddAllPipelines()
	if err != nil {
		log.Error(err)
		return err
	}

	writer := subscriber.app.GetWriter()
	log.WithFields(log.Fields{}).Info("Starting to fetch data from gravity...")
	_, err = subscriber.subscriber.Subscribe(func(record *pb.Record) {

		for {
			err := writer.ProcessData(record)
			if err == nil {
				break
			}

			<-time.After(time.Second * 5)
		}
	})
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}
