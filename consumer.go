package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"log"
	"runtime"
	"time"
)

func startConsumer(kafkaAddress []string, kafkaTopics []string, consumerGroup string) {
	log.Print("Starting consumer")

	config := sarama.NewConfig()

	group, err := sarama.NewConsumerGroup(
		kafkaAddress,
		consumerGroup,
		config,
	)
	if err != nil {
		log.Fatalln("Fail to create consumer group", err)
	}
	defer func() {
		if err := group.Close(); err != nil {
			log.Printf("Fail while closing producer group: %v", err)
		}
	}()

	ctx := context.Background()
	bi, err := createEsBulkInder()
	if err != nil {
		panic(1)
	}
	for {
		handler := myConsumer{
			Indexer: bi,
		}
		err := group.Consume(ctx, kafkaTopics, handler)
		if err != nil {
			log.Fatalln("Fail to consume from consumer group", err)
		}
	}
}

func createEsBulkInder() (esutil.BulkIndexer, error) {
	retryBackoff := backoff.NewExponentialBackOff()
	config := elasticsearch.Config{
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: 5,
	}
	client, err := elasticsearch.NewClient(config)
	if err != nil {
		log.Printf("Fail to connecto to ES: %v", err)
		return nil, err
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "test-bulk",
		Client:        client,
		NumWorkers:    runtime.NumCPU(),
		FlushBytes:    int(5e+6),
		FlushInterval: 30 * time.Second,
	})
	if err != nil {
		log.Printf("Fail to create bulk index to ES: %v", err)
		return nil, err
	}
	return bi, nil
}

func olderConsumer(kafkaAddress []string, kafkaTopics []string) {
	consumer, err := sarama.NewConsumer(kafkaAddress, nil)
	if err != nil {
		log.Fatalln("Fail to start consumer", err)
	}

	for _, topic := range kafkaTopics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			log.Fatalln("Fail to retrieve partitions", err)
		}

		for partitionIndex, partition := range partitions {
			log.Printf("Partintion index: %v\n", partitionIndex)

			pc, err := consumer.ConsumePartition(
				topic,
				partition,
				sarama.OffsetOldest,
			)
			if err != nil {
				log.Fatalln("Fail to start consumer to partition", err)
			}
			go func(pc sarama.PartitionConsumer) {
				for message := range pc.Messages() {
					log.Printf("Recieve message %v - %v\n", string(message.Key), string(message.Value))
				}
			}(pc)
		}
	}

	for {
		time.Sleep(time.Second * 30)
		log.Println("Waiting messages")
	}
}

type myConsumer struct {
	Indexer esutil.BulkIndexer
}

func (myConsumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}
func (h myConsumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	if err := h.Indexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error when closing index bulker: %s", err)
		return err
	}
	return nil
}
func (h myConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var message Message
		err := json.Unmarshal(msg.Value, &message)
		if err != nil {
			log.Printf("Fail unmarshaling message: %v\n", err)
			continue
		}
		log.Printf("%+v", message)
		log.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)

		for i := 0; i <= 10; i++ {
			document := Message{
				Id:         fmt.Sprintf("%s_%d", message.Id, i),
				Date:       message.Date,
				CustomerId: message.CustomerId,
				Type:       fmt.Sprintf("%s_%d", message.Type, i),
				Value:      message.Value,
			}
			documentData, err := json.Marshal(document)
			if err != nil {
				log.Printf("Fail to encode document to json: %v", err)
				continue
			}

			err = h.Indexer.Add(
				context.Background(),
				esutil.BulkIndexerItem{
					Action:     "index",
					DocumentID: document.Id,
					Body:       bytes.NewReader(documentData),
					OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						fmt.Printf("Document with id %v inserted\n", item.DocumentID)
					},
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						if err != nil {
							log.Printf("ERROR for message id %v %s", item.DocumentID, err)
						} else {
							log.Printf("ERROR for message id %v: %s: %s", item.DocumentID, res.Error.Type, res.Error.Reason)
						}
					},
				},
			)

			if err != nil {
				log.Printf("Fail to add document to ES bulk: %v", err)
				continue
			}

		}

		sess.MarkMessage(msg, "")
	}
	return nil
}
