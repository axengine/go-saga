package kafka

import (
	"fmt"
	"github.com/axengine/go-saga/storage"
	"github.com/juju/errors"
	"testing"
	"time"
)

var cfg storage.StorageConfig

func initKafka() {
	cfg.Kafka.ZkAddrs = []string{"192.168.10.32:2181"}
	cfg.Kafka.BrokerAddrs = []string{"192.168.10.32:9092"}
	cfg.Kafka.Partitions = 1
	cfg.Kafka.Replicas = 1
	cfg.Kafka.ReturnDuration = 50 * time.Millisecond
}

func TestLastLog(t *testing.T) {
	initKafka()
	store, err := newKafkaStorage(cfg.Kafka.ZkAddrs,
		cfg.Kafka.BrokerAddrs,
		1, 1, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	logIDs, err := store.LogIDs()
	if err != nil {
		t.Fatal(errors.Annotate(err, "Fetch logs failure"))
	}
	for _, logID := range logIDs {
		lastLogData, err := store.LastLog(logID)
		if err != nil {
			t.Fatal(errors.Annotate(err, "Fetch last log panic"))
		}
		fmt.Println(lastLogData)
	}
}

func TestLookup(t *testing.T) {
	initKafka()
	store, err := newKafkaStorage(cfg.Kafka.ZkAddrs,
		cfg.Kafka.BrokerAddrs,
		1, 1, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	logIDs, err := store.LogIDs()
	if err != nil {
		t.Fatal(errors.Annotate(err, "Fetch logs failure"))
	}
	for _, logID := range logIDs {
		lastLogDatas, err := store.Lookup(logID)
		if err != nil {
			t.Fatal(errors.Annotate(err, "Fetch last log panic"))
		}
		for _, lastLogData := range lastLogDatas {
			fmt.Println(lastLogData)
		}
	}
}
