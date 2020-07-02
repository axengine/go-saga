package kafka

import (
	"fmt"
	"github.com/juju/errors"
	"log"
	"testing"
	"time"
)

var (
	zkAddrs        = []string{"192.168.10.32:2181"}
	brokerAddrs    = []string{"192.168.10.32:9092"}
	partitions     = 1
	replicas       = 1
	returnDuration = 50 * time.Millisecond
)

func TestLastLog(t *testing.T) {
	store, err := NewKafkaStorage(zkAddrs,
		brokerAddrs,
		partitions, replicas, returnDuration, "saga_", &log.Logger{})
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
	store, err := NewKafkaStorage(zkAddrs,
		brokerAddrs,
		partitions, replicas, returnDuration, "saga_", &log.Logger{})
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
