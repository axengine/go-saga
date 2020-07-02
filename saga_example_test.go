package saga_test

import (
	"golang.org/x/net/context"
	"log"
	"testing"
	"time"

	"github.com/axengine/go-saga"
	_ "github.com/axengine/go-saga/storage/kafka"
)

// This example show how to initialize an Saga execution coordinator(SEC) and add Sub-transaction to it, then start a transfer transaction.
// In transfer transaction we deduce `100` from foo at first, then deposit 100 into `bar`, deduce & deduce wil both success or rollbacked.
func Test_Example_sagaTransaction(t *testing.T) {

	// 1. Define sub-transaction method, anonymous method is NOT required, Just define them as normal way.
	DeduceAccount := func(ctx context.Context, account string, amount int) error {
		// Do deduce amount from account, like: account.money - amount
		return nil
	}
	CompensateDeduce := func(ctx context.Context, account string, amount int) error {
		// Compensate deduce amount to account, like: account.money + amount
		return nil
	}
	DepositAccount := func(ctx context.Context, account string, amount int) error {
		// Do deposit amount to account, like: account.money + amount
		return nil
	}
	CompensateDeposit := func(ctx context.Context, account string, amount int) error {
		// Compensate deposit amount from account, like: account.money - amount
		return nil
	}

	// 2. Init SEC as global SINGLETON(this demo not..), and add Sub-transaction definition into SEC.
	saga.StorageConfig.Kafka.ZkAddrs = []string{"192.168.10.32:2181"}
	saga.StorageConfig.Kafka.BrokerAddrs = []string{"192.168.10.32:9092"}
	saga.StorageConfig.Kafka.Partitions = 1
	saga.StorageConfig.Kafka.Replicas = 1
	saga.StorageConfig.Kafka.ReturnDuration = 50 * time.Millisecond

	saga.AddSubTxDef("deduce", DeduceAccount, CompensateDeduce).
		AddSubTxDef("deposit", DepositAccount, CompensateDeposit)

	// 3. Start a saga to transfer 100 from foo to bar.

	from, to := "foo", "bar"
	amount := 100
	ctx := context.Background()

	var sagaID string = "anyid1" //不能有空格??
	err := saga.StartSaga(ctx, sagaID).
		ExecSub("deduce", from, amount).
		ExecSub("deposit", to, amount).
		EndSaga()

	// 4. done.

	if err != nil {
		log.Println("with an error ", err)
	}
}
