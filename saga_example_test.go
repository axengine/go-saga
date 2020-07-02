package saga_test

import (
	"github.com/axengine/go-saga"
	"github.com/axengine/go-saga/storage/kafka"
	"golang.org/x/net/context"
	"log"
	"os"
	"testing"
	"time"
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
	var (
		zkAddrs        = []string{"192.168.10.32:2181"}
		brokerAddrs    = []string{"192.168.10.32:9092"}
		partitions     = 1
		replicas       = 1
		returnDuration = 50 * time.Millisecond
	)

	store, err := kafka.NewKafkaStorage(zkAddrs, brokerAddrs, partitions, replicas, returnDuration, saga.LogPrefix,
		log.New(os.Stdout, "saga", log.LstdFlags))
	if err != nil {
		t.Fatal(err)
	}
	//store, err = memory.NewMemStorage()
	sec := saga.NewSEC(store)

	sec.AddSubTxDef("deduce", DeduceAccount, CompensateDeduce).
		AddSubTxDef("deposit", DepositAccount, CompensateDeposit)

	// 3. Start a saga to transfer 100 from foo to bar.

	from, to := "foo", "bar"
	amount := 100
	ctx := context.Background()

	var sagaID string = "anyid1" //不能有空格??
	err = sec.StartSaga(ctx, sagaID).
		ExecSub("deduce", from, amount).
		ExecSub("deposit", to, amount).
		EndSaga()

	// 4. done.

	if err != nil {
		log.Println("with an error ", err)
	}
}
