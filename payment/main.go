package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/nsqio/go-nsq"
)

const (
	PaymentChannel string = "PaymentChannel"
	ReplyChannel   string = "ReplyChannel"
	ActionStart    string = "Start"
	ActionDone     string = "DoneMsg"
	ActionError    string = "ErrorMsg"
	ActionRollback string = "RollbackMsg"
)

type Purchasing struct {
	Name   string `json:"name"`
	Amount int    `json:"amount"`
}

// Message represents the payload sent over redis pub/sub
type Message struct {
	ID      string     `json:"id"`
	Service string     `json:"service"`
	Action  string     `json:"action"`
	Message Purchasing `json:"message"`
}

type Payment struct {
	p *nsq.Producer
	c *nsq.Consumer
}

var balance = 0

func main() {
	p := &Payment{}

	// Creates a producer.
	producer, err := nsq.NewProducer("127.0.0.1:4150", nsq.NewConfig())
	if err != nil {
		log.Fatal("Producer: ", err)
	}

	// Creates a consumer.
	consumer, err := nsq.NewConsumer(PaymentChannel, "payment", nsq.NewConfig())
	if err != nil {
		log.Fatal("Consumer: ", err)
	}

	consumer.AddHandler(p)

	err = consumer.ConnectToNSQLookupd("127.0.0.1:4161")
	if err != nil {
		log.Fatal("Connection: ", err)
	}

	p.p = producer
	p.c = consumer

	r := mux.NewRouter()
	r.HandleFunc("/payment/{orderId}/{amount}", payment)
	r.HandleFunc("/get-balance", getBalance)
	log.Println("starting server")

	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8082",
	}

	log.Fatal(srv.ListenAndServe())
}

func (p Payment) HandleMessage(message *nsq.Message) error {
	m := Message{}
	err := json.Unmarshal(message.Body, &m)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Printf("recieved message with id %s ", m.ID)

	// Happy Flow
	if m.Action == ActionStart {
		m.Action = ActionDone

		balance = balance + m.Message.Amount

		messageBody, _ := json.Marshal(m)
		err = p.p.Publish(ReplyChannel, messageBody)
		if err != nil {
			log.Printf("error publishing done-message to %s channel", ReplyChannel)
			return err
		}
		log.Printf("done message published to channel :%s", ReplyChannel)
	}

	// Rollback flow
	if m.Action == ActionRollback {
		balance = balance - m.Message.Amount
		log.Printf("rolling back transaction with ID :%s", m.ID)
	}

	return nil
}

func payment(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	amount := vars["amount"]

	v, _ := strconv.Atoi(amount)
	balance = balance + v

	w.WriteHeader(http.StatusOK)
}

func getBalance(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf(`{"balance":%v}`, balance)))
	// fmt.Println(balance)
}
