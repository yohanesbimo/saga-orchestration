package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	nsq "github.com/nsqio/go-nsq"
)

const (
	PaymentChannel string = "PaymentChannel"
	OrderChannel   string = "OrderChannel"
	ReplyChannel   string = "ReplyChannel"
	ServicePayment string = "Payment"
	ServiceOrder   string = "Order"
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

type Orchestrator struct {
	p *nsq.Producer
	c *nsq.Consumer
}

func main() {
	var err error

	o := &Orchestrator{}

	// Creates a producer.
	producer, err := nsq.NewProducer("127.0.0.1:4150", nsq.NewConfig())
	if err != nil {
		log.Fatal("Producer: ", err)
	}

	// Creates a consumer.
	consumer, err := nsq.NewConsumer(ReplyChannel, "orchestrator", nsq.NewConfig())
	if err != nil {
		log.Fatal("Consumer: ", err)
	}

	consumer.AddHandler(o)

	err = consumer.ConnectToNSQLookupd("127.0.0.1:4161")
	if err != nil {
		log.Fatal("Connection: ", err)
	}

	o.p = producer
	o.c = consumer

	r := mux.NewRouter()
	r.HandleFunc("/create/{name}/{amount}", o.create)
	log.Println("starting server")

	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8080",
	}

	log.Fatal(srv.ListenAndServe())
}

// create the order - step 1 in the order processing pipeline
func (o Orchestrator) create(writer http.ResponseWriter, request *http.Request) {
	if _, err := fmt.Fprintf(writer, "responding"); err != nil {
		log.Printf("error while writing %s", err.Error())
	}

	vars := mux.Vars(request)

	name := vars["name"]
	amount := vars["amount"]
	val, _ := strconv.Atoi(amount)

	m := Message{
		ID: "1",
		Message: Purchasing{
			Name:   name,
			Amount: val,
		},
		Service: ServiceOrder,
		Action:  ActionStart,
	}
	o.next(OrderChannel, ServiceOrder, m)
}

// start the goroutine to orchestrate the process
func (o Orchestrator) HandleMessage(m *nsq.Message) error {
	var err error

	message := Message{}
	log.Println("MESSAGE:", string(m.Body))
	if err = json.Unmarshal([]byte(m.Body), &message); err != nil {
		log.Println(err)
		// continue to skip bad messages
		return err
	}
	// if there is any error, just rollback
	if message.Action == ActionError {
		log.Printf("Rolling back transaction with id %s", message.ID)
		o.rollback(message)
		return nil
	}

	// else start the next stage
	switch message.Service {
	case ServiceOrder:
		message.Action = ActionStart
		message.Service = ServicePayment
		o.next(PaymentChannel, ServicePayment, message)
	case ServicePayment:
		message.Action = ActionDone
		message.Service = ServiceOrder
		o.next(OrderChannel, ServiceOrder, message)
	}

	return nil
}

// next triggers start operation on the other micro-services
// based on the channel and service
func (o Orchestrator) next(channel, service string, message Message) {
	var err error

	messageBody, _ := json.Marshal(message)

	err = o.p.Publish(channel, messageBody)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Printf("error publishing message to %s channel", channel)
	}

	log.Printf("start message published to channel :%s", channel)
}

// rollback instructs the other micro-services to rollback the transaction
func (o Orchestrator) rollback(m Message) {
	var err error
	message := Message{
		ID:      m.ID, // ID is mandatory
		Action:  ActionRollback,
		Message: m.Message,
	}

	messageBody, _ := json.Marshal(message)

	err = o.p.Publish(OrderChannel, messageBody)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Printf("error publishing rollback message to %s channel", OrderChannel)
	}

	err = o.p.Publish(PaymentChannel, messageBody)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Printf("error publishing rollback message to %s channel", PaymentChannel)
	}
}
