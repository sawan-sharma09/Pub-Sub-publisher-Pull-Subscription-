package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gorilla/mux"
)

var (
	ProjectID = "learned-acolyte-380510"
	ctx       = context.Background()
)

type Pubsub_Data struct {
	Amount           int64     `json:"amount"`
	TransactionId    int64     `json:"txnid"`
	Transaction_Type string    `json:"txn_type"`
	UserId           int64     `json:"userid"`
	Is_sl            bool      `json:"is_sl"`
	Is_Bank          bool      `json:"is_bank"`
	Bank_Name        string    `json:"bank_name"`
	Created_Data     time.Time `json:"created_date"`
	// Updated_Date     time.Time `json:"updated_time"`
}

type Message struct {
	Key string       `json:"key"`
	Msg *Pubsub_Data `json:"message"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/mypubsub", Mytopic)
	r.HandleFunc("/myadvpubsub", AdvTopic).Methods(http.MethodPost)
	fmt.Println("Connected...")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func AdvTopic(w http.ResponseWriter, r *http.Request) {
	var pubsubData Pubsub_Data
	if err := json.NewDecoder(r.Body).Decode(&pubsubData); err != nil {
		log.Fatal("Decode error..", err)
	}
	fmt.Printf("Client data--> %+v\n", pubsubData)
	pubsubData.Created_Data = time.Now()

	m := Message{
		Key: "Commission",
		Msg: &pubsubData,
	}
	jsondata, err2 := json.Marshal(&m)
	if err2 != nil {
		log.Fatal("Error in marshalling pubsub data..", err2)
	}
	fmt.Println("Pubsub req data-->", string(jsondata))

	client, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatal("Error in creating client..", err)
	}
	topic := client.Topic("My_Push_pub_sub_topic")
	res := topic.Publish(ctx, &pubsub.Message{
		Data: jsondata,
	})
	defer topic.Stop()

	ID, err := res.Get(ctx)
	if err != nil {
		log.Fatal("Id generation error from pubsub..", err)
	}
	fmt.Println("Message has been published, please check Pub/Sub..")
	fmt.Fprintf(w, "Published message with msg ID: %v\n", ID)

}
func Mytopic(w http.ResponseWriter, r *http.Request) {

	projectID := "learned-acolyte-380510"
	// topicId := "projects/learned-acolyte-380510/topics/First_test"

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal("Error in creating client..", err)
	}
	topic := client.Topic("First_Client")
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte("3rd message..."),
	})

	defer topic.Stop()

	ID, err := res.Get(ctx)
	if err != nil {
		log.Fatal("Id generation error from pubsub..", err)
	}
	fmt.Fprintf(w, "Published message with msg ID: %v\n", ID)

}
