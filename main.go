package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	ms "github.com/mmcnicol/message-store"
)

// TopicEntry represents a generic topic entry
type TopicEntry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type MockableMessageStore interface {
	SaveEntry(topic string, entry ms.Entry) (int64, error)
	ReadEntry(topic string, offset int64) (*ms.Entry, error)
	PollForNextEntry(topic string, offset int64, pollDuration time.Duration) (*ms.Entry, error)
}

type MessageStoreClient struct {
	MessageStore MockableMessageStore
}

// ProducerHandler handles HTTP requests to send an entry to a message store topic
func (m *MessageStoreClient) ProducerHandler(w http.ResponseWriter, r *http.Request) {
	var entry TopicEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing 'topic' query parameter", http.StatusBadRequest)
		return
	}

	key, err := base64.StdEncoding.DecodeString(entry.Key)
	if err != nil {
		log.Fatal("error:", err)
		http.Error(w, "error when base64 decode Key", http.StatusBadRequest)
	}
	value, err := base64.StdEncoding.DecodeString(entry.Value)
	if err != nil {
		log.Fatal("error:", err)
		http.Error(w, "error when base64 decode Value", http.StatusBadRequest)
	}

	messageStoreEntry := ms.Entry{}
	messageStoreEntry.Key = []byte(key)
	messageStoreEntry.Value = []byte(value)
	messageStoreEntry.Timestamp = entry.Timestamp

	/*
		eventJSON, err := json.Marshal(messageStoreEntry)
		if err != nil {
			http.Error(w, "failed to marshal event", http.StatusInternalServerError)
			return
		}
	*/

	//messageStore := ms.NewMessageStore()
	offset, err := m.MessageStore.SaveEntry(topic, messageStoreEntry)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to save event: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-offset", strconv.FormatInt(offset, 10))
	w.WriteHeader(http.StatusCreated)
}

// ConsumerHandler handles HTTP requests to read an entry from a message store topic
func (m *MessageStoreClient) ConsumerHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing 'topic' query parameter", http.StatusBadRequest)
		return
	}
	offsetString := r.URL.Query().Get("offset")
	if offsetString == "" {
		http.Error(w, "missing 'offset' query parameter", http.StatusBadRequest)
		return
	}
	// Convert string to int64
	offset, err := strconv.ParseInt(offsetString, 10, 64)
	if err != nil {
		http.Error(w, "error parsing offset", http.StatusBadRequest)
		return
	}
	//messageStore := ms.NewMessageStore()
	entry, err := m.MessageStore.ReadEntry(topic, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read events: %v", err), http.StatusInternalServerError)
		return
	}
	if entry == nil {
		http.Error(w, fmt.Sprintf("'offset' not found in topic %s", topic), http.StatusBadRequest)
		return
	}

	key := base64.StdEncoding.EncodeToString([]byte(entry.Key))
	value := base64.StdEncoding.EncodeToString([]byte(entry.Value))

	topicEntry := TopicEntry{}
	topicEntry.Key = key
	topicEntry.Value = value
	topicEntry.Timestamp = entry.Timestamp

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(topicEntry)
}

// PollingConsumerHandler handles HTTP requests to poll for next entry in a message store topic
func (m *MessageStoreClient) PollingConsumerHandler(w http.ResponseWriter, r *http.Request) {

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing 'topic' query parameter", http.StatusBadRequest)
		return
	}

	offsetString := r.URL.Query().Get("offset")
	if offsetString == "" {
		http.Error(w, "missing 'offset' query parameter", http.StatusBadRequest)
		return
	}
	// Convert string to int64
	offset, err := strconv.ParseInt(offsetString, 10, 64)
	if err != nil {
		http.Error(w, "error parsing offset", http.StatusBadRequest)
		return
	}

	pollDurationString := r.URL.Query().Get("pollDuration")
	if offsetString == "" {
		http.Error(w, "missing 'pollDuration' query parameter", http.StatusBadRequest)
		return
	}
	// Parse the duration string to time.Duration
	pollDuration, err := time.ParseDuration(pollDurationString)
	if err != nil {
		http.Error(w, "invalid 'pollDuration' value", http.StatusBadRequest)
		return
	}

	//messageStore := ms.NewMessageStore()
	entry, err := m.MessageStore.PollForNextEntry(topic, offset, pollDuration)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read entry: %v", err), http.StatusInternalServerError)
		return
	}
	if entry == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	key := base64.StdEncoding.EncodeToString([]byte(entry.Key))
	value := base64.StdEncoding.EncodeToString([]byte(entry.Value))

	topicEntry := TopicEntry{}
	topicEntry.Key = key
	topicEntry.Value = value
	topicEntry.Timestamp = entry.Timestamp

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(topicEntry)
}

func main() {

	msgStore := ms.NewMessageStore()
	m := &MessageStoreClient{
		MessageStore: msgStore,
	}

	http.HandleFunc("/produce", m.ProducerHandler)
	http.HandleFunc("/consume", m.ConsumerHandler)
	http.HandleFunc("/poll", m.PollingConsumerHandler)

	log.Println("message store server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
