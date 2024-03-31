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

// producerHandler handles HTTP requests to send an entry to a message store topic
func producerHandler(w http.ResponseWriter, r *http.Request) {
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

	messageStore := ms.NewMessageStore()
	offset, err := messageStore.SaveEntry(topic, messageStoreEntry)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to save event: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-offset", strconv.FormatInt(offset, 10))
	w.WriteHeader(http.StatusCreated)
}

// consumerHandler handles HTTP requests to read an entry from a message store topic
func consumerHandler(w http.ResponseWriter, r *http.Request) {
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
	messageStore := ms.NewMessageStore()
	entry, err := messageStore.ReadEntry(topic, offset)
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

// pollHandler handles HTTP requests to poll for next entry in a message store topic
func pollHandler(w http.ResponseWriter, r *http.Request) {

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

	messageStore := ms.NewMessageStore()
	entry, err := messageStore.PollForNextEntry(topic, offset, pollDuration)
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

	http.HandleFunc("/produce", producerHandler)
	http.HandleFunc("/consume", consumerHandler)
	http.HandleFunc("/poll", pollHandler)

	log.Println("message store server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
