package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	ms "github.com/mmcnicol/message-store"
)

// MockMessageStore is a mock implementation of MessageStore interface for testing
type MockMessageStore struct{}

func (m *MockMessageStore) SaveEntry(topic string, entry ms.Entry) (int64, error) {
	// Mock implementation for SaveEntry
	return 12345, nil // Sample offset value
}

func (m *MockMessageStore) ReadEntry(topic string, offset int64) (*ms.Entry, error) {
	// Mock implementation for ReadEntry
	entry := &ms.Entry{
		Key:   []byte("12345"),
		Value: []byte("test"),
	}
	return entry, nil // Mock entry
}

func (m *MockMessageStore) PollForNextEntry(topic string, offset int64, pollDuration time.Duration) (*ms.Entry, error) {
	// Mock implementation for PollForNextEntry
	entry := &ms.Entry{
		Key:   []byte("12345"),
		Value: []byte("test"),
	}
	return entry, nil // Mock entry
}

func TestProducerHandler(t *testing.T) {
	mockStore := &MockMessageStore{}
	client := &MessageStoreClient{
		MessageStore: mockStore,
	}

	entry := TopicEntry{
		Key:       "aGVsbG8=",
		Value:     "d29ybGQ=",
		Timestamp: time.Now(),
	}
	body, _ := json.Marshal(entry)

	req, err := http.NewRequest("POST", "/produce?topic=test", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()

	client.ProducerHandler(recorder, req)

	// Check the status code is what you expect
	if status := recorder.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusCreated)
	}

	assert.Equal(t, "12345", recorder.Header().Get("x-offset"))
}

func TestConsumerHandler(t *testing.T) {
	mockStore := &MockMessageStore{}
	client := &MessageStoreClient{
		MessageStore: mockStore,
	}

	req, err := http.NewRequest("GET", "/consume?topic=test&offset=123", nil)
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()

	client.ConsumerHandler(recorder, req)

	// Check the status code is what you expect
	if status := recorder.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Parse the response body
	var response TopicEntry
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Errorf("error decoding response body: %v", err)
	}

	// Check the parsed response values
	expectedKey := base64.StdEncoding.EncodeToString([]byte("12345"))
	expectedValue := base64.StdEncoding.EncodeToString([]byte("test"))
	if response.Key != expectedKey {
		t.Errorf("unexpected key in response: got %s want %s", response.Key, expectedKey)
	}
	if response.Value != string(expectedValue) {
		t.Errorf("unexpected value in response: got %s want %s", response.Value, expectedValue)
	}
}

func TestPollingConsumerHandler(t *testing.T) {
	mockStore := &MockMessageStore{}
	client := &MessageStoreClient{
		MessageStore: mockStore,
	}

	req, err := http.NewRequest("GET", "/poll?topic=test&offset=123&pollDuration=100ms", nil)
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()

	client.PollingConsumerHandler(recorder, req)

	// Check the status code is what you expect
	if status := recorder.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Parse the response body
	var response TopicEntry
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Errorf("error decoding response body: %v", err)
	}

	// Check the parsed response values
	expectedKey := base64.StdEncoding.EncodeToString([]byte("12345"))
	expectedValue := base64.StdEncoding.EncodeToString([]byte("test"))
	if response.Key != expectedKey {
		t.Errorf("unexpected key in response: got %s want %s", response.Key, expectedKey)
	}
	if response.Value != expectedValue {
		t.Errorf("unexpected value in response: got %s want %s", response.Value, expectedValue)
	}
}
