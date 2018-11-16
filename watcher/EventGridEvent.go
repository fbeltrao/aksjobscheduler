package main

import (
	"bytes"
	"encoding/json"
	"net/http"

	log "github.com/sirupsen/logrus"
)

// EventGridEvent defines an Azure Event Grid
type EventGridEvent struct {
	Topic     string      `json:"topic"`
	EventType string      `json:"eventType"`
	Subject   string      `json:"subject"`
	ID        string      `json:"id"`
	Data      interface{} `json:"data"`
}

// Send publishes event grid event
func (ev EventGridEvent) Send(endpoint, sasToken string) error {

	evJSONBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(evJSONBytes))
	req.Header.Set("aeg-sas-key", sasToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Infof("Request to %s, returned with status %s and code %d", endpoint, resp.Status, resp.StatusCode)

	return nil

}
