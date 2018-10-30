package main

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	azqueue "github.com/Azure/azure-storage-queue-go/2017-07-29/azqueue"
)

// JobQueueItem defines the queue item payload
type JobQueueItem struct {
	File      string `json:"file"`
	Container string `json:"container"`
}

// CalculationRequest defines a calculation operation
type CalculationRequest struct {
	ID        string  `json:"id"`
	Value1    float64 `json:"value1"`
	Value2    float64 `json:"value2"`
	Operation string  `json:"op"`
}

// Calculate returns the result of the operation
func (req *CalculationRequest) Calculate() (*CalculationResponse, error) {
	result := float64(0)
	switch req.Operation {
	case "+":
	case "add":
		result = req.Value1 + req.Value2
	case "-":
	case "minus":
		result = req.Value1 - req.Value2
	case "/":
	case "divide":
		result = req.Value1 / req.Value2
	case "*":
	case "x":
	case "times":
	case "multiply":
		result = req.Value1 * req.Value2
	case "%":
	case "mod":
		result = float64(int64(req.Value1) % int64(req.Value2))
	default:
		return nil, fmt.Errorf("unknown operator '%s'", req.Operation)
	}

	return &CalculationResponse{
		ID:        req.ID,
		Operation: req.Operation,
		Value1:    req.Value1,
		Value2:    req.Value2,
		Result:    result,
	}, nil

}

// CalculationResponse defines a calculation operation response
type CalculationResponse struct {
	ID        string  `json:"id"`
	Value1    float64 `json:"value1"`
	Value2    float64 `json:"value2"`
	Operation string  `json:"op"`
	Result    float64 `json:"result"`
}

func main() {

	// From the Azure portal, get your Storage account's name and key and set environment variables.
	//accountName, accountKey := os.Getenv("ACCOUNT_NAME"), os.Getenv("ACCOUNT_KEY")
	accountName, accountKey := "", ""

	// Use your Storage account's name and key to create a credential object.
	credential := azqueue.NewSharedKeyCredential(accountName, accountKey)

	p := azqueue.NewPipeline(credential, azqueue.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net", accountName))
	serviceURL := azqueue.NewServiceURL(*u, p)

	ctx := context.Background() // This example uses a never-expiring context.

	queueURL := serviceURL.NewQueueURL("jobs")

	messagesURL := queueURL.NewMessagesURL()
	msgs, err := messagesURL.Dequeue(ctx, 1, time.Second*10)
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Found %d messages\n", msgs.NumMessages())

	for i := int32(0); i < msgs.NumMessages(); i++ {
		msg := msgs.Message(i)
		fmt.Printf("Found message %s, dequeue count: %d, message: %s\n", msg.ID, msg.DequeueCount, msg.Text)

		err = processMessage(ctx, msg)
		if err != nil {
			panic(err.Error())
		}

		// OPTIONAL: while processing a message, you can update the message's visibility timeout
		// (to prevent other servers from dequeuing the same message simultaneously) and update the
		// message's text (to prevent some successfully-completed processing from re-executing the
		// next time this message is dequeued):
		// update, err := messagesURL.NewMessageIDURL(msg.ID).Update(ctx, msg.PopReceipt, time.Second*20, "updated msg")
		// if err != nil {
		// 	log.Fatal(err)
		// }

		msgDeleteResponse, err := messagesURL.NewMessageIDURL(msg.ID).Delete(ctx, msg.PopReceipt)
		if err != nil {
			panic(err.Error())
		}

		fmt.Printf("Message delete: response status: %s\n", msgDeleteResponse.Status())
	}
}

func processMessage(ctx context.Context, msg *azqueue.DequeuedMessage) error {
	q := JobQueueItem{}
	// decode base64
	msgPayload, err := b64.StdEncoding.DecodeString(msg.Text)
	if err != nil {
		return err
	}

	fmt.Printf("Message decoded %s\n", string(msgPayload))

	if err := json.Unmarshal(msgPayload, &q); err != nil {
		return err
	}

	fmt.Printf("Will proces file %s//%s\n", q.Container, q.File)

	return processFile(ctx, q.Container, q.File)
}

// ProcessFile handles the calculations in a file
func processFile(ctx context.Context, container, file string) error {

	accountName, accountKey := "", ""
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}

	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, container))
	if err != nil {
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	containerURL := azblob.NewContainerURL(*u, p)

	fmt.Println("Listing the blobs in the container:")
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return err
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Print(" Blob name: " + blobInfo.Name + "\n")
		}
	}
	return nil
}
