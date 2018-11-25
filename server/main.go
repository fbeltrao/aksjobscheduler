package main

import (
	"net/http"

	schedulerapi "github.com/fbeltrao/aksjobscheduler/schedulerapi"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func main() {

	err := schedulerapi.Initialize()
	if err != nil {
		log.Fatal(err.Error())
	}

	router := mux.NewRouter()

	schedulerapi.SetupRoutes(router)

	log.Info("Starting Jobs API v1.1 listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}
