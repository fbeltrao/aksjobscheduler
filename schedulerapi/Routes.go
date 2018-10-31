package schedulerapi

import (
	"net/http"

	"github.com/gorilla/mux"
)

// Route - object representing a route handler
type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

func createHandler(router *mux.Router, route Route) {
	var handler http.Handler
	handler = route.HandlerFunc
	handler = loggerHandler(handler, route.Name)

	router.
		Methods(route.Method).
		Path(route.Pattern).
		Name(route.Name).
		Handler(handler)
}

// SetupRoutes register the api routes
func SetupRoutes(router *mux.Router) {
	//router.HandleFunc("/jobs", CreateJobHead).Methods("POST").Headers("Content-Type", "application/json")

	routes := []Route{
		Route{
			"Index",
			"GET",
			"/",
			indexHandler,
		},

		Route{
			"CreateJob",
			"POST",
			"/jobs",
			createJobHandler,
		},

		Route{
			"ListJobs",
			"GET",
			"/jobs",
			listJobsHandler,
		},

		Route{
			"GetJob",
			"GET",
			"/jobs/{id}",
			getJobHandler,
		},

		Route{
			"GetJobResults",
			"GET",
			"/jobs/{id}/results",
			getJobResultHandler,
		},

		Route{
			"DeleteJob",
			"DELETE",
			"/jobs/{id}",
			deleteJobHandler,
		},
	}

	for _, route := range routes {
		createHandler(router, route)
	}
}
