package schedulerapi

import (
	"net/http"
	"os"
	"strconv"
	"time"

	appinsights "github.com/Microsoft/ApplicationInsights-Go/appinsights"
	log "github.com/sirupsen/logrus"
)

func initLogging(debugLogging bool) {

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	log.SetFormatter(&log.TextFormatter{})

	if debugLogging {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

// loggerHandler logs the HTTP requests
func loggerHandler(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		sw := statusWriter{ResponseWriter: w}
		inner.ServeHTTP(&sw, r)

		if appInsightsClient != nil {
			end := time.Now()

			request := appinsights.NewRequestTelemetry(r.Method, r.URL.String(), end.Sub(start), strconv.Itoa(sw.status))

			// Note that the timestamp will be set to time.Now() minus the
			// specified duration.  This can be overridden by either manually
			// setting the Timestamp and Duration fields, or with MarkTime:
			request.MarkTime(start, end)

			// Source of request
			// request.Source = r.
			// Custom properties and measurements can be set here
			request.Properties["user-agent"] = r.UserAgent()
			request.Measurements["response-size"] = float64(sw.length)
			// Finally track it
			appInsightsClient.Track(request)
		}

		log.Infof("%s %s %s %s => %d %d bytes",
			r.Method,
			r.RequestURI,
			name,
			time.Since(start),
			sw.status,
			sw.length,
		)
	})
}
