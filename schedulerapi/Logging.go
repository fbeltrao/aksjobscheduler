package schedulerapi

import (
	"net/http"
	"os"
	"time"

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
