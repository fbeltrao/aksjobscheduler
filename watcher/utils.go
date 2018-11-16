package main

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func getEnvString(key, defaultValue string) string {
	result := os.Getenv(key)
	if len(result) > 0 {
		return result
	}

	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	envValue := os.Getenv(key)
	if len(envValue) > 0 {
		parsed, err := strconv.ParseBool(envValue)
		if err == nil {
			return parsed
		}
	}

	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	envValue := os.Getenv(key)
	if len(envValue) > 0 {
		parsed, err := strconv.Atoi(envValue)
		if err == nil {
			return parsed
		}
	}

	return defaultValue
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func responseWithError(w http.ResponseWriter, err error) {

	log.Error(err)

	respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func responseWithApplicationError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"message": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func encodeBlobNamePrefix(blobNamePrefix string) string {
	return strings.Replace(blobNamePrefix, "/", "__", -1)
}

func decodeBlobNamePrefix(encodedBlobNamePrefix string) string {
	return strings.Replace(encodedBlobNamePrefix, "__", "/", -1)
}
