package schedulerapi

import (
	"math/rand"
	"os"
	"strconv"
	"time"
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
