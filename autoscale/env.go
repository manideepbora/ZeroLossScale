package autoscale

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// EnvOr reads an environment variable or returns the fallback if unset/empty.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvInt reads an integer environment variable or returns the fallback.
func EnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}

// OrdinalFromHostname extracts the StatefulSet pod ordinal from a hostname.
// StatefulSet pods are named "{name}-{ordinal}", e.g. "consumer-3" -> 3.
// Returns -1 if the hostname doesn't match the expected pattern.
func OrdinalFromHostname(hostname string) (int, error) {
	idx := strings.LastIndex(hostname, "-")
	if idx < 0 || idx == len(hostname)-1 {
		return -1, fmt.Errorf("no ordinal in hostname %q", hostname)
	}
	ordinal, err := strconv.Atoi(hostname[idx+1:])
	if err != nil {
		return -1, fmt.Errorf("parse ordinal from hostname %q: %w", hostname, err)
	}
	return ordinal, nil
}
