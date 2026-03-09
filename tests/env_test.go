package tests

import (
	"os"
	"testing"

	"nats-poc/autoscale"
)

func TestEnvOr(t *testing.T) {
	const key = "TEST_ENVOR_KEY_12345"

	// Unset — should return fallback.
	os.Unsetenv(key)
	if got := autoscale.EnvOr(key, "default"); got != "default" {
		t.Errorf("EnvOr (unset) = %q, want %q", got, "default")
	}

	// Empty — should return fallback.
	os.Setenv(key, "")
	defer os.Unsetenv(key)
	if got := autoscale.EnvOr(key, "default"); got != "default" {
		t.Errorf("EnvOr (empty) = %q, want %q", got, "default")
	}

	// Set — should return env value.
	os.Setenv(key, "custom")
	if got := autoscale.EnvOr(key, "default"); got != "custom" {
		t.Errorf("EnvOr (set) = %q, want %q", got, "custom")
	}
}

func TestEnvInt(t *testing.T) {
	const key = "TEST_ENVINT_KEY_12345"

	// Unset — should return fallback.
	os.Unsetenv(key)
	if got := autoscale.EnvInt(key, 42); got != 42 {
		t.Errorf("EnvInt (unset) = %d, want 42", got)
	}

	// Valid int.
	os.Setenv(key, "100")
	defer os.Unsetenv(key)
	if got := autoscale.EnvInt(key, 42); got != 100 {
		t.Errorf("EnvInt (valid) = %d, want 100", got)
	}

	// Invalid int — should return fallback.
	os.Setenv(key, "notanumber")
	if got := autoscale.EnvInt(key, 42); got != 42 {
		t.Errorf("EnvInt (invalid) = %d, want 42", got)
	}

	// Empty — should return fallback.
	os.Setenv(key, "")
	if got := autoscale.EnvInt(key, 42); got != 42 {
		t.Errorf("EnvInt (empty) = %d, want 42", got)
	}

	// Zero is a valid value.
	os.Setenv(key, "0")
	if got := autoscale.EnvInt(key, 42); got != 0 {
		t.Errorf("EnvInt (zero) = %d, want 0", got)
	}

	// Negative is a valid value.
	os.Setenv(key, "-5")
	if got := autoscale.EnvInt(key, 42); got != -5 {
		t.Errorf("EnvInt (negative) = %d, want -5", got)
	}
}

func TestOrdinalFromHostname(t *testing.T) {
	tests := []struct {
		hostname string
		want     int
		wantErr  bool
	}{
		{"consumer-0", 0, false},
		{"consumer-3", 3, false},
		{"consumer-19", 19, false},
		{"my-app-consumer-7", 7, false},
		{"consumer", -1, true},     // no dash-ordinal
		{"consumer-", -1, true},    // trailing dash, no number
		{"consumer-abc", -1, true}, // non-numeric after dash
		{"", -1, true},             // empty hostname
	}

	for _, tt := range tests {
		t.Run(tt.hostname, func(t *testing.T) {
			got, err := autoscale.OrdinalFromHostname(tt.hostname)
			if (err != nil) != tt.wantErr {
				t.Errorf("OrdinalFromHostname(%q) error = %v, wantErr %v", tt.hostname, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("OrdinalFromHostname(%q) = %d, want %d", tt.hostname, got, tt.want)
			}
		})
	}
}
