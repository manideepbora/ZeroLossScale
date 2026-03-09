package tests

import (
	"testing"

	"nats-poc/autoscale"
)

// fnvHash replicates the hash logic in PartitionForKey for test expectations.
func fnvHash(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
}

func TestPartitionForKey_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		count int
		want  int
	}{
		// Zero and negative counts always return 0.
		{"count=0", "acct-5", 0, 0},
		{"count=-1", "acct-5", -1, 0},
		{"count=-100", "acct-5", -100, 0},

		// Single partition always returns 0.
		{"count=1, numeric", "acct-42", 1, 0},
		{"count=1, non-numeric", "hello", 1, 0},
		{"count=1, empty", "", 1, 0},

		// Numeric trailing keys use modulo.
		{"acct-0 mod 3", "acct-0", 3, 0},
		{"acct-1 mod 3", "acct-1", 3, 1},
		{"acct-2 mod 3", "acct-2", 3, 2},
		{"acct-3 mod 3", "acct-3", 3, 0},
		{"acct-99 mod 5", "acct-99", 5, 4},
		{"acct-20 mod 7", "acct-20", 7, 6},
		{"pure number", "42", 10, 2},

		// Non-numeric keys use FNV-like hash.
		{"non-numeric key", "hello", 3, int(fnvHash("hello") % 3)},
		{"non-numeric key2", "world", 5, int(fnvHash("world") % 5)},
		{"mixed no trailing digits", "abc-def", 4, int(fnvHash("abc-def") % 4)},

		// Empty key with count > 1 uses hash (hash of empty = 0).
		{"empty key", "", 3, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := autoscale.PartitionForKey(tt.key, tt.count)
			if got != tt.want {
				t.Errorf("PartitionForKey(%q, %d) = %d, want %d", tt.key, tt.count, got, tt.want)
			}

			// Verify determinism: calling twice gives same result.
			got2 := autoscale.PartitionForKey(tt.key, tt.count)
			if got != got2 {
				t.Errorf("non-deterministic: PartitionForKey(%q, %d) = %d then %d", tt.key, tt.count, got, got2)
			}
		})
	}
}

func TestExtractTrailingNumber(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"acct-42", 42},
		{"acct-0", 0},
		{"123", 123},
		{"abc", -1},
		{"", -1},
		{"acct-", -1},
		{"a1b2", 2},    // trailing "2"
		{"foo-99x", -1}, // no trailing digits (x is last)
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := autoscale.ExtractTrailingNumber(tt.input)
			if got != tt.want {
				t.Errorf("ExtractTrailingNumber(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestSubjectForPartition(t *testing.T) {
	tests := []struct {
		prefix    string
		partition int
		want      string
	}{
		{"MY_ORDERS", 0, "MY_ORDERS.00"},
		{"MY_ORDERS", 1, "MY_ORDERS.01"},
		{"MY_ORDERS", 9, "MY_ORDERS.09"},
		{"MY_ORDERS", 10, "MY_ORDERS.10"},
		{"MY_ORDERS", 19, "MY_ORDERS.19"},
	}

	for _, tt := range tests {
		got := autoscale.SubjectForPartition(tt.prefix, tt.partition)
		if got != tt.want {
			t.Errorf("SubjectForPartition(%q, %d) = %q, want %q", tt.prefix, tt.partition, got, tt.want)
		}
	}
}
