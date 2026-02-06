package main

import (
	"errors"
	"testing"
)

func TestAllocMetricsString(t *testing.T) {
	m := AllocMetrics{
		DeltaAlloc:     1024,
		DeltaHeap:      2048,
		Objects:        10,
		BytesPerRecord: 102.4,
	}

	s := m.String()
	if s == "" {
		t.Error("String() returned empty")
	}
	// Just verify it doesn't panic and contains key info
	if len(s) < 10 {
		t.Errorf("String() too short: %q", s)
	}
}

func TestClassifyRetryableError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		statusCode  int
		wantRetry   bool
		wantNil     bool
	}{
		{"nil error", nil, 0, false, true},

		// Non-retryable 4xx
		{"400 bad request", errors.New("bad request"), 400, false, false},
		{"401 unauthorized", errors.New("unauthorized"), 401, false, false},
		{"403 forbidden", errors.New("forbidden"), 403, false, false},
		{"404 not found", errors.New("not found"), 404, false, false},

		// Retryable
		{"429 rate limit", errors.New("rate limited"), 429, true, false},
		{"500 server error", errors.New("server error"), 500, true, false},
		{"502 bad gateway", errors.New("bad gateway"), 502, true, false},
		{"503 unavailable", errors.New("unavailable"), 503, true, false},

		// Network errors (retryable)
		{"timeout", errors.New("connection timeout"), 0, true, false},
		{"connection refused", errors.New("connection refused"), 0, true, false},
		{"connection reset", errors.New("connection reset by peer"), 0, true, false},
		{"EOF", errors.New("unexpected EOF"), 0, true, false},
		{"broken pipe", errors.New("broken pipe"), 0, true, false},
		{"no such host", errors.New("dial tcp: no such host"), 0, true, false},

		// Non-retryable generic
		{"generic error", errors.New("some other error"), 0, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyRetryableError(tt.err, tt.statusCode)
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil RetryableError")
			}
			if got.ShouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", got.ShouldRetry, tt.wantRetry)
			}
			if got.StatusCode != tt.statusCode {
				t.Errorf("StatusCode = %d, want %d", got.StatusCode, tt.statusCode)
			}
		})
	}
}

func TestRetryableErrorString(t *testing.T) {
	retryable := &RetryableError{
		Err:         errors.New("rate limited"),
		StatusCode:  429,
		ShouldRetry: true,
	}
	s := retryable.Error()
	if s == "" {
		t.Error("Error() returned empty")
	}

	nonRetryable := &RetryableError{
		Err:         errors.New("bad request"),
		StatusCode:  400,
		ShouldRetry: false,
	}
	s2 := nonRetryable.Error()
	if s2 == "" {
		t.Error("Error() returned empty")
	}

	// Retryable and non-retryable should have different prefixes
	if s == s2 {
		t.Error("retryable and non-retryable errors should differ")
	}
}

func TestContainsAny(t *testing.T) {
	tests := []struct {
		s       string
		substrs []string
		want    bool
	}{
		{"connection timeout", []string{"timeout", "refused"}, true},
		{"connection refused", []string{"timeout", "refused"}, true},
		{"unknown error", []string{"timeout", "refused"}, false},
		{"", []string{"timeout"}, false},
		{"timeout", []string{}, false},
		{"", []string{}, false},
	}

	for _, tt := range tests {
		got := containsAny(tt.s, tt.substrs)
		if got != tt.want {
			t.Errorf("containsAny(%q, %v) = %v, want %v", tt.s, tt.substrs, got, tt.want)
		}
	}
}

func TestVerifyAtomicWrite(t *testing.T) {
	// Non-existent file
	if VerifyAtomicWrite("/nonexistent/path/file.dat") {
		t.Error("expected false for non-existent file")
	}
}
