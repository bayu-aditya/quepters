package health_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bayu-aditya/quepters/internal/health"
)

func TestChecker(t *testing.T) {
	c := health.New()

	if code := status(c); code != http.StatusOK {
		t.Fatalf("default status = %d, want 200", code)
	}

	c.SetServing(false)
	if code := status(c); code != http.StatusServiceUnavailable {
		t.Fatalf("after SetServing(false) status = %d, want 503", code)
	}

	c.SetServing(true)
	if code := status(c); code != http.StatusOK {
		t.Fatalf("after SetServing(true) status = %d, want 200", code)
	}
}

func status(c *health.Checker) int {
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	return rec.Code
}
