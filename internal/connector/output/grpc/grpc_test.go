package grpc

import "testing"

func TestNormalizeTarget(t *testing.T) {
	cases := []struct {
		host       string
		wantTarget string
		wantSecure bool
	}{
		{"http://localhost:8080", "localhost:8080", false},
		{"https://api.example.com:443", "api.example.com:443", true},
		{"localhost:8080", "localhost:8080", false},
		{"192.168.1.1:9000", "192.168.1.1:9000", false},
	}
	for _, c := range cases {
		target, secure := normalizeTarget(c.host)
		if target != c.wantTarget || secure != c.wantSecure {
			t.Errorf("normalizeTarget(%q) = (%q, %v), want (%q, %v)",
				c.host, target, secure, c.wantTarget, c.wantSecure)
		}
	}
}
