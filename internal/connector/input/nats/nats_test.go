package nats

import "testing"

func TestNormalizeURL(t *testing.T) {
	cases := map[string]string{
		"192.168.1.1:4222": "nats://192.168.1.1:4222",
		"localhost:4222":   "nats://localhost:4222",
		"nats://host:4222": "nats://host:4222",
		"tls://host:4222":  "tls://host:4222",
	}
	for in, want := range cases {
		if got := normalizeURL(in); got != want {
			t.Errorf("normalizeURL(%q) = %q, want %q", in, got, want)
		}
	}
}
