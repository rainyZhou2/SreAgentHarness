package app

import "testing"

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:8080": true, "localhost:1": true, "[::1]:9": true,
		"0.0.0.0:8080": false, ":8080": false, "10.0.0.5:80": false, "nonsense": false,
	} {
		if got := isLoopback(addr); got != want {
			t.Errorf("isLoopback(%q)=%v want %v", addr, got, want)
		}
	}
}
