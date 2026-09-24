package config

import "testing"

func TestLoadRejectsMalformedEnvironment(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "many")
	if _, err := Load(); err == nil {
		t.Fatal("expected malformed integer to be rejected")
	}
}

func TestLoadDefaultsAreValid(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"gateway", "relay", "worker", "migrator"} {
		if err := cfg.Validate(role); err != nil {
			t.Fatalf("default config invalid for %s: %v", role, err)
		}
	}
}
