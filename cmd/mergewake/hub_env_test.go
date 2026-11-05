package main

import (
	"os"
	"testing"
)

// The daemon inherits the panewire hub environment from its host wrapper, and
// the pipeline refuses a reviewer spawn without it. Tests that drive panewire
// reviews through fake runners get fake values for the whole package.
func TestMain(m *testing.M) {
	os.Setenv("PANEWIRE_HUB_URL", "https://hub.example.invalid")
	os.Setenv("PANEWIRE_HUB_TOKEN_ENV", "EXAMPLE_HUB_TOKEN")
	os.Exit(m.Run())
}
