// Command replay will re-run historical transactions through rule and
// model versions for shadow evaluation. Phase 0 ships the entrypoint
// only; business logic lands in later phases.
package main

import (
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

// version is the build version; overridden at link time via -ldflags.
var version = "0.1.0"

func main() {
	logger := obs.NewLogger("info")
	logger.Info("replay", "version", version)
}
