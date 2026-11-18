//go:build !race

package pipeline

// raceEnabled guards tests that measure allocations: the race detector adds
// its own bookkeeping, so those budgets cannot hold under `-race`.
const raceEnabled = false
