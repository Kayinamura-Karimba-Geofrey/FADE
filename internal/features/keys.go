package features

// Redis key layout. docs/features.md is normative; these helpers are the
// only place the worker is allowed to construct a key.
//
// Phase 2 requires a single Redis primary because the Lua script touches the
// dedupe key and both account keys atomically. Cluster hash tags would need to
// move every key for one event into the same slot, which is impossible for
// sender/recipient pairs with different accounts.

func dedupeKey(eventID string) string { return "dedupe:" + eventID }

func accountStateKey(id string) string { return "s:acc:" + id }

func accountHistoryKey(id string) string { return "w:acc:" + id + ":hist" }

func accountSendKey(id string) string { return "w:acc:" + id + ":send" }

func accountInKey(id string) string { return "w:acc:" + id + ":in" }

func accountRecipKey(id string) string { return "w:acc:" + id + ":recip" }

func accountDailyKey(id string) string { return "s:acc:" + id + ":daily" }

func deviceKey(deviceID string) string { return "w:dev:" + deviceID }

func recipientSendersKey(id string) string { return "w:recip:" + id + ":senders" }

func recipientInKey(id string) string { return "w:recip:" + id + ":in" }

func recipientSendKey(id string) string { return "w:recip:" + id + ":send" }

func edgeKey(from, to string) string { return "g:edge:" + from + ":" + to }

func outEdgesKey(from string) string { return "g:out:" + from }

func agentStateKey(id string) string { return "a:agent:" + id }

func agentHourlyKey(id string) string { return "a:agent:" + id + ":hr" }

func cycleCacheKey(from, to string) string { return "cyc:" + from + ":" + to }
