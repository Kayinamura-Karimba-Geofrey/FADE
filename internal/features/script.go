package features

// featureLua is the atomic dedupe-check + read-prior-state + update script.
//
// It never applies an event twice: the dedupe key is written only at the end,
// after every state mutation. A duplicate or late event returns before any
// write. Window reads use ZRANGEBYSCORE with an exclusive lower bound, so the
// membership rule (t-W, t] is enforced in one place.
//
// ARGV[1] is a JSON object built by scriptInput in store.go. Redis keys are
// constructed inside the script, so EVAL carries one argument.
const featureLua = `
local a = cjson.decode(ARGV[1])
local eventID = a.event_id
local t = a.t
local txnType = a.txn_type
local amount = a.amount
local senderID = a.sender_id
local recipientID = a.recipient_id
local deviceID = a.device_id
local lat = a.lat
local lon = a.lon
local sim = a.sim
local lateTolMS = a.late_tol_ms
local dedupeTTLS = a.dedupe_ttl_s
local stateTTLS = a.state_ttl_s
local windowTTLS = a.window_ttl_s
local edgeTTLS = a.edge_ttl_s
local deviceTTLS = a.device_ttl_s
local cap = a.cap
local histCap = a.hist_cap
local dayKey = a.day_key
local dayCutoff = a.day_cutoff
local dayKeys = a.day_keys
local hourKeys = a.hour_keys
local currentHour = a.current_hour
local hourCutoff = a.hour_cutoff
local agentID = a.agent_id
local baseline = a.baseline
local senderOut = a.sender_out
local senderIn = a.sender_in
local recipientIn = a.recipient_in
local recipientOut = a.recipient_out
local smallLower = a.small_lower
local smallUpper = a.small_upper
local agentInAmt = a.agent_in
local agentOutAmt = a.agent_out
local agentTTLS = a.agent_ttl_s
local agentHourTTLS = a.agent_hour_ttl_s
local windowRetentionMS = a.window_retention_ms
local deviceRetentionMS = a.device_retention_ms
local edgeRetentionMS = a.edge_retention_ms
local recipSendersRetentionMS = a.recip_senders_retention_ms

-- Keys are built inside the script. Phase 2 runs on a single Redis primary
-- (documented in docs/features.md), so no cluster slot constraints apply.
local dedupe = 'dedupe:' .. eventID
local stateKey = 's:acc:' .. senderID
local histKey = 'w:acc:' .. senderID .. ':hist'
local sendKey = 'w:acc:' .. senderID .. ':send'
local inKey = 'w:acc:' .. senderID .. ':in'
local recipKey = 'w:acc:' .. senderID .. ':recip'
local dailyKey = 's:acc:' .. senderID .. ':daily'
local devKey = 'w:dev:' .. deviceID
local recipSendersKey = 'w:recip:' .. recipientID .. ':senders'
local edgeKey = 'g:edge:' .. senderID .. ':' .. recipientID
local outKey = 'g:out:' .. senderID
local agentKey = 'a:agent:' .. agentID
local agentHrKey = 'a:agent:' .. agentID .. ':hr'
local recipStateKey = 's:acc:' .. recipientID
local recipHistKey = 'w:acc:' .. recipientID .. ':hist'
local recipSendKey = 'w:recip:' .. recipientID .. ':send'
local recipInKey = 'w:recip:' .. recipientID .. ':in'

local function n(v) return tonumber(v) or 0 end

local function fmt(v) return string.format('%.0f', v or 0) end

local function parseSnapshot(member)
  local id, dev, blat, blon, bsim, simchg, cash =
    string.match(member, '^([^|]+)|([^|]*)|([^|]*)|([^|]*)|([^|]*)|([^|]*)|([^|]*)$')
  if not id then return nil end
  return {
    id = id, device = dev, lat = n(blat), lon = n(blon), sim = bsim,
    simchg = n(simchg), cashout = n(cash),
  }
end

-- priorSnapshot finds the latest snapshot at or before (t, eventID). It pages
-- in small chunks: normally the first snapshot is the answer, so reading 200
-- members per event is unnecessary.
local function priorSnapshot(key)
  -- Fast path: the newest snapshot is almost always the prior one.
  local first = redis.call('ZREVRANGEBYSCORE', key, t, '-inf', 'WITHSCORES', 'LIMIT', 0, 1)
  if #first > 0 then
    local score = n(first[2])
    local id = string.sub(first[1], 1, 26)
    if score < t or id <= eventID then return parseSnapshot(first[1]) end
  end
  local offset = 1
  while offset < histCap do
    local rows = redis.call('ZREVRANGEBYSCORE', key, t, '-inf', 'WITHSCORES', 'LIMIT', offset, 16)
    if #rows == 0 then return nil end
    local i = 1
    while i < #rows do
      local score = n(rows[i + 1])
      local id = string.sub(rows[i], 1, 26)
      if score < t or id <= eventID then
        return parseSnapshot(rows[i])
      end
      i = i + 2
    end
    offset = offset + 16
  end
  return nil
end

local function trimSet(key, cut, limit)
  redis.call('ZREMRANGEBYSCORE', key, '-inf', '(' .. fmt(cut))
  local size = redis.call('ZCARD', key)
  local dropped = 0
  if size > limit then
    dropped = size - limit
    redis.call('ZREMRANGEBYRANK', key, 0, dropped - 1)
  end
  return dropped
end

local function windowCount(key, windowMS)
  return redis.call('ZCOUNT', key, '(' .. fmt(t - windowMS), fmt(t))
end

local function sumRange(key, minScore, maxScore)
  local rows = redis.call('ZRANGEBYSCORE', key, minScore, maxScore)
  local total = 0
  for _, member in ipairs(rows) do
    total = total + n(string.match(member, ':(%d+)$'))
  end
  return total, #rows
end

local function distinct(key, minScore, maxScore, target)
  local rows = redis.call('ZRANGEBYSCORE', key, minScore, maxScore)
  local seen = {}
  local count = 0
  local present = 0
  for _, member in ipairs(rows) do
    local prefix = string.match(member, '^([^:]+)')
    if prefix == target then present = 1 end
    if prefix and not seen[prefix] then
      seen[prefix] = true
      count = count + 1
    end
  end
  return count, present
end

local function latestScore(key, maxScore)
  local first = redis.call('ZREVRANGEBYSCORE', key, fmt(maxScore), '-inf', 'WITHSCORES', 'LIMIT', 0, 1)
  if #first > 0 then
    local score = n(first[2])
    local id = string.match(first[1], '^([^:]+)')
    if score < t or (id and id <= eventID) then return score end
  end
  local offset = 1
  while offset < 200 do
    local rows = redis.call('ZREVRANGEBYSCORE', key, fmt(maxScore), '-inf', 'WITHSCORES', 'LIMIT', offset, 16)
    if #rows == 0 then return 0 end
    local i = 1
    while i < #rows do
      local score = n(rows[i + 1])
      local id = string.match(rows[i], '^([^:]+)')
      if score < t or (id and id <= eventID) then return score end
      i = i + 2
    end
    offset = offset + 16
  end
  return 0
end

local function parsePair(value)
  if not value then return 0, 0 end
  local a, b = string.match(value, '^(%d+),(%d+)$')
  return n(a), n(b)
end

-- 1. Dedupe before any write.
if redis.call('EXISTS', dedupe) == 1 then
  return cjson.encode({status='duplicate'})
end

-- 2. Watermark / late rejection before any write.
local stateVals = redis.call('HMGET', stateKey, 'first_seen_ms', 'last_seen_ms', 'watermark_ms')
local firstSeen = n(stateVals[1])
local lastSeen = n(stateVals[2])
local watermark = n(stateVals[3])
if watermark > 0 and t < watermark - lateTolMS then
  return cjson.encode({status='late', watermark_ms=watermark})
end
if t > watermark then watermark = t end

local base = priorSnapshot(histKey)
local recipBase = priorSnapshot(recipHistKey)

local cut = watermark - windowRetentionMS
if cut < 0 then cut = 0 end
local capped = 0
capped = capped + trimSet(sendKey, cut, cap)
capped = capped + trimSet(inKey, cut, cap)
capped = capped + trimSet(recipKey, cut, cap)
capped = capped + trimSet(recipInKey, cut, cap)
capped = capped + trimSet(recipSendKey, cut, cap)
local devCut = watermark - deviceRetentionMS
if devCut < 0 then devCut = 0 end
capped = capped + trimSet(devKey, devCut, cap)
local rsCut = watermark - recipSendersRetentionMS
if rsCut < 0 then rsCut = 0 end
capped = capped + trimSet(recipSendersKey, rsCut, cap)

-- 3. Read prior aggregates. One 24h send-window read serves velocity, amount,
-- small-run and outbound-1h aggregates.
local vel1m, vel10m, vel1h = 0, 0, 0
local amt10m, amt24h, smallRun, out1h = 0, 0, 0, 0
local sendRows = redis.call('ZRANGEBYSCORE', sendKey, '(' .. fmt(t - 86400000), fmt(t), 'WITHSCORES')
for i = 1, #sendRows, 2 do
  local score = n(sendRows[i + 1])
  local a = n(string.match(sendRows[i], ':(%d+)$'))
  amt24h = amt24h + a
  if score > t - 60000 then vel1m = vel1m + 1 end
  if score > t - 600000 then
    vel10m = vel10m + 1
    amt10m = amt10m + a
  end
  if score > t - 3600000 then
    vel1h = vel1h + 1
    out1h = out1h + a
    if a >= smallLower and a <= smallUpper then smallRun = smallRun + 1 end
  end
end
local distinctRecip, recipPresent = distinct(recipKey, '(' .. fmt(t - 86400000), fmt(t), recipientID)

local dailyCount, dailySum, dailySumsq = 0, 0, 0
local dailyValues = redis.call('HMGET', dailyKey, unpack(dayKeys))
for _, value in ipairs(dailyValues) do
  if value then
    local c, s, q = string.match(value, '^(%d+),(%d+),(%d+)$')
    dailyCount = dailyCount + n(c)
    dailySum = dailySum + n(s)
    dailySumsq = dailySumsq + n(q)
  end
end

local devAccounts, devPresent = distinct(devKey, '(' .. fmt(t - 604800000), fmt(t), senderID)
local edgeLast = n(redis.call('HGET', edgeKey, 'last_ms'))
local senderAccounts, senderPresent = distinct(recipSendersKey, '(' .. fmt(t - 86400000), fmt(t), senderID)
local lastIn = latestScore(recipInKey, t)
local lastCashout = 0
if recipBase then lastCashout = recipBase.cashout end
local lastInbound = lastIn
if recipientIn == 1 then lastInbound = t end

local in1h = sumRange(inKey, '(' .. fmt(t - 3600000), fmt(t))

local agentIn24, agentOut24, agentNet = 0, 0, 0
if agentID ~= '' then
  local hourly = redis.call('HMGET', agentHrKey, unpack(hourKeys))
  for _, value in ipairs(hourly) do
    local ai, ao = parsePair(value)
    agentIn24 = agentIn24 + ai
    agentOut24 = agentOut24 + ao
  end
  local netRaw = redis.call('HGET', agentKey, 'net_total')
  if netRaw then
    agentNet = n(netRaw)
  else
    -- One-time migration for state written before net_total existed.
    local all = redis.call('HGETALL', agentHrKey)
    local i = 1
    while i < #all do
      if all[i] <= currentHour then
        local ai, ao = parsePair(all[i + 1])
        agentNet = agentNet + ai - ao
      end
      i = i + 2
    end
    redis.call('HSET', agentKey, 'net_total', fmt(agentNet))
  end
end

-- 4. Apply the update.
local simChanged = 0
local simchg = -1
local lastCashoutAfter = 0
if base then
  simchg = base.simchg
  if base.sim ~= '' and sim ~= base.sim then
    simChanged = 1
    simchg = t
  end
  lastCashoutAfter = base.cashout
elseif sim ~= '' then
  simchg = -1
end
if txnType == 'CASH_OUT' then
  if t > lastCashoutAfter then lastCashoutAfter = t end
end

if senderOut == 1 then
  redis.call('ZADD', sendKey, fmt(t), eventID .. ':' .. fmt(amount))
  redis.call('ZADD', recipKey, fmt(t), recipientID .. ':' .. eventID)
end
if senderIn == 1 then
  redis.call('ZADD', inKey, fmt(t), eventID .. ':' .. fmt(amount))
end
if recipientIn == 1 then
  redis.call('ZADD', recipInKey, fmt(t), eventID .. ':' .. fmt(amount))
  redis.call('ZADD', recipSendersKey, fmt(t), senderID .. ':' .. eventID)
end
if recipientOut == 1 then
  redis.call('ZADD', recipSendKey, fmt(t), eventID .. ':' .. fmt(amount))
end
if deviceID ~= '' then
  redis.call('ZADD', devKey, fmt(t), senderID .. ':' .. eventID)
end

if senderOut == 1 then
  local value = redis.call('HGET', dailyKey, dayKey)
  local c, s, q = string.match(value or '0,0,0', '^(%d+),(%d+),(%d+)$')
  local nc, ns, nq = n(c) + 1, n(s) + amount, n(q) + amount * amount
  redis.call('HSET', dailyKey, dayKey, fmt(nc) .. ',' .. fmt(ns) .. ',' .. fmt(nq))
end

if recipientID ~= '' then
  local edgeTotal = redis.call('HGET', edgeKey, 'last_ms')
  local edgeLastBefore = n(edgeTotal)
  redis.call('HINCRBY', edgeKey, 'count', 1)
  redis.call('HINCRBY', edgeKey, 'total', amount)
  if t >= edgeLastBefore then
    redis.call('HSET', edgeKey, 'last_ms', fmt(t))
  end
  -- The cycle graph only contains transfer edges (P2P_SEND/BANK_TRANSFER).
  if txnType == 'P2P_SEND' or txnType == 'BANK_TRANSFER' then
    local lastTransfer = n(redis.call('HGET', edgeKey, 'last_transfer_ms'))
    if t >= lastTransfer then
      redis.call('HSET', edgeKey, 'last_transfer_ms', fmt(t))
      redis.call('ZADD', outKey, fmt(t), recipientID)
      local size = redis.call('ZCARD', outKey)
      if size > 50 then redis.call('ZREMRANGEBYRANK', outKey, 0, size - 51) end
    end
    redis.call('ZREMRANGEBYSCORE', outKey, '-inf', '(' .. fmt(watermark - edgeRetentionMS))
  end
end

if agentID ~= '' then
  local ai, ao = parsePair(redis.call('HGET', agentHrKey, currentHour))
  ai = ai + agentInAmt
  ao = ao + agentOutAmt
  redis.call('HSET', agentHrKey, currentHour, fmt(ai) .. ',' .. fmt(ao))
  local fs = redis.call('HGET', agentKey, 'float_start')
  if not fs then redis.call('HSET', agentKey, 'float_start', fmt(baseline)) end
  redis.call('HINCRBY', agentKey, 'net_total', agentInAmt - agentOutAmt)
  redis.call('HSET', agentKey, 'float_now', fmt(baseline + agentNet + agentInAmt - agentOutAmt))
  local lastAgent = n(redis.call('HGET', agentKey, 'last_ms'))
  if t > lastAgent then redis.call('HSET', agentKey, 'last_ms', fmt(t)) end
end

local snapshot = eventID .. '|' .. deviceID .. '|' .. string.format('%.10f', lat) .. '|' ..
  string.format('%.10f', lon) .. '|' .. sim .. '|' .. fmt(simchg) .. '|' .. fmt(lastCashoutAfter)
redis.call('ZADD', histKey, fmt(t), snapshot)
local hsize = redis.call('ZCARD', histKey)
if hsize > histCap then redis.call('ZREMRANGEBYRANK', histKey, 0, hsize - histCap - 1) end
redis.call('ZREMRANGEBYSCORE', histKey, '-inf', '(' .. fmt(watermark - windowRetentionMS))

-- Recipient history carries the recipient's own last-known attributes and its
-- cash-out clock; an incoming event must not overwrite them with sender data.
if recipientID ~= '' and recipientID ~= senderID then
  local rDevice, rLat, rLon, rSIM, rSIMChg, rCash = '', 0, 0, '', -1, 0
  if recipBase then
    rDevice, rLat, rLon, rSIM, rSIMChg, rCash =
      recipBase.device, recipBase.lat, recipBase.lon, recipBase.sim, recipBase.simchg, recipBase.cashout
  end
  if recipientOut == 1 and t > rCash then rCash = t end
  local rSnapshot = eventID .. '|' .. rDevice .. '|' .. string.format('%.10f', rLat) .. '|' ..
    string.format('%.10f', rLon) .. '|' .. rSIM .. '|' .. fmt(rSIMChg) .. '|' .. fmt(rCash)
  redis.call('ZADD', recipHistKey, fmt(t), rSnapshot)
  local rsize = redis.call('ZCARD', recipHistKey)
  if rsize > histCap then redis.call('ZREMRANGEBYRANK', recipHistKey, 0, rsize - histCap - 1) end
  redis.call('ZREMRANGEBYSCORE', recipHistKey, '-inf', '(' .. fmt(watermark - windowRetentionMS))
end

if t >= lastSeen or lastSeen == 0 then
  if firstSeen == 0 then firstSeen = t end
  redis.call('HSET', stateKey,
    'first_seen_ms', fmt(firstSeen), 'last_seen_ms', fmt(t),
    'watermark_ms', fmt(watermark), 'last_device', deviceID,
    'last_geo_lat', string.format('%.10f', lat), 'last_geo_lon', string.format('%.10f', lon),
    'last_sim', sim, 'last_sim_change_ms', fmt(simchg))
else
  redis.call('HSET', stateKey, 'watermark_ms', fmt(watermark))
  if firstSeen == 0 or t < firstSeen then redis.call('HSET', stateKey, 'first_seen_ms', fmt(t)) end
end

if recipientIn == 1 then
  local existing = n(redis.call('HGET', recipStateKey, 'last_in_ms'))
  if t > existing then redis.call('HSET', recipStateKey, 'last_in_ms', fmt(t)) end
end
if recipientOut == 1 then
  local existing = n(redis.call('HGET', recipStateKey, 'last_cashout_ms'))
  if t > existing then redis.call('HSET', recipStateKey, 'last_cashout_ms', fmt(t)) end
end

-- Trim daily / agent buckets only when this is not a late backfill.
if t >= watermark then
  local fields = redis.call('HKEYS', dailyKey)
  for _, field in ipairs(fields) do
    if field < dayCutoff then redis.call('HDEL', dailyKey, field) end
  end
  if agentID ~= '' then
    local hours = redis.call('HKEYS', agentHrKey)
    for _, field in ipairs(hours) do
      if field < hourCutoff then
        local ai, ao = parsePair(redis.call('HGET', agentHrKey, field))
        redis.call('HINCRBY', agentKey, 'net_total', ao - ai)
        redis.call('HDEL', agentHrKey, field)
      end
    end
  end
end

redis.call('EXPIRE', stateKey, stateTTLS)
redis.call('EXPIRE', histKey, windowTTLS)
redis.call('EXPIRE', sendKey, windowTTLS)
redis.call('EXPIRE', inKey, windowTTLS)
redis.call('EXPIRE', recipKey, windowTTLS)
redis.call('EXPIRE', dailyKey, windowTTLS)
redis.call('EXPIRE', devKey, deviceTTLS)
redis.call('EXPIRE', recipSendersKey, deviceTTLS)
redis.call('EXPIRE', recipStateKey, stateTTLS)
redis.call('EXPIRE', recipHistKey, windowTTLS)
redis.call('EXPIRE', recipSendKey, windowTTLS)
redis.call('EXPIRE', recipInKey, windowTTLS)
if recipientID ~= '' then
  redis.call('EXPIRE', edgeKey, edgeTTLS)
  redis.call('EXPIRE', outKey, edgeTTLS)
end
if agentID ~= '' then
  redis.call('EXPIRE', agentKey, agentTTLS)
  redis.call('EXPIRE', agentHrKey, agentHourTTLS)
end

-- 5. Acknowledge only after every mutation succeeded.
redis.call('SET', dedupe, '1', 'EX', dedupeTTLS)

return cjson.encode({
  status='ok', capped=capped,
  vel1m=vel1m, vel10m=vel10m, vel1h=vel1h, amt10m=amt10m, amt24h=amt24h,
  distinct_recip=distinctRecip, recip_present=recipPresent, small_run=smallRun,
  daily_count=dailyCount, daily_sum=dailySum, daily_sumsq=dailySumsq,
  base_device=base and base.device or '',
  base_lat=base and base.lat or 0,
  base_lon=base and base.lon or 0,
  base_sim=base and base.sim or '', base_simchg=base and base.simchg or -1,
  base_cashout=base and base.cashout or 0,
  base_present=base and 1 or 0,
  dev_accounts=devAccounts, dev_present=devPresent,
  edge_last_ms=edgeLast,
  sender_accounts=senderAccounts, sender_present=senderPresent,
  last_inbound_ms=lastInbound, last_cashout_ms=lastCashout,
  in1h=in1h, out1h=out1h,
  agent_in24=agentIn24, agent_out24=agentOut24, agent_net=agentNet,
  watermark_ms=watermark,
  sim_changed=simChanged,
})
`
