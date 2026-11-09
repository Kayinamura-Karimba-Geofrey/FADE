"""Independent Python reference for the Phase 2 real-time features.

This module intentionally does not share code with the Go worker. It reads the
Phase 1 dataset in arrival order, applies the event-time windows and dedupe
rules from ``docs/features.md`` to in-process state, and emits one feature
vector for every accepted event. It is the oracle used by the parity harness.

The reference computes cycles without the Redis 60s cache. Parity runs set
``cycle.cache_ttl_s: 0`` so the Go worker takes the same uncached path.
"""

from __future__ import annotations

import json
import math
from bisect import bisect_right
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

FEATURE_FIELDS: tuple[str, ...] = (
    "event_id",
    "velocity_1m",
    "velocity_10m",
    "velocity_1h",
    "amount_sum_10m",
    "amount_sum_24h",
    "distinct_recip_24h",
    "amount_vs_avg_30d",
    "amount_zscore_30d",
    "near_limit_ratio",
    "small_txn_run_1h",
    "new_device",
    "new_geo_km",
    "sim_changed_72h",
    "time_since_sim_change_min",
    "acc_per_device_7d",
    "recip_new_for_sender",
    "recip_distinct_senders_24h",
    "recip_cashout_lag_min",
    "inbound_outbound_ratio_1h",
    "night_hour",
    "cycle_len_2_4",
    "agent_cico_ratio_24h",
    "agent_float_delta_pct",
    "account_age_days",
    "kyc_tier",
    "feature_schema_version",
)

FEATURE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("velocity_1m", pa.int64()),
        ("velocity_10m", pa.int64()),
        ("velocity_1h", pa.int64()),
        ("amount_sum_10m", pa.int64()),
        ("amount_sum_24h", pa.int64()),
        ("distinct_recip_24h", pa.int64()),
        ("amount_vs_avg_30d", pa.float64()),
        ("amount_zscore_30d", pa.float64()),
        ("near_limit_ratio", pa.float64()),
        ("small_txn_run_1h", pa.int64()),
        ("new_device", pa.bool_()),
        ("new_geo_km", pa.float64()),
        ("sim_changed_72h", pa.bool_()),
        ("time_since_sim_change_min", pa.int64()),
        ("acc_per_device_7d", pa.int64()),
        ("recip_new_for_sender", pa.bool_()),
        ("recip_distinct_senders_24h", pa.int64()),
        ("recip_cashout_lag_min", pa.int64()),
        ("inbound_outbound_ratio_1h", pa.float64()),
        ("night_hour", pa.bool_()),
        ("cycle_len_2_4", pa.int64()),
        ("agent_cico_ratio_24h", pa.float64()),
        ("agent_float_delta_pct", pa.float64()),
        ("account_age_days", pa.int64()),
        ("kyc_tier", pa.int64()),
        ("feature_schema_version", pa.string()),
    ]
)

EVENT_COLUMNS = (
    "event_id",
    "event_time",
    "txn_type",
    "amount",
    "currency",
    "fee",
    "sender.account_id",
    "sender.msisdn_hash",
    "sender.device_id",
    "sender.sim_serial_hash",
    "sender.geo.lat",
    "sender.geo.lon",
    "sender.geo.cell_id",
    "sender.account_age_days",
    "sender.kyc_tier",
    "sender.balance_before",
    "recipient.account_id",
    "recipient.msisdn_hash",
    "recipient.is_agent",
    "recipient.first_seen_by_sender",
    "channel",
    "agent_id",
    "session_id",
)

EVENT_JSON_KEYS: tuple[tuple[str, str], ...] = (
    ("event_id", "event_id"),
    ("event_time", "event_time"),
    ("txn_type", "txn_type"),
    ("amount", "amount"),
    ("currency", "currency"),
    ("fee", "fee"),
    ("sender.account_id", "sender"),
    ("sender.msisdn_hash", "sender"),
    ("sender.device_id", "sender"),
    ("sender.sim_serial_hash", "sender"),
    ("sender.geo.lat", "sender"),
    ("sender.geo.lon", "sender"),
    ("sender.geo.cell_id", "sender"),
    ("sender.account_age_days", "sender"),
    ("sender.kyc_tier", "sender"),
    ("sender.balance_before", "sender"),
    ("recipient.account_id", "recipient"),
    ("recipient.msisdn_hash", "recipient"),
    ("recipient.is_agent", "recipient"),
    ("recipient.first_seen_by_sender", "recipient"),
    ("channel", "channel"),
    ("agent_id", "agent_id"),
    ("session_id", "session_id"),
)


def load_config(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate the shared feature YAML."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: feature config must be a mapping")
    for key in (
        "schema_version",
        "windows",
        "stats",
        "kyc_daily_limits_rwf",
        "structuring",
        "time",
        "geo",
        "cycle",
        "agent",
        "caps",
        "retention",
    ):
        if key not in raw:
            raise ValueError(f"{path}: missing {key}")
    return raw


def _ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float, radius: float) -> float:
    d2r = math.pi / 180.0
    phi1, phi2 = lat1 * d2r, lat2 * d2r
    dphi, dlam = (lat2 - lat1) * d2r, (lon2 - lon1) * d2r
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _amount_stats(
    amount: int, count: int, total: int, sumsq: int, min_count: int
) -> tuple[float, float]:
    if count < min_count or count == 0 or total == 0:
        return 1.0, 0.0
    mean = total / count
    if mean <= 0:
        return 1.0, 0.0
    variance = sumsq / count - mean * mean
    if variance <= 0:
        return amount / mean, 0.0
    std = math.sqrt(variance)
    return amount / mean, (amount - mean) / std


@dataclass(slots=True)
class Snapshot:
    event_id: str
    device: str
    lat: float
    lon: float
    sim: str
    sim_change_ms: int
    cashout_ms: int


class TimeWindow:
    """Sorted score list enforcing a (lo, hi] membership rule."""

    __slots__ = ("times", "values")

    def __init__(self) -> None:
        self.times: list[int] = []
        self.values: list[tuple[Any, ...]] = []

    def add(self, t: int, value: tuple[Any, ...]) -> None:
        i = bisect_right(self.times, t)
        self.times.insert(i, t)
        self.values.insert(i, value)

    def trim(self, cut: int, cap: int) -> int:
        i = bisect_right(self.times, cut)
        dropped = i
        if i:
            del self.times[:i]
            del self.values[:i]
        if len(self.times) > cap:
            extra = len(self.times) - cap
            del self.times[:extra]
            del self.values[:extra]
            dropped += extra
        return dropped

    def count(self, lo: int, hi: int) -> int:
        return bisect_right(self.times, hi) - bisect_right(self.times, lo)

    def total(self, lo: int, hi: int) -> tuple[int, int]:
        i, j = bisect_right(self.times, lo), bisect_right(self.times, hi)
        return sum(int(v[0]) for v in self.values[i:j]), j - i

    def distinct(self, lo: int, hi: int, target: str) -> tuple[int, int]:
        i, j = bisect_right(self.times, lo), bisect_right(self.times, hi)
        seen: set[str] = set()
        present = 0
        for value in self.values[i:j]:
            item = str(value[0])
            if item == target:
                present = 1
            seen.add(item)
        return len(seen), present

    def latest(self, hi: int, event_id: str) -> int:
        j = bisect_right(self.times, hi)
        for k in range(j - 1, -1, -1):
            if self.times[k] < hi:
                return self.times[k]
            if str(self.values[k][0]) <= event_id:
                return hi
        return 0


class History:
    """Score+member ordered snapshots, matching Redis ZSET prior lookup."""

    __slots__ = ("keys", "snaps", "cap")

    def __init__(self, cap: int) -> None:
        self.keys: list[tuple[int, str]] = []
        self.snaps: list[Snapshot] = []
        self.cap = cap

    def prior(self, t: int, event_id: str) -> Snapshot | None:
        i = bisect_right(self.keys, (t, event_id)) - 1
        return self.snaps[i] if i >= 0 else None

    def add(self, t: int, snap: Snapshot) -> None:
        key = (t, snap.event_id)
        i = bisect_right(self.keys, key)
        self.keys.insert(i, key)
        self.snaps.insert(i, snap)
        if len(self.keys) > self.cap:
            del self.keys[: len(self.keys) - self.cap]
            del self.snaps[: len(self.snaps) - self.cap]

    def trim(self, cut: int) -> None:
        i = bisect_right(self.keys, (cut, "\uffff"))
        if i:
            del self.keys[:i]
            del self.snaps[:i]


@dataclass(slots=True)
class Account:
    watermark: int = 0
    history: History = field(default_factory=lambda: History(200))
    send: TimeWindow = field(default_factory=TimeWindow)
    inbound: TimeWindow = field(default_factory=TimeWindow)
    recipients: TimeWindow = field(default_factory=TimeWindow)
    daily: dict[str, list[int]] = field(default_factory=dict)
    recip_in: TimeWindow = field(default_factory=TimeWindow)
    recip_send: TimeWindow = field(default_factory=TimeWindow)
    recip_senders: TimeWindow = field(default_factory=TimeWindow)


class FeatureComputer:
    """Deterministic reference implementation of the 25 features."""

    def __init__(self, cfg: dict[str, Any], late_tolerance_s: int = 60) -> None:
        self.cfg = cfg
        self.windows = cfg["windows"]
        self.stats = cfg["stats"]
        self.structuring = cfg["structuring"]
        self.time_cfg = cfg["time"]
        self.geo = cfg["geo"]
        self.cycle_cfg = cfg["cycle"]
        self.agent_cfg = cfg["agent"]
        self.caps = cfg["caps"]
        self.retention = cfg["retention"]
        self.schema_version = str(cfg["schema_version"])
        self.late_tolerance_ms = late_tolerance_s * 1000
        self.tz = ZoneInfo(str(self.time_cfg["timezone"]))
        self.accounts: dict[str, Account] = {}
        self.devices: dict[str, TimeWindow] = {}
        self.edge_last: dict[tuple[str, str], int] = {}
        self.transfer_last: dict[tuple[str, str], int] = {}
        self.out_edges: dict[str, dict[str, int]] = {}
        self.agents: dict[str, dict[str, list[int]]] = {}
        self.dedupe: set[str] = set()
        self.status: str = "ok"

    def _account(self, account_id: str) -> Account:
        acc = self.accounts.get(account_id)
        if acc is None:
            acc = Account(history=History(int(self.caps["state_history"])))
            self.accounts[account_id] = acc
        return acc

    def _window(self, table: dict[str, TimeWindow], key: str) -> TimeWindow:
        win = table.get(key)
        if win is None:
            win = TimeWindow()
            table[key] = win
        return win

    def compute(self, row: dict[str, Any]) -> dict[str, Any] | None:
        """Process one event row. Returns a vector or None for duplicate/late."""
        event_id = str(row["event_id"])
        if event_id in self.dedupe:
            self.status = "duplicate"
            return None
        t = _ms(str(row["event_time"]))
        dt = _utc(str(row["event_time"]))
        txn_type = str(row["txn_type"])
        amount = int(row["amount"])
        sender_out = txn_type != "CASH_IN"
        sender_in = not sender_out
        recipient_in = txn_type != "CASH_OUT"
        recipient_out = not recipient_in
        sender_id = str(row["sender.account_id"])
        recipient_id = str(row["recipient.account_id"])
        device_id = str(row["sender.device_id"])
        sim = str(row["sender.sim_serial_hash"])
        lat = float(row["sender.geo.lat"])
        lon = float(row["sender.geo.lon"])
        is_agent = bool(row.get("recipient.is_agent", False))
        agent_raw = row.get("agent_id")
        agent_id = (
            str(agent_raw) if agent_raw not in (None, "") else (recipient_id if is_agent else "")
        )

        sender = self._account(sender_id)
        watermark_before = sender.watermark
        if watermark_before > 0 and t < watermark_before - self.late_tolerance_ms:
            self.status = "late"
            return None
        watermark = max(watermark_before, t)

        sender_base = sender.history.prior(t, event_id)
        recipient = self._account(recipient_id)
        recip_base = recipient.history.prior(t, event_id)

        cut = max(0, watermark - int(self.retention["window_ttl_s"]) * 1000)
        cap = int(self.caps["window_zset"])
        for win in (
            sender.send,
            sender.inbound,
            sender.recipients,
            recipient.recip_in,
            recipient.recip_send,
        ):
            win.trim(cut, cap)
        dev_cut = max(0, watermark - int(self.retention["device_ttl_s"]) * 1000)
        dev_win = self._window(self.devices, device_id)
        dev_win.trim(dev_cut, cap)
        recipient.recip_senders.trim(dev_cut, cap)

        t_ms = t
        vel_1m = sender.send.count(t_ms - int(self.windows["velocity_1m_s"]) * 1000, t_ms)
        vel_10m = sender.send.count(t_ms - int(self.windows["velocity_10m_s"]) * 1000, t_ms)
        vel_1h = sender.send.count(t_ms - int(self.windows["velocity_1h_s"]) * 1000, t_ms)
        amt_10m, _ = sender.send.total(t_ms - int(self.windows["amount_10m_s"]) * 1000, t_ms)
        amt_24h, _ = sender.send.total(t_ms - int(self.windows["amount_24h_s"]) * 1000, t_ms)
        distinct_recip, recip_present = sender.recipients.distinct(
            t_ms - int(self.windows["distinct_recip_24h_s"]) * 1000, t_ms, recipient_id
        )
        lower = int(
            float(self.structuring["threshold_rwf"]) * float(self.structuring["lower_ratio"])
        )
        upper = int(
            float(self.structuring["threshold_rwf"]) * float(self.structuring["upper_ratio"])
        )
        small_run = 0
        small_win = sender.send
        i = bisect_right(small_win.times, t_ms - int(self.windows["small_txn_1h_s"]) * 1000)
        j = bisect_right(small_win.times, t_ms)
        for value in small_win.values[i:j]:
            if lower <= int(value[0]) <= upper:
                small_run += 1

        day = dt.strftime("%Y%m%d")
        day_keys = [
            (dt - timedelta(days=k)).strftime("%Y%m%d")
            for k in range(int(self.stats["daily_window_days"]) - 1, -1, -1)
        ]
        daily_count = daily_sum = daily_sumsq = 0
        for key in day_keys:
            bucket = sender.daily.get(key)
            if bucket:
                daily_count += bucket[0]
                daily_sum += bucket[1]
                daily_sumsq += bucket[2]

        dev_accounts, dev_present = dev_win.distinct(
            t_ms - int(self.windows["device_7d_s"]) * 1000, t_ms, sender_id
        )
        edge_last = self.edge_last.get((sender_id, recipient_id), 0)
        senders_win = recipient.recip_senders
        sender_accounts, sender_present = senders_win.distinct(
            t_ms - int(self.windows["recipient_senders_24h_s"]) * 1000, t_ms, sender_id
        )
        last_inbound = recipient.recip_in.latest(t_ms, event_id)
        if recipient_in:
            last_inbound = t
        last_cashout = recip_base.cashout_ms if recip_base else 0

        in_1h, _ = sender.inbound.total(
            t_ms - int(self.windows["inbound_outbound_1h_s"]) * 1000, t_ms
        )
        out_1h, _ = sender.send.total(
            t_ms - int(self.windows["inbound_outbound_1h_s"]) * 1000, t_ms
        )
        agent_in = agent_out = 0
        if agent_id:
            agent_in = amount if txn_type == "CASH_IN" else 0
            agent_out = amount if txn_type == "CASH_OUT" else 0
            if txn_type == "BANK_TRANSFER" and is_agent:
                agent_in = amount
            current_hour = dt.strftime("%Y%m%d%H")
            hour_keys = [(dt - timedelta(hours=k)).strftime("%Y%m%d%H") for k in range(23, -1, -1)]
            hourly = self.agents.get(agent_id, {})
            agent_in_24 = sum(hourly.get(k, [0, 0])[0] for k in hour_keys)
            agent_out_24 = sum(hourly.get(k, [0, 0])[1] for k in hour_keys)
            agent_net = sum(v[0] - v[1] for k, v in hourly.items() if k <= current_hour)
        else:
            current_hour = ""
            agent_in_24 = agent_out_24 = agent_net = 0

        sim_changed = 0
        sim_change_ms = -1
        last_cashout_after = 0
        if sender_base:
            sim_change_ms = sender_base.sim_change_ms
            if sender_base.sim and sim != sender_base.sim:
                sim_changed = 1
                sim_change_ms = t
            last_cashout_after = sender_base.cashout_ms
        if txn_type == "CASH_OUT" and t > last_cashout_after:
            last_cashout_after = t

        if sender_out:
            sender.send.add(t, (amount,))
            sender.recipients.add(t, (recipient_id,))
        if sender_in:
            sender.inbound.add(t, (amount,))
        if recipient_in:
            recipient.recip_in.add(t, (event_id, amount))
            recipient.recip_senders.add(t, (sender_id,))
        if recipient_out:
            recipient.recip_send.add(t, (amount,))
        dev_win.add(t, (sender_id,))

        if sender_out:
            bucket = sender.daily.setdefault(day, [0, 0, 0])
            bucket[0] += 1
            bucket[1] += amount
            bucket[2] += amount * amount

        if recipient_id:
            prior_edge = self.edge_last.get((sender_id, recipient_id), 0)
            if t >= prior_edge:
                self.edge_last[(sender_id, recipient_id)] = t
            if txn_type in ("P2P_SEND", "BANK_TRANSFER"):
                prior_transfer = self.transfer_last.get((sender_id, recipient_id), 0)
                if t >= prior_transfer:
                    self.transfer_last[(sender_id, recipient_id)] = t
                    edges_for = self.out_edges.setdefault(sender_id, {})
                    edges_for[recipient_id] = t
                    if len(edges_for) > 50:
                        for node in sorted(edges_for, key=lambda n: (edges_for[n], n))[
                            : len(edges_for) - 50
                        ]:
                            del edges_for[node]
                out_cut = watermark - int(self.retention["edge_ttl_s"]) * 1000
                current_edges = self.out_edges.get(sender_id)
                if current_edges:
                    for node in [n for n, score in current_edges.items() if score < out_cut]:
                        del current_edges[node]

        if agent_id:
            hourly = self.agents.setdefault(agent_id, {})
            pair = hourly.setdefault(current_hour, [0, 0])
            pair[0] += agent_in
            pair[1] += agent_out

        sender.history.add(
            t, Snapshot(event_id, device_id, lat, lon, sim, sim_change_ms, last_cashout_after)
        )

        if recipient_id and recipient_id != sender_id:
            r_device = recip_base.device if recip_base else ""
            r_lat = recip_base.lat if recip_base else 0.0
            r_lon = recip_base.lon if recip_base else 0.0
            r_sim = recip_base.sim if recip_base else ""
            r_simchg = recip_base.sim_change_ms if recip_base else -1
            r_cash = recip_base.cashout_ms if recip_base else 0
            if recipient_out and t > r_cash:
                r_cash = t
            recipient.history.add(
                t, Snapshot(event_id, r_device, r_lat, r_lon, r_sim, r_simchg, r_cash)
            )

        if watermark_before <= t:
            day_cutoff = (dt - timedelta(days=int(self.stats["daily_buckets"]) - 1)).strftime(
                "%Y%m%d"
            )
            for key in [k for k in sender.daily if k < day_cutoff]:
                del sender.daily[key]
            if agent_id:
                hour_cutoff = (
                    dt - timedelta(seconds=int(self.retention["window_ttl_s"]))
                ).strftime("%Y%m%d%H")
                hourly = self.agents.get(agent_id, {})
                for key in [k for k in hourly if k < hour_cutoff]:
                    del hourly[key]

        sender.watermark = watermark
        sender.history.trim(cut)
        recipient.history.trim(cut)
        self.dedupe.add(event_id)

        vector = self._vector(
            row=row,
            t=t,
            dt=dt,
            sender_id=sender_id,
            recipient_id=recipient_id,
            sender_base=sender_base,
            amount=amount,
            sender_out=sender_out,
            sender_in=sender_in,
            recipient_in=recipient_in,
            vel_1m=vel_1m,
            vel_10m=vel_10m,
            vel_1h=vel_1h,
            amt_10m=amt_10m,
            amt_24h=amt_24h,
            distinct_recip=distinct_recip,
            recip_present=recip_present,
            small_run=small_run,
            daily_count=daily_count,
            daily_sum=daily_sum,
            daily_sumsq=daily_sumsq,
            dev_accounts=dev_accounts,
            dev_present=dev_present,
            edge_last=edge_last,
            sender_accounts=sender_accounts,
            sender_present=sender_present,
            last_inbound=last_inbound,
            last_cashout=last_cashout,
            in_1h=in_1h,
            out_1h=out_1h,
            agent_id=agent_id,
            agent_in=agent_in,
            agent_out=agent_out,
            agent_in_24=agent_in_24,
            agent_out_24=agent_out_24,
            agent_net=agent_net,
            sim_changed=sim_changed,
            sim_change_ms=sim_change_ms,
            lower=lower,
            upper=upper,
        )
        self.status = "ok"
        return vector

    def _vector(self, **kw: Any) -> dict[str, Any]:
        row = kw["row"]
        t = int(kw["t"])
        amount = int(kw["amount"])
        sender_out = bool(kw["sender_out"])
        sender_in = bool(kw["sender_in"])
        recipient_in = bool(kw["recipient_in"])
        cfg = self.cfg
        vec: dict[str, Any] = {
            "event_id": str(row["event_id"]),
            "velocity_1m": int(kw["vel_1m"]) + (1 if sender_out else 0),
            "velocity_10m": int(kw["vel_10m"]) + (1 if sender_out else 0),
            "velocity_1h": int(kw["vel_1h"]) + (1 if sender_out else 0),
            "amount_sum_10m": int(kw["amt_10m"]) + (amount if sender_out else 0),
            "amount_sum_24h": int(kw["amt_24h"]) + (amount if sender_out else 0),
            "distinct_recip_24h": int(kw["distinct_recip"])
            + (1 if sender_out and int(kw["recip_present"]) == 0 else 0),
            "amount_vs_avg_30d": 1.0,
            "amount_zscore_30d": 0.0,
            "near_limit_ratio": 0.0,
            "small_txn_run_1h": int(kw["small_run"])
            + (1 if sender_out and int(kw["lower"]) <= amount <= int(kw["upper"]) else 0),
            "new_device": False,
            "new_geo_km": 0.0,
            "sim_changed_72h": False,
            "time_since_sim_change_min": -1,
            "acc_per_device_7d": int(kw["dev_accounts"]) + (0 if int(kw["dev_present"]) else 1),
            "recip_new_for_sender": (
                int(kw["edge_last"]) == 0
                or int(kw["edge_last"]) > t
                or int(kw["edge_last"]) <= t - int(self.windows["recipient_30d_s"]) * 1000
            ),
            "recip_distinct_senders_24h": int(kw["sender_accounts"])
            + (1 if recipient_in and int(kw["sender_present"]) == 0 else 0),
            "recip_cashout_lag_min": -1,
            "inbound_outbound_ratio_1h": 1.0,
            "night_hour": (
                int(self.time_cfg["night_start_hour"])
                <= kw["dt"].astimezone(self.tz).hour
                <= int(self.time_cfg["night_end_hour"])
            ),
            "cycle_len_2_4": 0,
            "agent_cico_ratio_24h": 0.0,
            "agent_float_delta_pct": 0.0,
            "account_age_days": int(row["sender.account_age_days"]),
            "kyc_tier": int(row["sender.kyc_tier"]),
            "feature_schema_version": self.schema_version,
        }
        ratio, zscore = _amount_stats(
            amount,
            int(kw["daily_count"]),
            int(kw["daily_sum"]),
            int(kw["daily_sumsq"]),
            int(self.stats["min_count_30d"]),
        )
        vec["amount_vs_avg_30d"] = ratio
        vec["amount_zscore_30d"] = zscore
        limit = cfg["kyc_daily_limits_rwf"].get(int(row["sender.kyc_tier"]))
        if limit:
            vec["near_limit_ratio"] = amount / float(limit)

        base: Snapshot | None = kw["sender_base"]
        if base is not None:
            vec["new_device"] = bool(base.device) and base.device != str(row["sender.device_id"])
            vec["new_geo_km"] = _haversine_km(
                base.lat,
                base.lon,
                float(row["sender.geo.lat"]),
                float(row["sender.geo.lon"]),
                float(self.geo["earth_radius_km"]),
            )
        sim_changed = bool(kw["sim_changed"])
        base_simchg = base.sim_change_ms if base else -1
        if not sim_changed and base_simchg >= 0:
            sim_changed = t - base_simchg <= int(self.windows["sim_change_72h_s"]) * 1000
        vec["sim_changed_72h"] = sim_changed
        if kw["sim_changed"]:
            vec["time_since_sim_change_min"] = 0
        elif base_simchg >= 0:
            vec["time_since_sim_change_min"] = max(0, (t - base_simchg) // 60000)

        if int(kw["last_inbound"]) > 0 and int(kw["last_cashout"]) >= int(kw["last_inbound"]):
            vec["recip_cashout_lag_min"] = (
                int(kw["last_cashout"]) - int(kw["last_inbound"])
            ) // 60000

        in_1h = int(kw["in_1h"]) + (amount if sender_in else 0)
        out_1h = int(kw["out_1h"]) + (amount if sender_out else 0)
        vec["inbound_outbound_ratio_1h"] = (in_1h + 1) / (out_1h + 1)

        if str(row["txn_type"]) in ("P2P_SEND", "BANK_TRANSFER"):
            vec["cycle_len_2_4"] = self._cycle_length(
                str(row["sender.account_id"]), str(row["recipient.account_id"]), t
            )

        agent_id = str(kw["agent_id"])
        if agent_id:
            vec["agent_cico_ratio_24h"] = (int(kw["agent_in_24"]) + int(kw["agent_in"]) + 1) / (
                int(kw["agent_out_24"]) + int(kw["agent_out"]) + 1
            )
            net = int(kw["agent_net"]) + int(kw["agent_in"]) - int(kw["agent_out"])
            vec["agent_float_delta_pct"] = 100.0 * net / float(self.agent_cfg["float_baseline_rwf"])
        return {name: vec[name] for name in FEATURE_FIELDS}

    def _cycle_length(self, sender_id: str, recipient_id: str, t: int) -> int:
        if not sender_id or not recipient_id or sender_id == recipient_id:
            return 0
        max_hops = int(self.cycle_cfg["max_hops"])
        max_neighbors = int(self.cycle_cfg["max_neighbors"])
        max_visited = int(self.cycle_cfg["max_visited"])
        min_score = t - int(self.windows["cycle_24h_s"]) * 1000
        frontier = [recipient_id]
        visited = {recipient_id, sender_id}
        found = 0
        for depth in range(max_hops):
            nxt: list[str] = []
            for node in frontier:
                edges = [
                    (score, neighbour)
                    for neighbour, score in self.out_edges.get(node, {}).items()
                    if min_score < score <= t
                ]
                edges.sort()
                for _, neighbour in edges[:max_neighbors]:
                    if neighbour == sender_id:
                        found = depth + 2
                        break
                    if depth + 1 >= max_hops or neighbour in visited:
                        continue
                    visited.add(neighbour)
                    if len(visited) > max_visited:
                        return 0
                    nxt.append(neighbour)
                if found:
                    break
            if found:
                break
            frontier = nxt
            if not frontier:
                break
        return found


def _event_json(row: dict[str, Any]) -> dict[str, Any]:
    event: dict[str, Any] = {
        "sender": {},
        "recipient": {},
        "geo": {},
    }
    for column, parent in EVENT_JSON_KEYS:
        value = row[column]
        if isinstance(value, float) and math.isnan(value):
            value = 0.0
        if parent == "sender":
            key = column.split(".", 1)[1]
            if key.startswith("geo."):
                event["geo"][key.split(".", 1)[1]] = value
            else:
                event["sender"][key] = value
        elif parent == "recipient":
            event["recipient"][column.split(".", 1)[1]] = value
        elif parent == "agent_id":
            event["agent_id"] = value if value not in (None, "") else None
        else:
            event[parent] = value
    event["sender"]["geo"] = event.pop("geo")
    return event


def iter_event_rows(dataset: str | Path) -> Iterator[dict[str, Any]]:
    """Yield events.parquet rows in arrival order."""
    path = Path(dataset) / "events.parquet"
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=8192, columns=list(EVENT_COLUMNS)):
        columns = batch.to_pydict()
        for i in range(batch.num_rows):
            yield {name: columns[name][i] for name in EVENT_COLUMNS}


def run_reference(
    dataset: str | Path,
    config_path: str | Path,
    events_jsonl: str | Path | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield feature vectors for accepted events in arrival order."""
    computer = FeatureComputer(load_config(config_path))
    export = None
    if events_jsonl is not None:
        path = Path(events_jsonl)
        path.parent.mkdir(parents=True, exist_ok=True)
        export = path.open("w", encoding="utf-8")
    try:
        for row in iter_event_rows(dataset):
            if export is not None:
                export.write(json.dumps(_event_json(row), separators=(",", ":")) + "\n")
            vector = computer.compute(row)
            if vector is not None:
                yield vector
    finally:
        if export is not None:
            export.close()


def flatten_event(event: dict[str, Any]) -> dict[str, Any]:
    """Flatten contract JSON into the dotted column names used by the dataset."""
    row: dict[str, Any] = {
        "event_id": event["event_id"],
        "event_time": event["event_time"],
        "txn_type": event["txn_type"],
        "amount": event["amount"],
        "currency": event["currency"],
        "fee": event["fee"],
        "channel": event["channel"],
        "agent_id": event.get("agent_id"),
        "session_id": event["session_id"],
    }
    sender = event["sender"]
    for key in (
        "account_id",
        "msisdn_hash",
        "device_id",
        "sim_serial_hash",
        "account_age_days",
        "kyc_tier",
        "balance_before",
    ):
        row[f"sender.{key}"] = sender[key]
    geo = sender["geo"]
    for key in ("lat", "lon", "cell_id"):
        row[f"sender.geo.{key}"] = geo[key]
    recipient = event["recipient"]
    for key in ("account_id", "msisdn_hash", "is_agent", "first_seen_by_sender"):
        row[f"recipient.{key}"] = recipient[key]
    return row


def run_reference_jsonl(
    events_path: str | Path, config_path: str | Path
) -> Iterator[dict[str, Any]]:
    """Yield vectors for a JSONL event stream (used by the committed oracle fixture)."""
    computer = FeatureComputer(load_config(config_path))
    with Path(events_path).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            vector = computer.compute(flatten_event(json.loads(line)))
            if vector is not None:
                yield vector


def write_features(
    dataset: str | Path,
    out: str | Path,
    config_path: str | Path,
    events_jsonl: str | Path | None = None,
) -> int:
    """Write features.parquet and features.jsonl; return accepted count."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    jsonl_path = out / "features.jsonl"
    parquet_path = out / "features.parquet"
    writer = pq.ParquetWriter(parquet_path, FEATURE_SCHEMA, compression="zstd")
    count = 0
    try:
        with jsonl_path.open("w", encoding="utf-8") as handle:
            batch: list[dict[str, Any]] = []
            for vector in run_reference(dataset, config_path, events_jsonl):
                handle.write(json.dumps(vector, separators=(",", ":")) + "\n")
                batch.append(vector)
                count += 1
                if len(batch) >= 8192:
                    writer.write_table(pa.Table.from_pylist(batch, schema=FEATURE_SCHEMA))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=FEATURE_SCHEMA))
    finally:
        writer.close()
    return count


def read_feature_jsonl(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def compare_features(
    reference: Iterable[dict[str, Any]],
    actual: Iterable[dict[str, Any]],
    float_tolerance: float = 1e-9,
) -> list[str]:
    """Return human-readable mismatches; empty means parity."""
    problems: list[str] = []
    ref = iter(reference)
    act = iter(actual)
    index = 0
    while True:
        try:
            expected = next(ref)
        except StopIteration:
            expected = None
        try:
            got = next(act)
        except StopIteration:
            got = None
        if expected is None and got is None:
            return problems
        if expected is None:
            extra = got or {}
            problems.append(f"row {index}: actual has extra {extra.get('event_id')}")
            return problems
        if got is None:
            problems.append(f"row {index}: actual missing {expected.get('event_id')}")
            return problems
        if expected["event_id"] != got["event_id"]:
            problems.append(f"row {index}: event_id {expected['event_id']} != {got['event_id']}")
        for name in FEATURE_FIELDS:
            a, b = expected[name], got[name]
            if isinstance(a, float) or isinstance(b, float):
                if not math.isclose(float(a), float(b), rel_tol=float_tolerance, abs_tol=1e-9):
                    problems.append(f"row {index} {expected['event_id']} {name}: {a} != {b}")
            elif a != b:
                problems.append(f"row {index} {expected['event_id']} {name}: {a} != {b}")
        index += 1
        if len(problems) >= 20:
            problems.append("more mismatches suppressed")
            return problems
