#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import atexit
import asyncio
import html
import csv
import io
import json
import re
import os
import signal
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import websockets
import yaml

BINANCE_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
BYBIT_TICKERS_URL = "https://api.bybit.com/v5/market/tickers"

BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_24H = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_OI_NOW = "https://fapi.binance.com/fapi/v1/openInterest"
BINANCE_OI_HIST = "https://fapi.binance.com/futures/data/openInterestHist"
BINANCE_FUNDING = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_RATIO = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

BOT_DIR = Path(__file__).resolve().parent
RUNTIME = BOT_DIR / ".runtime"
RUNTIME.mkdir(parents=True, exist_ok=True)

LOCK_FILE = RUNTIME / "bot.lock"
HEALTH_FILE = RUNTIME / "health.json"
HEARTBEAT_FILE = RUNTIME / "heartbeat.txt"
RUNTIME_OVERRIDES_FILE = RUNTIME / "runtime_overrides.yaml"
os.environ.setdefault("TZ", "Europe/Berlin")
if hasattr(time, "tzset"):
    time.tzset()
TELEGRAM_POLL_OFFSET_FILE = RUNTIME / "telegram_update_offset.txt"

BLOCKLIST_RUNTIME_FILE = RUNTIME / "blocklist_runtime.json"
MUTE_RUNTIME_FILE = RUNTIME / "mute_runtime.json"

BUILD_VERSION = "V3_3_BTC"
BUILD_DATE = "2026-04-24"


BTC_SYMBOLS = {"BTCUSDT", "BTCPERP"}
BTC_SINGLE_THRESHOLD = 1_000_000
BTC_AGG_THRESHOLD = 1_000_000
BTC_COOLDOWN_SEC = 900



def load():
    with open(BOT_DIR / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if RUNTIME_OVERRIDES_FILE.exists():
        try:
            with open(RUNTIME_OVERRIDES_FILE, "r", encoding="utf-8") as f:
                overrides = yaml.safe_load(f) or {}
            for section, values in overrides.items():
                if isinstance(values, dict):
                    cfg.setdefault(section, {})
                    cfg[section].update(values)
        except Exception as e:
            print(f"Overrides load error: {e!r}", flush=True)

    env_token = os.getenv("BOT_TOKEN")
    env_chat_id = os.getenv("CHAT_ID")
    env_debug_chat_id = os.getenv("DEBUG_CHAT_ID")

    if env_token:
        cfg.setdefault("telegram", {})
        cfg["telegram"]["bot_token"] = env_token
    if env_chat_id:
        cfg.setdefault("telegram", {})
        cfg["telegram"]["chat_id"] = env_chat_id
    if env_debug_chat_id:
        cfg.setdefault("telegram", {})
        cfg["telegram"]["debug_chat_id"] = env_debug_chat_id

    return cfg

def ensure_single_instance():
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text(encoding="utf-8").strip())
            os.kill(pid, 0)
            print(f"❌ Bot already running (PID {pid})", flush=True)
            sys.exit(0)
        except Exception:
            try:
                LOCK_FILE.unlink()
            except Exception:
                pass
    LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
    print(f"✅ Single instance lock set (PID {os.getpid()})", flush=True)


def cleanup_lock():
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass


atexit.register(cleanup_lock)


class Bot:

    def __init__(self, cfg):
        self.cfg = cfg
        self.events_1m = defaultdict(deque)
        self.events_5m = defaultdict(deque)
        self.events_10m = defaultdict(deque)
        self.events_15m = defaultdict(deque)
        self.events_3h = defaultdict(deque)

        self.symbol_state = defaultdict(lambda: {
            "range_active": False,
            "range_start_ts": 0.0,
            "range_exchange": "",
            "last_sent_level": 0,
            "max_level_sent": 0,
            "last_level_change_ts": 0.0,
            "pending_level": 0,
            "pending_since_ts": 0.0,
            "pending_snapshot": None,
            "hyper_cooldown_until": 0.0,
            "super_hyper_cooldown_until": 0.0,
            "monster_mute_until": 0.0,
            "lvl8_active": False,
            "lvl9_active": False,
            "monster_active": False,
            "monster_base_sum15": 0.0,
            "cascade_step_sent": 0,
            "last_sent_signature": "",
            "last_sent_ts": 0.0,
            "last_lvl1_signature": "",
            "last_lvl1_ts": 0.0,
            "last_state_signature": "",
            "last_state_ts": 0.0,
            "anomaly_debug_ts": 0.0,
        })

        self.binance_cache = {}
        self.rank_cache = {}
        self.rank_cache_ts = 0
        self.bybit_symbols_cache = None
        self.bybit_symbols_cache_ts = 0
        self.stop_event = asyncio.Event()
        self.startup_telegram_sent = False
        self.market_events_30m = {"BINANCE": deque(), "BYBIT": deque()}
        self.market_events_4h = {"BINANCE": deque(), "BYBIT": deque()}
        self.signal_history = deque(maxlen=5000)
        self.chat_history = deque(maxlen=10000)
        self.btc_debug_history = deque(maxlen=2000)
        self.state_locks = {}
        self.btc_alerts_enabled = False
        self.btc_last_alert_ts = 0.0
        self.btc_last_alert_reason = None
        self.btc_last_alert_exchange = None
        self.btc_last_alert_amount = 0.0
        self.control_state = {"awaiting_key": None}
        self.daily_cycle_counters = {}
        self.last_daily_restart_key = None
        self.limit_button_map = {
            "1️⃣ Уровень 1": "signals.liq_level_1_usd",
            "2️⃣ Уровень 2": "signals.level_2_usd",
            "3️⃣ Уровень 3": "signals.level_3_usd",
            "4️⃣ Уровень 4": "signals.level_4_usd",
            "5️⃣ Уровень 5": "signals.level_5_usd",
            "6️⃣ Уровень 6": "signals.level_6_usd",
            "7️⃣ Уровень 7": "signals.level_7_usd",
            "⚡ HYPER": "signals.hyper_usd",
            "🚨 SUPER HYPER": "signals.super_hyper_usd",
            "💀 MONSTER": "signals.monster_3h_usd",
        }
        self.allowed_override_keys = {
            "filters.min_terminal_usd": int,
            "filters.min_event_usd": int,
            "signals.liq_level_1_usd": int,
            "signals.level_2_usd": int,
            "signals.level_3_usd": int,
            "signals.level_4_usd": int,
            "signals.level_5_usd": int,
            "signals.level_6_usd": int,
            "signals.level_7_usd": int,
            "signals.hyper_usd": int,
            "signals.super_hyper_usd": int,
            "signals.monster_3h_usd": int,
            "signals.range_delay_sec": int,
            "signals.range_reset_sec": int,
            "signals.allowed_exchanges": str,
        }
        self.keyboard = {
            "keyboard": [
                [{"text": "🏆 TOP"}, {"text": "📥 Скачать"}],
                [{"text": "📊 Лимиты"}, {"text": "⚙️ Режим"}],
                [{"text": "🔄 Reload"}, {"text": "❓ Help"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.top_menu = {
            "keyboard": [
                [{"text": "🏆 BINANCE /30м"}, {"text": "🏆 BYBIT /30м"}],
                [{"text": "🏆 BINANCE /4ч"}, {"text": "🏆 BYBIT /4ч"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.download_menu = {
            "keyboard": [
                [{"text": "📤 Сигналы 24ч"}, {"text": "🗂 Чат 24ч"}],
                [{"text": "🧠 Debug 24ч"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.mode_menu = {
            "keyboard": [
                [{"text": "🟢 Normal"}, {"text": "🟡 Fast test"}],
                [{"text": "🟠 Power day"}, {"text": "⚫ OFF"}],
                [{"text": "₿ BTC ON"}, {"text": "₿ BTC OFF"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.help_menu = {
            "keyboard": [
                [{"text": "📛 Блок лист"}, {"text": "🔕 Mute лист"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.block_menu = {
            "keyboard": [
                [{"text": "📋 Список блока"}],
                [{"text": "➕ Добавить в блок лист"}, {"text": "➖ Убрать из блок листа"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.mute_menu = {
            "keyboard": [
                [{"text": "📋 Список mute"}],
                [{"text": "➕ Добавить в mute лист"}, {"text": "➖ Убрать из mute листа"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.reload_menu = {
            "keyboard": [
                [{"text": "🔄 Reload config"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.limit_menu = {
            "keyboard": [
                [{"text": "1️⃣ Уровень 1"}, {"text": "2️⃣ Уровень 2"}],
                [{"text": "3️⃣ Уровень 3"}, {"text": "4️⃣ Уровень 4"}],
                [{"text": "5️⃣ Уровень 5"}, {"text": "6️⃣ Уровень 6"}],
                [{"text": "7️⃣ Уровень 7"}, {"text": "⚡ HYPER"}],
                [{"text": "🚨 SUPER HYPER"}, {"text": "💀 MONSTER"}],
                [{"text": "↩️ Назад"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }

        self.health = {
            "pid": os.getpid(),
            "started_ts": int(time.time()),
            "last_heartbeat_ts": time.time(),
            "binance_connected": False,
            "bybit_connected": False,
            "telegram_ok": False,
            "last_binance_msg_ts": 0,
            "last_bybit_msg_ts": 0,
            "last_telegram_ok_ts": 0,
            "signals_sent": 0,
            "watchdog_restart_count": 0,
            "status": "starting",
        }

    def write_health(self):
        try:
            HEALTH_FILE.write_text(json.dumps(self.health, ensure_ascii=False, indent=2), encoding="utf-8")
            HEARTBEAT_FILE.write_text(str(int(time.time())), encoding="utf-8")
        except Exception:
            pass

    def msk_day_key(self, ts=None):
        from datetime import datetime, timedelta
        dt = datetime.fromtimestamp(ts or time.time(), tz=ZoneInfo("Europe/Moscow"))
        if dt.hour == 0 and dt.minute == 0:
            dt = dt - timedelta(days=1)
        return dt.strftime("%Y-%m-%d")

    def cycle_counter_entry(self, symbol):
        key = self.msk_day_key()
        entry = self.daily_cycle_counters.get(symbol)
        if not entry or entry.get("day") != key:
            entry = {"day": key, "completed": 0, "current": 0}
            self.daily_cycle_counters[symbol] = entry
        return entry

    def register_cycle_start(self, symbol):
        entry = self.cycle_counter_entry(symbol)
        if entry["current"] <= 0:
            entry["current"] = int(entry["completed"]) + 1
        return int(entry["current"])

    def note_cycle_reset(self, symbol):
        entry = self.cycle_counter_entry(symbol)
        if entry["current"] > 0:
            entry["completed"] = max(int(entry["completed"]), int(entry["current"]))
            entry["current"] = 0

    def load_runtime_mute(self):
        if not MUTE_RUNTIME_FILE.exists():
            return []
        try:
            return json.loads(MUTE_RUNTIME_FILE.read_text(encoding="utf-8")) or []
        except Exception:
            return []

    def save_runtime_mute(self, items):
        try:
            MUTE_RUNTIME_FILE.write_text(json.dumps(sorted(set(items)), ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Mute save error: {e!r}", flush=True)

    def current_mute_list(self):
        return sorted(set(self.load_runtime_mute()))

    def mute_text(self):
        items = self.current_mute_list()
        if not items:
            return "<b>Mute лист</b>\n\nПусто."
        out = ["<b>Mute лист</b>", ""]
        for i, sym in enumerate(items, 1):
            out.append(f"{i}. {self.esc(sym)}")
        return "\n".join(out)

    def is_muted(self, symbol):
        return symbol in set(self.current_mute_list())

    def metric_marks(self, value, thresholds):
        if value is None:
            return ""
        av = abs(float(value))
        if av > thresholds[2]:
            return "❗️❗️❗️"
        if av > thresholds[1]:
            return "❗️❗️"
        if av > thresholds[0]:
            return "❗️"
        return ""

    def pct_with_arrow_marks(self, value, thresholds):
        if value is None:
            return "н/д"
        arrow = "⬆️" if value > 0 else ("⬇️" if value < 0 else "")
        base = self.fmt_pct(value)
        marks = self.metric_marks(value, thresholds)
        if arrow and marks:
            return f"{arrow} {base}{marks}"
        if arrow:
            return f"{arrow} {base}"
        return base

    def funding_with_marks(self, funding):
        if funding is None:
            return "н/д"
        arrow = "⬆️" if funding > 0 else ("⬇️" if funding < 0 else "")
        base = self.fmt_pct(funding)
        if funding < -1:
            marks = "❗️❗️❗️"
        elif funding < -0.5:
            marks = "❗️❗️"
        elif funding < -0.1:
            marks = "❗️"
        else:
            marks = ""
        if arrow and marks:
            return f"{arrow} {base}{marks}"
        if arrow:
            return f"{arrow} {base}"
        return base



    async def watchdog_loop(self):
        startup_grace_sec = int(self.cfg["runtime"].get("startup_grace_sec", 120))
        idle_limit = int(self.cfg["runtime"].get("watchdog_no_data_sec", 180))
        await asyncio.sleep(startup_grace_sec)

        while not self.stop_event.is_set():
            now = time.time()
            self.health["last_heartbeat_ts"] = now
            binance_alive = self.health["binance_connected"] and (now - self.health["last_binance_msg_ts"] <= idle_limit)
            bybit_alive = self.health["bybit_connected"] and (now - self.health["last_bybit_msg_ts"] <= idle_limit)

            if not self.health["binance_connected"] and not self.health["bybit_connected"]:
                self.health["status"] = "waiting_connections"
                self.write_health()
                await asyncio.sleep(5)
                continue

            self.health["status"] = "running" if (binance_alive or bybit_alive) else "degraded"
            self.write_health()

            if self.cfg["runtime"].get("watchdog_enabled", True):
                if not binance_alive and not bybit_alive:
                    print("WATCHDOG: no market data, terminating for auto-restart", flush=True)
                    self.health["status"] = "watchdog_restart"
                    self.health["watchdog_restart_count"] += 1
                    self.write_health()
                    os._exit(21)

            # scheduled restart removed intentionally: manual Railway redeploy only

            await asyncio.sleep(5)

    def fmt_usd(self, v):
        if v is None:
            return "н/д"
        if v >= 1_000_000_000:
            return f"${v/1_000_000_000:.2f}B"
        if v >= 1_000_000:
            return f"${v/1_000_000:.2f}M"
        if v >= 1_000:
            return f"${v/1_000:.1f}K"
        return f"${v:.0f}"

    def fmt_num(self, v):
        if v is None:
            return "н/д"
        v = float(v)
        sign = "-" if v < 0 else ""
        a = abs(v)
        if a >= 1_000_000_000:
            return f"{sign}{a/1_000_000_000:.2f}B"
        if a >= 1_000_000:
            return f"{sign}{a/1_000_000:.2f}M"
        if a >= 1_000:
            return f"{sign}{a/1_000:.1f}K"
        if a >= 100:
            return f"{sign}{a:.0f}"
        if a >= 10:
            return f"{sign}{a:.1f}"
        return f"{sign}{a:.2f}"

    def fmt_pct(self, v):
        if v is None:
            return "н/д"
        return f"{v:+.2f}%"

    def level_marks(self, n):
        return " ".join(["❗️"] * max(0, int(n)))

    def sentiment_pct(self, v, mild=3.0, strong=8.0, extreme=15.0):
        if v is None:
            return ""
        av = abs(v)
        if av >= extreme:
            return self.level_marks(3)
        if av >= strong:
            return self.level_marks(2)
        if av >= mild:
            return self.level_marks(1)
        return ""

    def directional_mark(self, v, mild=5.0, strong=12.0):
        if v is None:
            return ""
        av = abs(v)
        if av < mild:
            return ""
        arrow = "⬆️" if v > 0 else "⬇️"
        return "❗️" + arrow

    def price_mark(self, v, mild=8.0):
        if v is None:
            return ""
        if abs(v) < mild:
            return ""
        return "❗️⬆️" if v > 0 else "❗️⬇️"

    def oi_mark(self, v, mild=1.0):
        if v is None:
            return ""
        if abs(v) < mild:
            return ""
        return "❗️⬆️" if v > 0 else "❗️⬇️"

    def sentiment_accounts(self, long_pct, short_pct):
        if long_pct is None or short_pct is None:
            return ("", "")
        if long_pct > short_pct:
            return ("⬆️", "⬇️")
        if short_pct > long_pct:
            return ("⬇️", "⬆️")
        return ("", "")

    def funding_mark(self, funding):
        if funding is None:
            return ""
        af = abs(funding)
        arrow = "⬆️" if funding > 0 else "⬇️"
        if af == 0:
            return ""
        if af < 0.5:
            return f"❗️ {arrow}"
        if af < 1:
            return f"❗️❗️ {arrow}"
        return f"❗️❗️❗️ {arrow}"

    def mark_usd(self, v, thr):
        prefix = (self.level_marks(3) + " ") if (v is not None and v >= thr) else ""
        return prefix + self.fmt_usd(v)

    def esc(self, s):
        return html.escape(str(s))

    def coinglass_link(self, exchange, symbol):
        ex = "Bybit" if exchange == "BYBIT" else "Binance"
        return f"https://www.coinglass.com/tv/{ex}_{symbol}"

    def bybit_link(self, symbol):
        return f"https://www.bybit.com/trade/usdt/{symbol}"

    def resolve_bybit_symbol(self, symbol):
        try:
            syms = self.bybit_symbols_cache or []
        except Exception:
            syms = []
        if symbol in syms:
            return symbol, "exact"
        base = symbol[:-4] if symbol.endswith("USDT") else symbol
        candidates = [s for s in syms if s.startswith(base)]
        if candidates:
            candidates.sort(key=len)
            return candidates[0], "mapped"
        return None, "na"

    def compact_links(self, exchange, symbol, elapsed_text="", cycle_num=None):
        cg = self.coinglass_link(exchange, symbol)
        by_sym, mode = self.resolve_bybit_symbol(symbol)

        copy_part = f'<code>{self.esc(symbol)}</code>'

        if by_sym:
            by = self.bybit_link(by_sym)
            left = f'🔗 <a href="{cg}">CG</a> | <a href="{by}">BY</a> | {copy_part}'
        else:
            left = f'🔗 <a href="{cg}">CG</a> | BY n/a | {copy_part}'

        parts = [left]
        if elapsed_text:
            parts.append("     " + elapsed_text)
        if cycle_num:
            parts.append("     " + f"{int(cycle_num)}🔄")
        return "".join(parts)

    async def send_timer_anomaly_debug(self, symbol, st, now, elapsed_sec, source_label):
        debug_chat_id = str(self.cfg.get("telegram", {}).get("debug_chat_id", "") or "").strip()
        if not debug_chat_id:
            return

        throttle_sec = 300
        last_debug_ts = float(st.get("anomaly_debug_ts", 0.0) or 0.0)
        if now - last_debug_ts < throttle_sec:
            return

        mins = int(elapsed_sec // 60)
        level = st.get("last_sent_level", 0)
        max_level = st.get("max_level_sent", 0)
        monster_active = bool(st.get("monster_active"))
        cascade_step = int(st.get("cascade_step_sent", 0) or 0)
        last_change_ts = float(st.get("last_level_change_ts", 0.0) or 0.0)
        since_change_min = int((now - last_change_ts) // 60) if last_change_ts else None

        lines = [
            "<b>❌ TIMER ANOMALY</b>",
            f"Символ: <b>{self.esc(symbol)}</b>",
            f"Источник: <b>{self.esc(source_label)}</b>",
            f"Прошло с прошлого сигнала: <b>{mins}м</b>",
            f"range_active: <b>{'True' if st.get('range_active') else 'False'}</b>",
            f"last_sent_level: <b>{level}</b>",
            f"max_level_sent: <b>{max_level}</b>",
            f"monster_active: <b>{'True' if monster_active else 'False'}</b>",
            f"cascade_step_sent: <b>{cascade_step}</b>",
        ]
        if since_change_min is not None:
            lines.append(f"С прошлого meaningful change: <b>{since_change_min}м</b>")
        lines.append("")
        lines.append("Это визуальный флаг сбоя lifecycle/reset-логики.")
        lines.append("По ТЗ после 15м без развития должен быть полный reset и следующий сигнал обязан быть 🆕.")

        message = "\n".join(lines)
        await self.send(message, chat_id=debug_chat_id, count_signal=False, kind="debug")
        st["anomaly_debug_ts"] = now

    async def elapsed_timer_text(self, st, symbol, source_label="", now=None):
        now = now or time.time()
        last_ts = float(st.get("last_sent_ts", 0.0) or 0.0)
        if not last_ts:
            return "🆕"

        elapsed = max(0.0, now - last_ts)
        mins = int(elapsed // 60)
        reset_sec = int(self.cfg["signals"].get("range_reset_sec", 900))

        if st.get("range_active", False):
            if elapsed < reset_sec:
                return f"✅{mins}м"
            await self.send_timer_anomaly_debug(symbol, st, now, elapsed, source_label or "signal")
            return f"❌{mins}м"

        return "🆕"

    # ========= helpers =========
    def prune(self, dq, sec, now):
        while dq and now - dq[0][0] > sec:
            dq.popleft()

    def is_blacklisted(self, symbol):
        bl = set(self.current_blocklist())
        return symbol in bl or symbol.endswith("USDC") or symbol.startswith("BTC") or ("BTC" in symbol)

    # ========= helpers =========
    def prune(self, dq, sec, now):
        while dq and now - dq[0][0] > sec:
            dq.popleft()

    def allow_symbol_global(self, symbol):
        cooldown_sec = int(self.cfg["signals"].get("cross_exchange_cooldown_sec", 90))
        now = time.time()
        last = self.cross_exchange_gate.get(symbol, 0.0)
        if now - last > cooldown_sec:
            self.cross_exchange_gate[symbol] = now
            return True
        return False


    def level_marks(self, n):
        return "".join(["❗️" for _ in range(max(0, int(n)))])

    def trigger_marks(self, level):
        if level >= 7:
            return self.level_marks(3)
        if level >= 5:
            return self.level_marks(2)
        return self.level_marks(1)

    def flow_icon(self, level):
        if level >= 6:
            return "☄️"
        if level >= 4:
            return "🔥"
        return "🛑"

    def level_badge(self, level):
        badges = {
            1: "1️⃣",
            2: "2️⃣",
            3: "3️⃣",
            4: "4️⃣",
            5: "5️⃣",
            6: "6️⃣",
            7: "7️⃣",
            8: "8️⃣",
            9: "9️⃣",
            10: "🔟",
        }
        return badges.get(level, str(level))
    def signal_power_bar(self, level):
        badges = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
        lvl = max(1, min(10, int(level)))
        return "".join(badges[i] if i < lvl else "⬜️" for i in range(10))

    
    def build_state_signature(self, symbol, ex, level, sum15, cnt):
        flow_bucket = int(float(sum15) // 500)
        cnt_bucket = int(int(cnt) // 5)
        return f"{symbol}|{ex}|L{int(level)}|F{flow_bucket}|C{cnt_bucket}"

    def should_skip_state_signal(self, st, state_sig, level, now):
        cooldown = 30 if int(level) == 1 else 15
        return state_sig == st.get("last_state_signature", "") and now - st.get("last_state_ts", 0.0) < cooldown

    def mark_state_signal_sent(self, st, state_sig, now):
        st["last_state_signature"] = state_sig
        st["last_state_ts"] = now

    def compute_level(self, trigger_usd, sum15, _unused1=0, _unused2=0):
        flow_lvl1 = float(self.cfg["signals"].get("level_1_flow_usd", self.cfg["signals"].get("liq_level_1_usd", 9000)))
        if sum15 >= float(self.cfg["signals"].get("level_7_usd", 150000)):
            return 7
        if sum15 >= float(self.cfg["signals"].get("level_6_usd", 120000)):
            return 6
        if sum15 >= float(self.cfg["signals"].get("level_5_usd", 90000)):
            return 5
        if sum15 >= float(self.cfg["signals"].get("level_4_usd", 50000)):
            return 4
        if sum15 >= float(self.cfg["signals"].get("level_3_usd", 30000)):
            return 3
        if sum15 >= float(self.cfg["signals"].get("level_2_usd", 15000)):
            return 2
        if trigger_usd >= float(self.cfg["signals"].get("liq_level_1_usd", 9000)) or sum15 >= flow_lvl1:
            return 1
        return 0

    def level_label(self, level):
        labels = {
            1: "УРОВЕНЬ 1",
            2: "УРОВЕНЬ 2",
            3: "УРОВЕНЬ 3",
            4: "УРОВЕНЬ 4",
            5: "УРОВЕНЬ 5",
            6: "УРОВЕНЬ 6",
            7: "УРОВЕНЬ 7",
            8: "HYPER",
            9: "SUPER HYPER",
            10: "MONSTER",
        }
        return labels.get(level, "нет")

    def should_reset_range(self, st, now):
        if not st["range_active"]:
            return False
        reset_sec = int(self.cfg["signals"].get("range_reset_sec", 600))
        return (now - st["last_level_change_ts"]) >= reset_sec

    def clear_symbol_flow_windows(self, symbol, side="short"):
        to_clear_1m = [k for k in self.events_1m.keys() if len(k) >= 4 and k[0] == "FLOW_LOCAL" and k[2] == symbol and k[3] == side]
        for k in to_clear_1m:
            self.events_1m[k].clear()

        agg_key = ("FLOW_AGG15", symbol, side)
        if agg_key in self.events_15m:
            self.events_15m[agg_key].clear()


    def reset_range(self, state_key):
        symbol = state_key[1] if isinstance(state_key, tuple) and len(state_key) > 1 else None
        st = self.symbol_state[state_key]
        st["range_active"] = False
        st["range_start_ts"] = 0.0
        st["range_exchange"] = ""
        st["last_sent_level"] = 0
        st["max_level_sent"] = 0
        st["last_level_change_ts"] = 0.0
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None
        st["lvl8_active"] = False
        st["lvl9_active"] = False
        st["monster_active"] = False
        st["monster_base_sum15"] = 0.0
        st["cascade_step_sent"] = 0
        st["last_sent_signature"] = ""
        st["last_sent_ts"] = 0.0
        st["last_lvl1_signature"] = ""
        st["last_lvl1_ts"] = 0.0
        st["last_state_signature"] = ""
        st["last_state_ts"] = 0.0
        st["anomaly_debug_ts"] = 0.0
        if symbol:
            self.note_cycle_reset(symbol)


    def open_range(self, state_key, ex, now):
        symbol = state_key[1] if isinstance(state_key, tuple) and len(state_key) > 1 else None
        st = self.symbol_state[state_key]
        st["range_active"] = True
        st["range_start_ts"] = now
        st["range_exchange"] = ex
        st["last_sent_level"] = 0
        st["max_level_sent"] = 0
        st["last_level_change_ts"] = now
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None
        st["lvl8_active"] = False
        st["lvl9_active"] = False
        st["monster_active"] = False
        st["monster_base_sum15"] = 0.0
        st["cascade_step_sent"] = 0
        st["last_sent_signature"] = ""
        st["last_sent_ts"] = 0.0
        st["last_lvl1_signature"] = ""
        st["last_lvl1_ts"] = 0.0
        st["last_state_signature"] = ""
        st["last_state_ts"] = 0.0
        st["anomaly_debug_ts"] = 0.0
        if symbol:
            self.register_cycle_start(symbol)

    def events_word(self, n):
        n = int(n)
        n10 = n % 10
        n100 = n % 100
        if n10 == 1 and n100 != 11:
            return "событие"
        if 2 <= n10 <= 4 and not 12 <= n100 <= 14:
            return "события"
        return "событий"


    def load_poll_offset(self):
        try:
            if TELEGRAM_POLL_OFFSET_FILE.exists():
                return int(TELEGRAM_POLL_OFFSET_FILE.read_text(encoding="utf-8").strip())
        except Exception:
            pass
        return 0

    def save_poll_offset(self, value):
        try:
            TELEGRAM_POLL_OFFSET_FILE.write_text(str(int(value)), encoding="utf-8")
        except Exception:
            pass

    def load_runtime_overrides(self):
        try:
            if RUNTIME_OVERRIDES_FILE.exists():
                with open(RUNTIME_OVERRIDES_FILE, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
        except Exception:
            pass
        return {}

    def save_runtime_overrides(self, overrides):
        with open(RUNTIME_OVERRIDES_FILE, "w", encoding="utf-8") as f:
            yaml.safe_dump(overrides, f, allow_unicode=True, sort_keys=False)

    def parse_override_value(self, key, value):
        parser = self.allowed_override_keys[key]
        if key == "signals.allowed_exchanges":
            values = [x.strip().upper() for x in value.split(",") if x.strip()]
            values = [x for x in values if x in {"BINANCE", "BYBIT"}]
            return values or ["BINANCE", "BYBIT"]
        return parser(value)

    def set_config_value(self, dotted_key, parsed_value):
        section, key = dotted_key.split(".", 1)
        self.cfg.setdefault(section, {})
        self.cfg[section][key] = parsed_value


    def format_limits(self):
        s = self.cfg.get("signals", {})
        f = self.cfg.get("filters", {})
        ex = ",".join(s.get("allowed_exchanges", ["BINANCE", "BYBIT"]))
        mode = str(self.cfg.get("runtime", {}).get("signal_mode", "combat"))
        mode_text = "Fast test" if mode == "fast_test" else ("Power day" if mode == "power_day" else ("OFF" if mode == "off" else "Normal"))
        liq1 = int(s.get("liq_level_1_usd", 9000))
        flow1 = int(s.get("level_1_flow_usd", liq1))
        min_event_effective = int(f.get("min_event_usd", liq1))
        return (
            "<b>Текущие лимиты</b>\n\n"
            f"Режим: {mode_text}\n"
            f"Ликвидация 1: {liq1} или поток {flow1}\n"
            f"Уровень 2: {int(s.get('level_2_usd', 15000))}\n"
            f"Уровень 3: {int(s.get('level_3_usd', 30000))}\n"
            f"Уровень 4: {int(s.get('level_4_usd', 50000))}\n"
            f"Уровень 5: {int(s.get('level_5_usd', 90000))}\n"
            f"Уровень 6: {int(s.get('level_6_usd', 120000))}\n"
            f"Уровень 7: {int(s.get('level_7_usd', 150000))}\n"
            f"HYPER: {int(s.get('hyper_usd', 200000))}\n"
            f"SUPER HYPER: {int(s.get('super_hyper_usd', 300000))}\n"
            f"MONSTER: {int(s.get('monster_3h_usd', 500000))}\n"
            f"Буфер: {int(s.get('range_delay_sec', 30))} сек\n"
            f"Reset диапазона: {int(s.get('range_reset_sec', 900)) // 60} мин\n"
            f"Биржи сигналов: {ex}\n"
            f"Терминал: {int(f.get('min_terminal_usd', 10000))}\n"
            f"Мин. событие: {min_event_effective}\n"
            f"Top 10 минимум: {int(f.get('top30_min_usd', 3000))}\n"
            f"BTC alerts: {'ON' if self.btc_alerts_enabled else 'OFF'}"
        )

    def limit_prompt(self, dotted_key):
        section, key = dotted_key.split(".", 1)
        current = self.cfg.get(section, {}).get(key)
        return (
            f"<b>Введи новое значение</b>\n\n"
            f"<code>{dotted_key}</code>\n"
            f"Текущее: <b>{current}</b>"
        )


    def prune_market_events(self, ex, now):
        dq = self.market_events_30m[ex]
        while dq and now - dq[0]["ts"] > 1800:
            dq.popleft()
        dq4 = self.market_events_4h[ex]
        while dq4 and now - dq4[0]["ts"] > 14400:
            dq4.popleft()


    def format_top_window(self, ex, seconds, title):
        now = time.time()
        self.prune_market_events(ex, now)
        min_usd = float(self.cfg.get("filters", {}).get("top30_min_usd", 3000))
        dq = self.market_events_30m[ex] if seconds <= 1800 else self.market_events_4h[ex]
        agg = {}
        for row in dq:
            sym = row["symbol"]
            agg[sym] = agg.get(sym, 0.0) + float(row["usd"])
        rows = [(sym, usd) for sym, usd in agg.items() if usd >= min_usd]
        rows.sort(key=lambda x: x[1], reverse=True)
        rows = rows[:10]
        header = f"<b>Топ 10 ликвидаций за {title} — {ex}</b>"
        if seconds <= 1800:
            header += "\n<i>с момента запуска бота</i>"
        if not rows:
            return f"{header}\n\nПока пусто от {self.fmt_usd(min_usd)}."
        out = [header]
        for i, (sym, usd) in enumerate(rows, 1):
            cg = self.coinglass_link(ex, sym)
            by_sym, mode = self.resolve_bybit_symbol(sym)
            if by_sym:
                by = self.bybit_link(by_sym)
                by_label = "BY" if mode == "exact" else f"BY→{by_sym}"
                out.append(f'{i}. {sym} — {self.fmt_usd(usd)} [<a href="{cg}">CG</a>] [<a href="{by}">{by_label}</a>]')
            else:
                out.append(f'{i}. {sym} — {self.fmt_usd(usd)} [<a href="{cg}">CG</a>] [BY n/a]')
        return "\n".join(out)

    def format_top30(self, ex):
        return self.format_top_window(ex, 1800, "30м")

    def format_top4h(self, ex):
        return self.format_top_window(ex, 14400, "4ч")

    def load_runtime_blocklist(self):
        if not BLOCKLIST_RUNTIME_FILE.exists():
            return []
        try:
            return json.loads(BLOCKLIST_RUNTIME_FILE.read_text(encoding="utf-8")) or []
        except Exception:
            return []

    def save_runtime_blocklist(self, items):
        try:
            BLOCKLIST_RUNTIME_FILE.write_text(json.dumps(sorted(set(items)), ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Blocklist save error: {e!r}", flush=True)

    def current_blocklist(self):
        base = list(self.cfg.get("symbols", {}).get("blacklist", []))
        extra = self.load_runtime_blocklist()
        merged = sorted(set(base) | set(extra))
        return merged

    def blocklist_text(self):
        items = self.current_blocklist()
        if not items:
            return "<b>Блок лист</b>\n\nПусто."
        out = ["<b>Блок лист</b>", ""]
        for i, sym in enumerate(items, 1):
            out.append(f"{i}. {self.esc(sym)}")
        return "\n".join(out)



    def is_btc_whitelisted(self, symbol):
        return (symbol or "").upper() in BTC_SYMBOLS

    def append_btc_debug(self, action, symbol="", exchange="", event_usd=0.0, agg_sum15=0.0, reason="", sent=False, cooldown_active=False):
        self.btc_debug_history.append({
            "ts": time.time(),
            "time": time.strftime("%H:%M:%S", time.localtime(time.time())),
            "action": action,
            "symbol": symbol,
            "exchange": exchange,
            "event_usd": self.fmt_usd(event_usd) if event_usd else "$0",
            "agg_sum15": self.fmt_usd(agg_sum15) if agg_sum15 else "$0",
            "reason": reason,
            "sent": "yes" if sent else "no",
            "cooldown_active": "yes" if cooldown_active else "no",
        })

    def check_btc_trigger(self, ex, symbol, usd, agg_sum15):
        if not self.is_btc_whitelisted(symbol):
            return None
        if ex == "BINANCE" and float(usd) >= BTC_SINGLE_THRESHOLD:
            return "BINANCE SINGLE"
        if ex == "BYBIT" and float(usd) >= BTC_SINGLE_THRESHOLD:
            return "BYBIT SINGLE"
        if float(agg_sum15) >= BTC_AGG_THRESHOLD:
            return "AGG 15M"
        return None

    def msg_btc_alert(self, reason, ex, usd, agg_sum15, agg_cnt):
        source = self.esc(reason)
        ex_safe = self.esc(ex)
        return (
            f"<b>₿ BTC ALERT - {self.fmt_usd(max(float(usd), float(agg_sum15)))}</b>\n\n"
            f"Источник: <b>{source}</b>\n\n"
            f"💥 Событие: {self.fmt_usd(usd)}\n"
            f"📊 Поток 15м: {self.fmt_usd(agg_sum15)} | {agg_cnt} событий\n"
            f"🏦 Биржа: {ex_safe}\n\n"
            f"⏱ Cooldown: 15m\n\n"
            f"🔗 CG | BY | BTCUSDT"
        )

    async def process_btc_alert_hook(self, ex, symbol, side, usd, agg_sum15, agg_cnt):
        if not self.btc_alerts_enabled:
            if self.is_btc_whitelisted(symbol):
                self.append_btc_debug("btc_alert_disabled", symbol, ex, usd, agg_sum15, "", False, False)
            return

        if not self.is_btc_whitelisted(symbol):
            return

        now = time.time()
        cooldown_active = (now - float(self.btc_last_alert_ts or 0.0)) < BTC_COOLDOWN_SEC
        if cooldown_active:
            self.append_btc_debug("btc_alert_suppressed_cooldown", symbol, ex, usd, agg_sum15, "", False, True)
            return

        reason = self.check_btc_trigger(ex, symbol, usd, agg_sum15)
        if not reason:
            return

        text = self.msg_btc_alert(reason, ex, usd, agg_sum15, agg_cnt)
        message_id = await self.send(text, count_signal=False, kind="btc_alert")
        self.btc_last_alert_ts = now
        self.btc_last_alert_reason = reason
        self.btc_last_alert_exchange = ex
        self.btc_last_alert_amount = max(float(usd), float(agg_sum15))
        action = {
            "BINANCE SINGLE": "btc_alert_binance_single",
            "BYBIT SINGLE": "btc_alert_bybit_single",
            "AGG 15M": "btc_alert_agg_15m",
        }.get(reason, "btc_alert")
        self.append_btc_debug(action, symbol, ex, usd, agg_sum15, reason, True, False)
        self.signal_history.append({
            "ts": now,
            "time": time.strftime("%H:%M:%S", time.localtime(now)),
            "symbol": symbol,
            "exchange": ex,
            "label": "BTC_ALERT",
            "event": self.fmt_usd(usd),
            "flow": self.fmt_usd(agg_sum15),
            "cnt": agg_cnt,
            "mode": str(self.cfg.get("runtime", {}).get("signal_mode", "combat")),
            "mapped": "BTC",
            "message_id": message_id or "",
            "rank": "",
            "price_1h": "",
            "price_4h": "",
            "price_24h": "",
            "oi_now": "",
            "oi_5m": "",
            "oi_4h": "",
            "long_pct": "",
            "short_pct": "",
            "funding": "",
        })

    def help_text(self):
        return (
            "<b>Команды управления</b>\n\n"
            f"Сборка: <b>{BUILD_VERSION}</b> - актуальная версия\n"
            f"Заливка: <b>{BUILD_DATE}</b> - актуальная дата\n\n"
            "/limits — показать лимиты\n"
            "/top30_binance — топ 10 ликвидаций Binance за 30 минут\n"
            "/top30_bybit — топ 10 ликвидаций Bybit за 30 минут\n"
            "/reload — открыть меню reload\n"
            "/export_chat_24h — выгрузить историю чата за 24ч\n"
            "/export_debug_24h — выгрузить debug за 24ч\n"
            "/set section.key value — изменить лимит\n\n"
            "BTC alerts:\n"
            "₿ BTC ON / ₿ BTC OFF\n"
            "threshold: $1M single / agg 15m\n"
            "cooldown: 15m\n"
            "уровни Tiger не используются\n\n"
            "В Help:\n"
            "📛 Блок лист\n"
            "🔕 Mute лист"
        )

    async def telegram_api(self, method, payload):
        url = f"https://api.telegram.org/bot{self.cfg['telegram']['bot_token']}/{method}"
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=60) as resp:
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    print(f"Telegram {method}: {resp.status} {body[:160]}", flush=True)
                    return None
                print(f"Telegram {method}: {resp.status}", flush=True)
                return data

    def export_signals_text(self):
        now = time.time()
        items = [row for row in list(self.signal_history) if now - row.get('ts', 0) <= 86400]
        if not items:
            return "Сигналы за 24ч\n\nПока пусто."
        out = ["Сигналы за 24ч", ""]
        for i, row in enumerate(items[-5000:], 1):
            out.append(
                f"{i}. {row.get('time','')} | {row.get('symbol','')} | {row.get('exchange','')} | "
                f"{row.get('label','')} | event={row.get('event','')} | flow={row.get('flow','')} | cnt={row.get('cnt','')} | "
                f"mode={row.get('mode','')} | bybit={row.get('mapped','')} | tg={row.get('message_id','')} | "
                f"rank={row.get('rank','')} | p1h={row.get('price_1h','')} | p4h={row.get('price_4h','')} | "
                f"p24h={row.get('price_24h','')} | oi5m={row.get('oi_5m','')} | oi4h={row.get('oi_4h','')} | "
                f"long={row.get('long_pct','')} | short={row.get('short_pct','')} | funding={row.get('funding','')}"
            )
        return "\n".join(out)

    def export_signals_csv_text(self):
        now = time.time()
        items = [row for row in list(self.signal_history) if now - row.get('ts', 0) <= 86400]
        cols = ['time','symbol','exchange','label','event','flow','cnt','mode','mapped','message_id','rank','price_1h','price_4h','price_24h','oi_now','oi_5m','oi_4h','long_pct','short_pct','funding']
        sio = io.StringIO()
        w = csv.DictWriter(sio, fieldnames=cols)
        w.writeheader()
        for row in items:
            w.writerow({c: row.get(c, '') for c in cols})
        return sio.getvalue()

    def export_chat_history_text(self):
        now = time.time()
        items = [row for row in list(self.chat_history) if now - row.get("ts", 0) <= 86400]
        if not items:
            return "История чата за 24ч\n\nПока пусто."
        out = ["История чата за 24ч", ""]
        for i, row in enumerate(items[-10000:], 1):
            out.append(f"{i}. {row.get('time','')} | {row.get('dir','')} | {row.get('kind','')} | {row.get('text','')}")
        return "\n".join(out)

    def export_debug_text(self):
        now = time.time()
        out = ["Debug за 24ч", ""]
        signals = [row for row in list(self.signal_history) if now - row.get("ts", 0) <= 86400]
        chats = [row for row in list(self.chat_history) if now - row.get("ts", 0) <= 86400]

        out.append("[signals]")
        if not signals:
            out.append("empty")
        else:
            for i, row in enumerate(signals[-5000:], 1):
                out.append(
                    f"{i}. {row.get('time','')} | signal | {row.get('symbol','')} | {row.get('exchange','')} | "
                    f"{row.get('label','')} | event={row.get('event','')} | flow={row.get('flow','')} | cnt={row.get('cnt','')} | "
                    f"mode={row.get('mode','')} | bybit={row.get('mapped','')} | tg={row.get('message_id','')} | "
                    f"rank={row.get('rank','')} | p1h={row.get('price_1h','')} | p4h={row.get('price_4h','')} | "
                    f"p24h={row.get('price_24h','')} | oi_now={row.get('oi_now','')} | oi5m={row.get('oi_5m','')} | "
                    f"oi4h={row.get('oi_4h','')} | long={row.get('long_pct','')} | short={row.get('short_pct','')} | "
                    f"funding={row.get('funding','')}"
                )

        out.append("")
        out.append("[chat]")
        if not chats:
            out.append("empty")
        else:
            for i, row in enumerate(chats[-10000:], 1):
                out.append(f"{i}. {row.get('time','')} | {row.get('dir','')} | {row.get('kind','')} | {row.get('text','')}")

        out.append("")
        out.append("[runtime]")
        mode = str(self.cfg.get("runtime", {}).get("signal_mode", "combat"))
        out.append(f"mode={mode}")
        out.append(f"blocklist={','.join(self.current_blocklist()) or '-'}")
        out.append(f"mutelist={','.join(self.load_runtime_mute()) or '-'}")
        out.append(f"btc_alerts={'ON' if self.btc_alerts_enabled else 'OFF'}")

        out.append("")
        out.append("[btc]")
        btc_rows = [row for row in list(self.btc_debug_history) if now - row.get("ts", 0) <= 86400]
        if not btc_rows:
            out.append("empty")
        else:
            for i, row in enumerate(btc_rows[-2000:], 1):
                out.append(
                    f"{i}. {row.get('time','')} | {row.get('action','')} | {row.get('symbol','')} | {row.get('exchange','')} | "
                    f"event={row.get('event_usd','')} | agg15={row.get('agg_sum15','')} | reason={row.get('reason','')} | "
                    f"sent={row.get('sent','')} | cooldown={row.get('cooldown_active','')}"
                )
        return "\n".join(out)



    def apply_signal_mode(self, mode_name):
        self.cfg.setdefault("runtime", {})
        self.cfg.setdefault("filters", {})
        self.cfg.setdefault("signals", {})
        self.cfg["runtime"]["signal_mode"] = mode_name

        if mode_name == "fast_test":
            self.cfg["signals"]["liq_level_1_usd"] = 500
            self.cfg["signals"]["level_1_flow_usd"] = 500
            self.cfg["signals"]["level_2_usd"] = 1000
            self.cfg["signals"]["level_3_usd"] = 2000
            self.cfg["signals"]["level_4_usd"] = 3000
            self.cfg["signals"]["level_5_usd"] = 4000
            self.cfg["signals"]["level_6_usd"] = 5000
            self.cfg["signals"]["level_7_usd"] = 10000
            self.cfg["signals"]["hyper_usd"] = 20000
            self.cfg["signals"]["super_hyper_usd"] = 30000
            self.cfg["signals"]["hyper_cooldown_sec"] = 300
            self.cfg["signals"]["super_hyper_cooldown_sec"] = 3600
            self.cfg["signals"]["monster_3h_usd"] = 50000
            self.cfg["signals"]["monster_mute_sec"] = 10800
            self.cfg["signals"]["range_delay_sec"] = 30
            self.cfg["signals"]["range_reset_sec"] = 900
            self.cfg["filters"]["min_event_usd"] = 500
        elif mode_name == "power_day":
            self.cfg["signals"]["liq_level_1_usd"] = 25000
            self.cfg["signals"]["level_1_flow_usd"] = 25000
            self.cfg["signals"]["level_2_usd"] = 35000
            self.cfg["signals"]["level_3_usd"] = 50000
            self.cfg["signals"]["level_4_usd"] = 90000
            self.cfg["signals"]["level_5_usd"] = 120000
            self.cfg["signals"]["level_6_usd"] = 150000
            self.cfg["signals"]["level_7_usd"] = 180000
            self.cfg["signals"]["hyper_usd"] = 200000
            self.cfg["signals"]["super_hyper_usd"] = 300000
            self.cfg["signals"]["hyper_cooldown_sec"] = 300
            self.cfg["signals"]["super_hyper_cooldown_sec"] = 3600
            self.cfg["signals"]["monster_3h_usd"] = 500000
            self.cfg["signals"]["monster_mute_sec"] = 10800
            self.cfg["signals"]["range_delay_sec"] = 30
            self.cfg["signals"]["range_reset_sec"] = 900
            self.cfg["filters"]["min_event_usd"] = 25000
        elif mode_name == "off":
            off = 10000000
            self.cfg["signals"]["liq_level_1_usd"] = off
            self.cfg["signals"]["level_1_flow_usd"] = off
            self.cfg["signals"]["level_2_usd"] = off
            self.cfg["signals"]["level_3_usd"] = off
            self.cfg["signals"]["level_4_usd"] = off
            self.cfg["signals"]["level_5_usd"] = off
            self.cfg["signals"]["level_6_usd"] = off
            self.cfg["signals"]["level_7_usd"] = off
            self.cfg["signals"]["hyper_usd"] = off
            self.cfg["signals"]["super_hyper_usd"] = off
            self.cfg["signals"]["monster_3h_usd"] = off
            self.cfg["signals"]["range_delay_sec"] = 30
            self.cfg["signals"]["range_reset_sec"] = 900
            self.cfg["filters"]["min_event_usd"] = off
        else:
            self.cfg["signals"]["liq_level_1_usd"] = 9000
            self.cfg["signals"]["level_1_flow_usd"] = 10000
            self.cfg["signals"]["level_2_usd"] = 15000
            self.cfg["signals"]["level_3_usd"] = 30000
            self.cfg["signals"]["level_4_usd"] = 50000
            self.cfg["signals"]["level_5_usd"] = 90000
            self.cfg["signals"]["level_6_usd"] = 120000
            self.cfg["signals"]["level_7_usd"] = 150000
            self.cfg["signals"]["hyper_usd"] = 200000
            self.cfg["signals"]["super_hyper_usd"] = 300000
            self.cfg["signals"]["hyper_cooldown_sec"] = 300
            self.cfg["signals"]["super_hyper_cooldown_sec"] = 3600
            self.cfg["signals"]["monster_3h_usd"] = 500000
            self.cfg["signals"]["monster_mute_sec"] = 10800
            self.cfg["signals"]["range_delay_sec"] = 30
            self.cfg["signals"]["range_reset_sec"] = 900
            self.cfg["filters"]["min_event_usd"] = 9000

        return self.format_limits(), self.keyboard


    async def handle_control_command(self, text):
        text = (text or "").strip()

        if self.control_state.get("awaiting_key"):
            if text == "↩️ Назад":
                self.control_state["awaiting_key"] = None
                return self.help_text(), self.keyboard
            key = self.control_state["awaiting_key"]
            if key == "__add_block__":
                items = self.load_runtime_blocklist()
                sym = text.strip().upper()
                if sym and sym not in items:
                    items.append(sym)
                    self.save_runtime_blocklist(items)
                self.control_state["awaiting_key"] = None
                return f"✅ Добавлен в блок лист: <code>{self.esc(sym)}</code>", self.block_menu
            if key == "__remove_block__":
                items = self.load_runtime_blocklist()
                sym = text.strip().upper()
                items = [x for x in items if x != sym]
                self.save_runtime_blocklist(items)
                self.control_state["awaiting_key"] = None
                return f"✅ Убран из блок листа: <code>{self.esc(sym)}</code>", self.block_menu
            if key == "__add_mute__":
                items = self.load_runtime_mute()
                sym = text.strip().upper()
                if sym and sym not in items:
                    items.append(sym)
                    self.save_runtime_mute(items)
                self.control_state["awaiting_key"] = None
                return f"✅ Добавлен в mute лист: <code>{self.esc(sym)}</code>", self.mute_menu
            if key == "__remove_mute__":
                items = self.load_runtime_mute()
                sym = text.strip().upper()
                items = [x for x in items if x != sym]
                self.save_runtime_mute(items)
                self.control_state["awaiting_key"] = None
                return f"✅ Убран из mute листа: <code>{self.esc(sym)}</code>", self.mute_menu
            try:
                parsed = self.parse_override_value(key, text)
                overrides = self.load_runtime_overrides()
                section, k = key.split(".", 1)
                overrides.setdefault(section, {})
                overrides[section][k] = parsed
                self.save_runtime_overrides(overrides)
                self.set_config_value(key, parsed)
                self.control_state["awaiting_key"] = None
                return f"✅ Обновлено\n<code>{key} = {parsed}</code>", self.limit_menu
            except Exception as e:
                return f"❌ Ошибка: {e}\nПовтори ввод или нажми ↩️ Назад", self.limit_menu

        if text in ("🏆 TOP", "/top"):
            return "<b>TOP</b>\n\nВыбери окно и биржу.", self.top_menu
        if text in ("📥 Скачать", "/download"):
            return "<b>Скачать</b>\n\nВыбери тип выгрузки.", self.download_menu
        if text in ("/export_24h", "📤 Сигналы 24ч"):
            payload = self.export_signals_text()
            fname = f"signals_24h_{time.strftime('%Y%m%d_%H%M%S')}.txt"
            ok = await self.send_document_text(fname, payload, caption="<b>Сигналы за 24ч</b>")
            return ("✅ Файл с сигналами за 24ч отправлен." if ok else "❌ Не удалось отправить файл."), self.download_menu
        if text in ("/export_chat_24h", "🗂 Чат 24ч"):
            payload = self.export_chat_history_text()
            fname = f"chat_24h_{time.strftime('%Y%m%d_%H%M%S')}.txt"
            ok = await self.send_document_text(fname, payload, caption="<b>История чата за 24ч</b>")
            return ("✅ Файл с историей чата за 24ч отправлен." if ok else "❌ Не удалось отправить файл."), self.download_menu
        if text in ("/export_debug_24h", "🧠 Debug 24ч"):
            payload = self.export_debug_text()
            fname = f"debug_24h_{time.strftime('%Y%m%d_%H%M%S')}.txt"
            ok = await self.send_document_text(fname, payload, caption="<b>Debug за 24ч</b>")
            return ("✅ Файл debug за 24ч отправлен." if ok else "❌ Не удалось отправить файл."), self.download_menu

        if text in ("⚙️ Режим", "/mode"):
            return "<b>Выбор режима</b>\n\n🟢 Normal — мин. событие = 9000\n🟡 Fast test — мин. событие = 500\n🟠 Power day — мин. событие = 25000\n⚫ OFF — уведомления выключены", self.mode_menu
        if text == "🟢 Normal":
            msg, kb = self.apply_signal_mode("combat")
            return "✅ Режим переключен: <b>Normal</b>\n\n" + msg, kb
        if text == "🟡 Fast test":
            msg, kb = self.apply_signal_mode("fast_test")
            return "✅ Режим переключен: <b>Fast test</b>\n\n" + msg, kb
        if text == "🟠 Power day":
            msg, kb = self.apply_signal_mode("power_day")
            return "✅ Режим переключен: <b>Power day</b>\n\n" + msg, kb
        if text == "⚫ OFF":
            _msg, kb = self.apply_signal_mode("off")
            return "✅ Режим переключен: <b>OFF</b>\n\nУведомления отключены, сбор данных продолжается.", kb
        if text == "₿ BTC ON":
            self.btc_alerts_enabled = True
            return "✅ BTC alerts включены", self.mode_menu
        if text == "₿ BTC OFF":
            self.btc_alerts_enabled = False
            return "✅ BTC alerts выключены", self.mode_menu

        if text in ("/limits", "📊 Лимиты"):
            return self.format_limits(), self.limit_menu
        if text in ("/top30_binance", "/top15_binance", "🏆 BINANCE /30м", "🏆 BINANCE /15м"):
            return self.format_top30("BINANCE"), self.top_menu
        if text in ("/top30_bybit", "/top15_bybit", "🏆 BYBIT /30м", "🏆 BYBIT /15м"):
            return self.format_top30("BYBIT"), self.top_menu
        if text == "🏆 BINANCE /4ч":
            return self.format_top4h("BINANCE"), self.top_menu
        if text == "🏆 BYBIT /4ч":
            return self.format_top4h("BYBIT"), self.top_menu

        if text in ("/reload", "🔄 Reload"):
            return "<b>Reload</b>\n\nВыбери действие.", self.reload_menu
        if text == "🔄 Reload config":
            self.cfg = load()
            return "✅ Конфиг и overrides перечитаны.", self.keyboard
        if text == "♻️ Restart bot":
            return "⛔ Restart bot удалён. Используй только ручной redeploy в Railway.", self.keyboard

        if text in ("❓ Help",):
            self.control_state["awaiting_key"] = None
            return self.help_text(), self.help_menu
        if text in ("/help", "/start", "↩️ Назад"):
            self.control_state["awaiting_key"] = None
            return self.help_text(), self.keyboard
        if text == "📛 Блок лист":
            return "<b>Блок лист</b>\n\nВыбери действие.", self.block_menu
        if text == "📋 Список блока":
            return self.blocklist_text(), self.block_menu
        if text == "➕ Добавить в блок лист":
            self.control_state["awaiting_key"] = "__add_block__"
            return "Пришли тикер для добавления в блок лист", self.block_menu
        if text == "➖ Убрать из блок листа":
            self.control_state["awaiting_key"] = "__remove_block__"
            return "Пришли тикер для удаления из блок листа", self.block_menu
        if text == "🔕 Mute лист":
            return "<b>Mute лист</b>\n\nВыбери действие.", self.mute_menu
        if text == "📋 Список mute":
            return self.mute_text(), self.mute_menu
        if text == "➕ Добавить в mute лист":
            self.control_state["awaiting_key"] = "__add_mute__"
            return "Пришли тикер для добавления в mute лист", self.mute_menu
        if text == "➖ Убрать из mute листа":
            self.control_state["awaiting_key"] = "__remove_mute__"
            return "Пришли тикер для удаления из mute листа", self.mute_menu

        if text in self.limit_button_map:
            key = self.limit_button_map[text]
            self.control_state["awaiting_key"] = key
            return self.limit_prompt(key), self.limit_menu

        if text.startswith("/set "):
            parts = text.split(maxsplit=2)
            if len(parts) != 3:
                return "❌ Формат: /set section.key value", self.keyboard
            key, value = parts[1], parts[2]
            if key not in self.allowed_override_keys:
                return "❌ Этот ключ менять нельзя.", self.keyboard
            try:
                parsed = self.parse_override_value(key, value)
                overrides = self.load_runtime_overrides()
                section, k = key.split(".", 1)
                overrides.setdefault(section, {})
                overrides[section][k] = parsed
                self.save_runtime_overrides(overrides)
                self.set_config_value(key, parsed)
                return f"✅ Обновлено\n<code>{key} = {parsed}</code>", self.keyboard
            except Exception as e:
                return f"❌ Ошибка: {e}", self.keyboard

        return None, None

    async def telegram_control_loop(self):
        offset = self.load_poll_offset()
        expected_chat_id = str(self.cfg.get("telegram", {}).get("chat_id", ""))

        # Fix Telegram 409 conflicts with stale webhook
        await self.telegram_api("deleteWebhook", {"drop_pending_updates": False})

        while not self.stop_event.is_set():
            try:
                data = await self.telegram_api("getUpdates", {
                    "offset": offset,
                    "timeout": 50,
                    "allowed_updates": ["message"],
                })
                if not data:
                    await asyncio.sleep(3)
                    continue
                if not data.get("ok"):
                    desc = str(data.get("description", ""))
                    if "terminated by other getUpdates request" in desc or "Conflict" in desc:
                        await asyncio.sleep(3)
                        continue
                    await asyncio.sleep(3)
                    continue

                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    self.save_poll_offset(offset)
                    msg = upd.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if expected_chat_id and chat_id != expected_chat_id:
                        continue
                    text = msg.get("text", "")
                    self.chat_history.append({
                        "ts": time.time(),
                        "time": time.strftime("%H:%M:%S", time.localtime(time.time())),
                        "dir": "IN",
                        "kind": "command",
                        "text": text[:1500],
                    })
                    reply, markup = await self.handle_control_command(text)
                    if reply:
                        await self.send(reply, reply_markup=markup or self.keyboard)
            except Exception as e:
                print(f"Telegram control error: {e!r}", flush=True)
                await asyncio.sleep(5)

    async def send(self, text, reply_markup=None, chat_id=None, count_signal=True, kind="message"):
        url = f"https://api.telegram.org/bot{self.cfg['telegram']['bot_token']}/sendMessage"
        payload = {
            "chat_id": chat_id or self.cfg["telegram"]["chat_id"],
            "text": text,
            "disable_web_page_preview": True,
            "parse_mode": "HTML",
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=20) as resp:
                body = await resp.text()
                print("Telegram:", resp.status, body[:160], flush=True)
                if resp.status == 200:
                    self.chat_history.append({
                        "ts": time.time(),
                        "time": time.strftime("%H:%M:%S", time.localtime(time.time())),
                        "dir": "OUT",
                        "kind": kind,
                        "text": re.sub(r"<[^>]+>", "", text)[:3000],
                    })
                    self.health["telegram_ok"] = True
                    self.health["last_telegram_ok_ts"] = time.time()
                    if count_signal:
                        self.health["signals_sent"] += 1
                    self.write_health()
                    try:
                        return json.loads(body).get("result", {}).get("message_id")
                    except Exception:
                        return None


    async def send_document_text(self, filename, content, caption=None):
        url = f"https://api.telegram.org/bot{self.cfg['telegram']['bot_token']}/sendDocument"
        form = aiohttp.FormData()
        form.add_field("chat_id", str(self.cfg["telegram"]["chat_id"]))
        if caption:
            form.add_field("caption", caption)
            form.add_field("parse_mode", "HTML")
        form.add_field(
            "document",
            content.encode("utf-8"),
            filename=filename,
            content_type="text/plain",
        )
        async with aiohttp.ClientSession() as s:
            async with s.post(url, data=form, timeout=60) as resp:
                body = await resp.text()
                print("Telegram document:", resp.status, body[:160], flush=True)
                if resp.status == 200:
                    self.chat_history.append({
                        "ts": time.time(),
                        "time": time.strftime("%H:%M:%S", time.localtime(time.time())),
                        "dir": "OUT",
                        "kind": "document",
                        "text": f"{filename} | {caption or ''}"[:3000],
                    })
                    self.health["telegram_ok"] = True
                    self.health["last_telegram_ok_ts"] = time.time()
                    self.write_health()
                    return True
        return False

    # ========= external data =========
    async def get_all_bybit_symbols(self):
        now = time.time()
        if self.bybit_symbols_cache and now - self.bybit_symbols_cache_ts < 3600:
            return self.bybit_symbols_cache

        symbols = []
        cursor = None
        async with aiohttp.ClientSession() as session:
            while True:
                url = BYBIT_INSTRUMENTS_URL + (f"&cursor={cursor}" if cursor else "")
                async with session.get(url, timeout=20) as resp:
                    data = await resp.json()
                result = data.get("result", {})
                for row in result.get("list", []):
                    symbol = row.get("symbol")
                    status = row.get("status")
                    if symbol and status == "Trading":
                        symbols.append(symbol)
                cursor = result.get("nextPageCursor")
                if not cursor:
                    break

        self.bybit_symbols_cache = sorted(set(symbols))
        self.bybit_symbols_cache_ts = now
        return self.bybit_symbols_cache

    async def refresh_rank_cache(self):
        now = time.time()
        if self.rank_cache and now - self.rank_cache_ts < 1800:
            return

        out = {}
        async with aiohttp.ClientSession() as session:
            for page in [1, 2]:
                params = {
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 250,
                    "page": page,
                    "sparkline": "false",
                }
                try:
                    async with session.get(COINGECKO_MARKETS, params=params, timeout=20) as resp:
                        data = await resp.json()
                    for row in data:
                        sym = str(row.get("symbol", "")).upper() + "USDT"
                        rank = row.get("market_cap_rank")
                        if sym and rank:
                            out[sym] = rank
                except Exception:
                    pass

        if out:
            self.rank_cache = out
            self.rank_cache_ts = now

    async def get_market_rank(self, symbol):
        await self.refresh_rank_cache()
        return self.rank_cache.get(symbol)

    def _cache_get(self, key, ttl=20):
        rec = self.binance_cache.get(key)
        if rec and time.time() - rec["ts"] < ttl:
            return rec["data"]
        return None

    def _cache_set(self, key, data):
        self.binance_cache[key] = {"ts": time.time(), "data": data}

    async def fetch_binance_stats(self, symbol):
        cached = self._cache_get(("stats", symbol))
        if cached is not None:
            return cached

        stats = {"1ч": {}, "4ч": {}, "24ч": {}}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(BINANCE_24H, params={"symbol": symbol}, timeout=20) as resp:
                    t24 = await resp.json()
                stats["24ч"] = {
                    "vol_usd": float(t24.get("quoteVolume", 0)),
                    "price_pct": float(t24.get("priceChangePercent", 0)),
                    "vol_pct": None,
                }
            except Exception:
                stats["24ч"] = {"vol_usd": None, "vol_pct": None, "price_pct": None}

            try:
                async with session.get(BINANCE_KLINES, params={"symbol": symbol, "interval": "1m", "limit": 480}, timeout=20) as resp:
                    rows = await resp.json()
                if isinstance(rows, list) and len(rows) >= 240:
                    close_now = float(rows[-1][4])
                    open_1h = float(rows[-60][1])
                    open_4h = float(rows[-240][1])

                    vol_1h = sum(float(x[7]) for x in rows[-60:])
                    vol_4h = sum(float(x[7]) for x in rows[-240:])
                    vol_1h_prev = sum(float(x[7]) for x in rows[-120:-60]) if len(rows) >= 120 else None
                    vol_4h_prev = sum(float(x[7]) for x in rows[-480:-240]) if len(rows) >= 480 else None

                    stats["1ч"] = {
                        "vol_usd": vol_1h,
                        "vol_pct": ((vol_1h - vol_1h_prev) / vol_1h_prev) * 100 if vol_1h_prev else None,
                        "price_pct": ((close_now - open_1h) / open_1h) * 100 if open_1h else None,
                    }
                    stats["4ч"] = {
                        "vol_usd": vol_4h,
                        "vol_pct": ((vol_4h - vol_4h_prev) / vol_4h_prev) * 100 if vol_4h_prev else None,
                        "price_pct": ((close_now - open_4h) / open_4h) * 100 if open_4h else None,
                    }
                else:
                    stats["1ч"] = {"vol_usd": None, "vol_pct": None, "price_pct": None}
                    stats["4ч"] = {"vol_usd": None, "vol_pct": None, "price_pct": None}
            except Exception:
                stats["1ч"] = {"vol_usd": None, "vol_pct": None, "price_pct": None}
                stats["4ч"] = {"vol_usd": None, "vol_pct": None, "price_pct": None}

        self._cache_set(("stats", symbol), stats)
        return stats

    async def fetch_binance_oi(self, symbol):
        cached = self._cache_get(("oi", symbol))
        if cached is not None:
            return cached

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(BINANCE_OI_NOW, params={"symbol": symbol}, timeout=20) as resp:
                    oi_now = await resp.json()
                async with session.get(BINANCE_OI_HIST, params={"symbol": symbol, "period": "5m", "limit": 3}, timeout=20) as resp:
                    oi_hist_5m = await resp.json()
                async with session.get(BINANCE_OI_HIST, params={"symbol": symbol, "period": "4h", "limit": 2}, timeout=20) as resp:
                    oi_hist_4h = await resp.json()

                current_contracts = float(oi_now.get("openInterest", 0))
                current_usd = None
                pct_1m = None
                pct_5m = None
                pct_4h = None

                if isinstance(oi_hist_5m, list) and len(oi_hist_5m) >= 1:
                    current_usd = float(oi_hist_5m[-1].get("sumOpenInterestValue", 0))
                    current_hist_contracts = float(oi_hist_5m[-1].get("sumOpenInterest", 0))
                    if len(oi_hist_5m) >= 2:
                        prev_5m_contracts = float(oi_hist_5m[-2].get("sumOpenInterest", 0))
                        pct_5m = ((current_hist_contracts - prev_5m_contracts) / prev_5m_contracts) * 100 if prev_5m_contracts else None
                    pct_1m = ((current_contracts - current_hist_contracts) / current_hist_contracts) * 100 if current_hist_contracts else None

                if isinstance(oi_hist_4h, list) and len(oi_hist_4h) >= 2:
                    cur_4h_contracts = float(oi_hist_4h[-1].get("sumOpenInterest", 0))
                    prev_4h_contracts = float(oi_hist_4h[-2].get("sumOpenInterest", 0))
                    pct_4h = ((cur_4h_contracts - prev_4h_contracts) / prev_4h_contracts) * 100 if prev_4h_contracts else None
                    if current_usd is None:
                        current_usd = float(oi_hist_4h[-1].get("sumOpenInterestValue", 0))

                result = {"now": current_contracts, "now_usd": current_usd, "pct_1m": pct_1m, "pct_5m": pct_5m, "pct_4h": pct_4h}
            except Exception:
                result = {"now": None, "now_usd": None, "pct_1m": None, "pct_5m": None, "pct_4h": None}

        self._cache_set(("oi", symbol), result)
        return result

    async def fetch_bybit_funding(self, symbol):
        cached = self._cache_get(("funding", "BYBIT", symbol))
        if cached is not None:
            return cached

        by_sym, _mode = self.resolve_bybit_symbol(symbol)
        if not by_sym:
            self._cache_set(("funding", "BYBIT", symbol), None)
            return None

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(BYBIT_TICKERS_URL, params={"category": "linear", "symbol": by_sym}, timeout=20) as resp:
                    data = await resp.json()
                rows = ((data or {}).get("result") or {}).get("list") or []
                row = rows[0] if rows else {}
                value = row.get("fundingRate")
                result = float(value) * 100 if value not in (None, "") else None
            except Exception:
                result = None

        self._cache_set(("funding", "BYBIT", symbol), result)
        return result

    async def fetch_binance_ratio(self, symbol):
        cached = self._cache_get(("ratio", symbol))
        if cached is not None:
            return cached

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(BINANCE_RATIO, params={"symbol": symbol, "period": "5m", "limit": 1}, timeout=20) as resp:
                    data = await resp.json()
                if isinstance(data, list) and data:
                    row = data[-1]
                    result = {
                        "long": float(row.get("longAccount", 0)) * 100,
                        "short": float(row.get("shortAccount", 0)) * 100,
                    }
                else:
                    result = {"long": None, "short": None}
            except Exception:
                result = {"long": None, "short": None}

        self._cache_set(("ratio", symbol), result)
        return result

    async def get_symbol_stats(self, symbol):
        return {
            "rank": await self.get_market_rank(symbol),
            "tf": await self.fetch_binance_stats(symbol),
            "oi": await self.fetch_binance_oi(symbol),
            "funding": await self.fetch_bybit_funding(symbol),
            "ratio": await self.fetch_binance_ratio(symbol),
        }

    # ========= message rendering =========
    def flow_level_name(self, lvl1, lvl5, lvl10):
        parts = []
        if lvl1 > 0:
            parts.append(f"1м Уровень {lvl1}")
        if lvl5 > 0:
            parts.append(f"5м Уровень {lvl5}")
        if lvl10 > 0:
            parts.append(f"10м Уровень {lvl10}")
        return " / ".join(parts) if parts else "нет"


    def render_blocks(self, stats, compact_oi=False):
        rank = stats.get("rank")
        rank_text = f"#{rank}" if rank else ">250 / н/д"

        tf = stats.get("tf", {})
        oi = stats.get("oi", {})
        funding = stats.get("funding")
        ratio = stats.get("ratio", {})

        r1 = tf.get("1ч", {})
        r4 = tf.get("4ч", {})
        r24 = tf.get("24ч", {})
        long_pct = ratio.get("long")
        short_pct = ratio.get("short")

        vol1 = self.pct_with_arrow_marks(r1.get("vol_pct"), [100, 1000, 10000])
        vol4 = self.pct_with_arrow_marks(r4.get("vol_pct"), [100, 1000, 10000])
        px1 = self.pct_with_arrow_marks(r1.get("price_pct"), [10, 25, 100])
        px4 = self.pct_with_arrow_marks(r4.get("price_pct"), [10, 25, 100])
        px24 = self.pct_with_arrow_marks(r24.get("price_pct"), [10, 25, 100])
        oi5 = self.pct_with_arrow_marks(oi.get("pct_5m"), [10, 25, 100])
        oi4 = self.pct_with_arrow_marks(oi.get("pct_4h"), [10, 25, 100])
        acct_long_mark, acct_short_mark = self.sentiment_accounts(long_pct, short_pct)
        funding_text = self.funding_with_marks(funding)

        return (
            f"<b>🏷 Капа-рейтинг:</b> {rank_text}\n\n"
            f"<b>💵 Объём:</b>\n"
            f"1ч: {self.fmt_usd(r1.get('vol_usd'))} | {vol1}\n"
            f"4ч: {self.fmt_usd(r4.get('vol_usd'))} | {vol4}\n\n"
            f"<b>📈 Рост цены:</b>\n"
            f"1ч: {px1}\n"
            f"4ч: {px4}\n"
            f"24ч: {px24}\n\n"
            f"<b>📊 Открытый интерес:</b> сейчас: {self.fmt_usd(oi.get('now_usd'))}\n"
            f"5м: {oi5}\n"
            f"4ч: {oi4}\n\n"
            f"<b>👥 Аккаунты:</b>\n"
            f"{acct_long_mark} лонг: {self.fmt_pct(long_pct)} | {acct_short_mark} шорт: {self.fmt_pct(short_pct)}\n\n"
            f"<b>🩸 Фандинг:</b>\n"
            f"{funding_text}"
        )

    async def maybe_send_startup_checklist(self):
        if self.startup_telegram_sent:
            return
        if not (self.health.get("binance_connected") and self.health.get("bybit_connected")):
            return

        checklist = (
            f"<b>🤖 Mighty Tiger / {BUILD_VERSION}</b>\n<i>Ggrrr... Liquidity jungle hunter</i>\n\n"
            "✅ Telegram OK\n"
            "✅ Binance connected\n"
            "✅ Bybit symbols loaded\n"
            "✅ Bybit subscribe sent\n"
            "✅ Bot ready for signals"
        )
        print("✅ START CHECKLIST", flush=True)
        print("✅ Telegram OK", flush=True)
        print("✅ Binance connected", flush=True)
        print("✅ Bybit symbols loaded", flush=True)
        print("✅ Bybit subscribe sent", flush=True)
        print("✅ Bot ready for signals", flush=True)
        try:
            await self.send(checklist, reply_markup=self.keyboard)
        finally:
            self.startup_telegram_sent = True

    def trigger_log_line(self, symbol, snapshot, hyper=False):
        ex = snapshot["ex"]
        level = 10 if snapshot.get("monster") else (9 if snapshot.get("super_hyper") else (8 if hyper else snapshot["level"]))
        label = "MONSTER" if snapshot.get("monster") else ("SUPER HYPER" if snapshot.get("super_hyper") else ("HYPER" if hyper else self.level_label(level)))
        return (
            f"TRIGGER {symbol} {ex} {label} "
            f"event={self.fmt_usd(snapshot['trigger_usd'])} "
            f"15м={self.fmt_usd(snapshot['sum15'])}"
        )

    def msg_signal(self, symbol, snapshot, stats, hyper=False):
        ex = snapshot["ex"]
        symbol_safe = self.esc(symbol)
        trigger_usd = snapshot["trigger_usd"]
        sum15 = snapshot["sum15"]
        cnt = snapshot["cnt"]
        level = snapshot["level"]
        super_hyper = snapshot.get("super_hyper", False)
        monster = snapshot.get("monster", False)
        cascade_step = int(snapshot.get("cascade_step", 0) or 0)
        cascade_extra_usd = int(snapshot.get("cascade_extra_usd", 0) or 0)
        elapsed_text = snapshot.get("elapsed_text", "")

        cycle_num = snapshot.get("cycle_num")
        if monster:
            top_line = f"💀 {symbol_safe} AGG - {self.fmt_usd(sum15)}"
            header = f"MONSTER {self.signal_power_bar(10)}"
        elif super_hyper:
            top_line = f"🚨 {symbol_safe} AGG - {self.fmt_usd(sum15)}"
            header = f"SUPER HYPER {self.signal_power_bar(9)}"
        elif hyper:
            top_line = f"☄️ {symbol_safe} AGG - {self.fmt_usd(sum15)}"
            header = f"HYPER {self.signal_power_bar(8)}"
        elif level == 1:
            top_line = f"🛑 {symbol_safe} {ex} - {self.fmt_usd(sum15)}"
            header = f"LVL1 {self.signal_power_bar(1)}"
        else:
            icon = "🛑" if level <= 3 else ("🔥" if level <= 5 else "☄️")
            top_line = f"{icon} {symbol_safe} AGG - {self.fmt_usd(sum15)}"
            header = f"LVL{level} {self.signal_power_bar(level)}"

        trigger_flow = f"💥 <b>{self.fmt_usd(sum15)} | {cnt} {self.events_word(cnt)}</b> 💥"
        by_sym, mode = self.resolve_bybit_symbol(symbol)
        mapping_note = ""
        if mode == "mapped" and by_sym:
            mapping_note = f"\n⚠️ Bybit аналог: <b>{self.esc(by_sym)}</b>"
        elif mode == "na":
            mapping_note = "\n⚠️ Нет точного тикера на Bybit"

        cascade_line = f"\n<b>КАСКАД: +{cascade_extra_usd:,}$</b>".replace(",", " ") if monster else ""

        return (
            f"<b>{top_line}</b>\n"
            f"<b>{header}</b>{cascade_line}\n\n"
            f"Событие: {self.fmt_usd(trigger_usd)}\n"
            f"Поток 15м: {trigger_flow}{mapping_note}\n\n"
            f"{self.render_blocks(stats, compact_oi=True)}\n\n"
            f"{self.compact_links(ex if level == 1 else 'BYBIT', symbol, elapsed_text=elapsed_text, cycle_num=cycle_num)}"
        )

    def maybe_queue_level(self, state_key, snapshot):
        st = self.symbol_state[state_key]
        level = snapshot["level"]
        if level <= 0:
            return
        current_pending = st["pending_level"]
        ceiling = max(st.get("last_sent_level", 0), st.get("max_level_sent", 0))
        if level > ceiling and level >= current_pending:
            st["pending_level"] = level
            st["pending_since_ts"] = snapshot["now"]
            st["pending_snapshot"] = snapshot

    async def maybe_send_pending(self, symbol, state_key):
        st = self.symbol_state[state_key]
        if st["pending_level"] <= max(st["last_sent_level"], st.get("max_level_sent", 0)) or not st["pending_snapshot"]:
            return

        delay_sec = int(self.cfg["signals"].get("range_delay_sec", 30))
        now = time.time()
        if now - st["pending_since_ts"] < delay_sec:
            return

        snap = dict(st["pending_snapshot"])
        if snap.get("level") == 10:
            snap["monster"] = True
            if float(st.get("monster_base_sum15", 0.0) or 0.0) <= 0:
                st["monster_base_sum15"] = float(snap["sum15"])
                st["cascade_step_sent"] = 0
            snap["cascade_step"] = int(st.get("cascade_step_sent", 0) or 0)
            snap["cascade_extra_usd"] = int(st.get("cascade_step_sent", 0) or 0) * 100000
            snap["cascade_base_sum15"] = float(st.get("monster_base_sum15", 0.0) or 0.0)
        elif snap.get("level") == 9:
            snap["super_hyper"] = True
        signature = f"{symbol}|{snap['ex']}|{snap['level']}|{round(float(snap['sum15']),1)}"
        state_sig = self.build_state_signature(symbol, snap["ex"], snap["level"], snap["sum15"], snap["cnt"])

        if snap["level"] == 1 and signature == st.get("last_lvl1_signature", "") and now - st.get("last_lvl1_ts", 0.0) < 30:
            st["pending_level"] = 0
            st["pending_since_ts"] = 0.0
            st["pending_snapshot"] = None
            return
        if signature == st.get("last_sent_signature", "") and now - st.get("last_sent_ts", 0.0) < 180:
            st["pending_level"] = 0
            st["pending_since_ts"] = 0.0
            st["pending_snapshot"] = None
            return
        if self.should_skip_state_signal(st, state_sig, snap["level"], now):
            st["pending_level"] = 0
            st["pending_since_ts"] = 0.0
            st["pending_snapshot"] = None
            return

        snap["cycle_num"] = self.register_cycle_start(symbol)
        stats = await self.get_symbol_stats(symbol)
        tf = stats.get("tf", {})
        oi = stats.get("oi", {})
        ratio = stats.get("ratio", {})
        by_sym, _mode = self.resolve_bybit_symbol(symbol)
        row = {
            "ts": now, "time": time.strftime("%H:%M:%S", time.localtime(now)),
            "symbol": symbol, "exchange": snap["ex"], "label": ("MONSTER" if snap['level']==10 else ("SUPER HYPER" if snap['level']==9 else ("HYPER" if snap['level']==8 else f"LVL{snap['level']}"))),
            "event": self.fmt_usd(snap["trigger_usd"]), "flow": self.fmt_usd(snap["sum15"]), "cnt": snap["cnt"],
            "mode": str(self.cfg.get("runtime", {}).get("signal_mode", "combat")),
            "mapped": by_sym or "BY n/a",
            "message_id": "",
            "rank": stats.get("rank",""),
            "price_1h": self.fmt_pct(tf.get("1ч", {}).get("price_pct")),
            "price_4h": self.fmt_pct(tf.get("4ч", {}).get("price_pct")),
            "price_24h": self.fmt_pct(tf.get("24ч", {}).get("price_pct")),
            "oi_now": self.fmt_usd(oi.get("now_usd")),
            "oi_5m": self.fmt_pct(oi.get("pct_5m")),
            "oi_4h": self.fmt_pct(oi.get("pct_4h")),
            "long_pct": self.fmt_pct(ratio.get("long")),
            "short_pct": self.fmt_pct(ratio.get("short")),
            "funding": self.fmt_pct(stats.get("funding")),
        }
        self.signal_history.append(row)

        snap["elapsed_text"] = await self.elapsed_timer_text(
            st,
            symbol,
            source_label=("MONSTER" if snap.get("monster") else ("SUPER HYPER" if snap.get("super_hyper") else ("HYPER" if snap.get("level") == 8 else f"LVL{snap.get('level', 0)}"))),
            now=now,
        )
        print(self.trigger_log_line(symbol, snap, hyper=False), flush=True)
        message_id = None if self.is_muted(symbol) else await self.send(self.msg_signal(symbol, snap, stats, hyper=False))
        row["message_id"] = message_id or ""

        st["last_sent_level"] = st["pending_level"]
        st["max_level_sent"] = max(st.get("max_level_sent", 0), st["last_sent_level"])
        st["last_level_change_ts"] = now
        st["last_sent_signature"] = signature
        st["last_sent_ts"] = now
        self.mark_state_signal_sent(st, state_sig, now)
        if snap["level"] == 10:
            st["monster_active"] = True
        if snap["level"] == 1:
            st["last_lvl1_signature"] = signature
            st["last_lvl1_ts"] = now
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None


    async def maybe_send_hyper(self, symbol, state_key, snapshot):
        st = self.symbol_state[state_key]
        sum15 = float(snapshot["sum15"])
        lvl = 0
        if sum15 >= float(self.cfg["signals"].get("super_hyper_usd", 300000)):
            lvl = 9
        elif sum15 >= float(self.cfg["signals"].get("hyper_usd", 200000)):
            lvl = 8
        if lvl <= 0:
            return
        snap = dict(snapshot)
        snap["level"] = lvl
        self.maybe_queue_level(state_key, snap)

    async def maybe_send_cascade(self, symbol, state_key, snapshot):
        st = self.symbol_state[state_key]
        if st.get("last_sent_level", 0) < 10:
            return

        base = float(st.get("monster_base_sum15", 0.0) or 0.0)
        if base <= 0:
            return

        cascade_step = int(snapshot.get("cascade_step", 0) or 0)
        if cascade_step <= int(st.get("cascade_step_sent", 0) or 0):
            return

        snap = dict(snapshot)
        snap["level"] = 10
        snap["monster"] = True
        snap["cascade_step"] = cascade_step
        snap["cascade_extra_usd"] = cascade_step * 100000

        now = time.time()
        signature = f"{symbol}|{snap['ex']}|MONSTER|C{cascade_step}|{round(float(snap['sum15']),1)}"
        snap["cycle_num"] = self.register_cycle_start(symbol)
        stats = await self.get_symbol_stats(symbol)

        tf = stats.get("tf", {})
        oi = stats.get("oi", {})
        ratio = stats.get("ratio", {})
        by_sym, _mode = self.resolve_bybit_symbol(symbol)
        row = {
            "ts": now, "time": time.strftime("%H:%M:%S", time.localtime(now)),
            "symbol": symbol, "exchange": snap["ex"], "label": f"MONSTER +{cascade_step * 100}k",
            "event": self.fmt_usd(snap["trigger_usd"]), "flow": self.fmt_usd(snap["sum15"]), "cnt": snap["cnt"],
            "mode": str(self.cfg.get("runtime", {}).get("signal_mode", "combat")),
            "mapped": by_sym or "BY n/a",
            "message_id": "",
            "rank": stats.get("rank",""),
            "price_1h": self.fmt_pct(tf.get("1ч", {}).get("price_pct")),
            "price_4h": self.fmt_pct(tf.get("4ч", {}).get("price_pct")),
            "price_24h": self.fmt_pct(tf.get("24ч", {}).get("price_pct")),
            "oi_now": self.fmt_usd(oi.get("now_usd")),
            "oi_5m": self.fmt_pct(oi.get("pct_5m")),
            "oi_4h": self.fmt_pct(oi.get("pct_4h")),
            "long_pct": self.fmt_pct(ratio.get("long")),
            "short_pct": self.fmt_pct(ratio.get("short")),
            "funding": self.fmt_pct(stats.get("funding")),
        }
        self.signal_history.append(row)

        snap["elapsed_text"] = await self.elapsed_timer_text(
            st,
            symbol,
            source_label=f"MONSTER +{cascade_step * 100}k",
            now=now,
        )

        print(self.trigger_log_line(symbol, snap, hyper=False) + f" CASCADE +{cascade_step * 100}k", flush=True)
        message_id = None if self.is_muted(symbol) else await self.send(self.msg_signal(symbol, snap, stats, hyper=False))
        row["message_id"] = message_id or ""

        st["cascade_step_sent"] = cascade_step
        st["last_level_change_ts"] = now
        st["last_sent_signature"] = signature
        st["last_sent_ts"] = now
    async def pending_flush_loop(self):
        while not self.stop_event.is_set():
            try:
                for state_key, st in list(self.symbol_state.items()):
                    if not st.get("range_active"):
                        continue
                    if not st.get("pending_snapshot"):
                        continue
                    symbol = state_key[1]
                    lock = self.state_locks.setdefault(state_key, asyncio.Lock())
                    async with lock:
                        await self.maybe_send_pending(symbol, state_key)
            except Exception as e:
                print(f"Pending flush error: {e!r}", flush=True)
            await asyncio.sleep(1.0)


    def update_monster(self, state_key, now, usd):
        dq = self.events_3h[state_key]
        dq.append((now, usd))
        while dq and now - dq[0][0] > 18000:
            dq.popleft()
        total = sum(v for _, v in dq)
        thr = float(self.cfg["signals"].get("monster_3h_usd", 500000))
        return total, total >= thr

    async def handle(self, ex, symbol, side, usd):
        now = time.time()

        allowed_exchanges = set(self.cfg.get("signals", {}).get("allowed_exchanges", ["BINANCE", "BYBIT"]))
        if ex not in allowed_exchanges:
            return

        local_key = ("FLOW_LOCAL", ex, symbol, side)
        agg15_key = ("FLOW_AGG15", symbol, side)

        self.events_15m[agg15_key].append((now, usd))
        self.prune(self.events_15m[agg15_key], 900, now)
        agg_sum15 = sum(v for _, v in self.events_15m[agg15_key])
        agg_cnt = len(self.events_15m[agg15_key])

        await self.process_btc_alert_hook(ex, symbol, side, usd, agg_sum15, agg_cnt)

        if self.is_blacklisted(symbol):
            return

        if side == "short":
            row_event = {"ts": now, "symbol": symbol, "usd": usd}
            self.market_events_30m[ex].append(row_event)
            self.market_events_4h[ex].append(dict(row_event))
            self.prune_market_events(ex, now)

        state_key = ("STATE", symbol)
        lock = self.state_locks.setdefault(state_key, asyncio.Lock())

        async with lock:
            st = self.symbol_state[state_key]
            if self.should_reset_range(st, now):
                self.reset_range(state_key)
                self.clear_symbol_flow_windows(symbol, side)

            self.events_1m[local_key].append((now, usd))

            self.prune(self.events_1m[local_key], 60, now)

            local_sum1 = sum(v for _, v in self.events_1m[local_key])

            if side != "short":
                return

            if usd >= float(self.cfg["filters"]["min_terminal_usd"]):
                print(f"{ex:<7} {symbol:<18} SHORT {self.fmt_usd(usd)} | local1м={self.fmt_usd(local_sum1)} agg15м={self.fmt_usd(agg_sum15)}", flush=True)

            st = self.symbol_state[state_key]

            entry_threshold = float(self.cfg["signals"].get("liq_level_1_usd", 9000))
            flow_entry_threshold = float(self.cfg["signals"].get("level_1_flow_usd", entry_threshold))
            min_event_usd = float(self.cfg.get("filters", {}).get("min_event_usd", entry_threshold))
            local_entry_hit = usd >= entry_threshold or local_sum1 >= flow_entry_threshold

            if usd < min_event_usd and not local_entry_hit and not st["range_active"]:
                return

            monster_threshold = float(self.cfg["signals"].get("monster_3h_usd", 500000))

            if not st["range_active"]:
                if not local_entry_hit:
                    return
                self.open_range(state_key, ex, now)
                first_sum = max(local_sum1, agg_sum15, usd)
                first_level = max(1, self.compute_level(entry_threshold, first_sum))
                if float(first_sum) >= monster_threshold:
                    first_level = 10
                elif float(first_sum) >= float(self.cfg["signals"].get("super_hyper_usd", 300000)):
                    first_level = 9
                elif float(first_sum) >= float(self.cfg["signals"].get("hyper_usd", 200000)):
                    first_level = 8
                entry_snapshot = {
                    "now": now,
                    "ex": ex,
                    "trigger_usd": max(usd, entry_threshold),
                    "sum15": first_sum,
                    "cnt": max(len(self.events_1m[local_key]), agg_cnt),
                    "level": first_level,
                }
                self.maybe_queue_level(state_key, entry_snapshot)
                await self.maybe_send_pending(symbol, state_key)
                return

            if st["last_sent_level"] < 1:
                await self.maybe_send_pending(symbol, state_key)
                return

            level = self.compute_level(entry_threshold, agg_sum15)
            if float(agg_sum15) >= monster_threshold:
                level = 10
            elif float(agg_sum15) >= float(self.cfg["signals"].get("super_hyper_usd", 300000)):
                level = 9
            elif float(agg_sum15) >= float(self.cfg["signals"].get("hyper_usd", 200000)):
                level = 8

            snapshot = {
                "now": now,
                "ex": ex,
                "trigger_usd": max(usd, entry_threshold),
                "sum15": agg_sum15,
                "cnt": agg_cnt,
                "level": level,
            }

            if level < 2 and st.get("last_sent_level", 0) < 10:
                await self.maybe_send_pending(symbol, state_key)
                return

            if st.get("last_sent_level", 0) >= 10:
                snapshot["level"] = 10
                base = float(st.get("monster_base_sum15", 0.0) or 0.0)
                if base > 0:
                    extra = max(0.0, float(agg_sum15) - base)
                    cascade_step = int(extra // 100000)
                    snapshot["cascade_step"] = cascade_step
                    snapshot["cascade_extra_usd"] = cascade_step * 100000
                    next_step_usd = (int(st.get("cascade_step_sent", 0) or 0) + 1) * 100000
                    to_next = max(0.0, next_step_usd - extra)
                    if cascade_step > int(st.get("cascade_step_sent", 0) or 0):
                        await self.maybe_send_cascade(symbol, state_key, snapshot)
                    elif usd >= float(self.cfg["filters"]["min_terminal_usd"]):
                        print(
                            f"CASCADE_WAIT {symbol} base={self.fmt_usd(base)} flow={self.fmt_usd(agg_sum15)} "
                            f"extra={self.fmt_usd(extra)} next=+{next_step_usd//1000}k left={self.fmt_usd(to_next)}",
                            flush=True,
                        )
                await self.maybe_send_pending(symbol, state_key)
                return

            if 2 <= level <= 10 and level > max(st["last_sent_level"], st.get("max_level_sent", 0)):
                self.maybe_queue_level(state_key, snapshot)

            await self.maybe_send_pending(symbol, state_key)

    async def run_binance(self):
        while True:
            try:
                print("🔄 Connecting Binance...", flush=True)
                async with websockets.connect(BINANCE_WS, ping_interval=20, ping_timeout=20) as ws:
                    print("✅ Binance connected", flush=True)
                    self.health["binance_connected"] = True
                    self.write_health()
                    await self.maybe_send_startup_checklist()

                    async for msg in ws:
                        self.health["last_binance_msg_ts"] = time.time()
                        data = json.loads(msg)
                        items = data if isinstance(data, list) else [data]

                        for item in items:
                            if item.get("e") != "forceOrder":
                                continue
                            o = item["o"]
                            symbol = o["s"]
                            price = float(o["ap"])
                            qty = float(o["z"])
                            usd = price * qty
                            side = "long" if o["S"] == "SELL" else "short"
                            await self.handle("BINANCE", symbol, side, usd)
            except Exception as e:
                self.health["binance_connected"] = False
                self.write_health()
                print("Binance error:", repr(e), flush=True)
                await asyncio.sleep(3)

    async def run_bybit(self):
        while True:
            try:
                print("🔄 Loading Bybit symbols...", flush=True)
                symbols = await self.get_all_bybit_symbols()
                print(f"✅ Bybit symbols loaded: {len(symbols)}", flush=True)

                print("🔄 Connecting Bybit...", flush=True)
                async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                    chunk_size = 50
                    for i in range(0, len(symbols), chunk_size):
                        chunk = symbols[i:i + chunk_size]
                        await ws.send(json.dumps({"op": "subscribe", "args": [f"allLiquidation.{s}" for s in chunk]}))
                        await asyncio.sleep(0.15)

                    print("✅ Bybit subscribe sent", flush=True)
                    self.health["bybit_connected"] = True
                    self.write_health()
                    await self.maybe_send_startup_checklist()

                    async for msg in ws:
                        self.health["last_bybit_msg_ts"] = time.time()
                        data = json.loads(msg)
                        if "success" in data or "op" in data or "data" not in data:
                            continue

                        for d in data["data"]:
                            symbol = d.get("s") or d.get("symbol")
                            if not symbol:
                                continue
                            price = float(d.get("p") or d.get("price"))
                            qty = float(d.get("v") or d.get("size"))
                            usd = price * qty
                            raw_side = d.get("S") or d.get("side")
                            side = "long" if str(raw_side).lower() == "buy" else "short"
                            await self.handle("BYBIT", symbol, side, usd)
            except Exception as e:
                self.health["bybit_connected"] = False
                self.write_health()
                print("Bybit error:", repr(e), flush=True)
                await asyncio.sleep(5)

    async def run(self):
        print(f"✅ BOT STARTED / {BUILD_VERSION}", flush=True)
        await asyncio.gather(self.run_binance(), self.run_bybit(), self.watchdog_loop(), self.telegram_control_loop(), self.pending_flush_loop())


async def main():
    ensure_single_instance()
    bot = Bot(load())

    def handle_stop(*_args):
        bot.stop_event.set()
        cleanup_lock()
        raise SystemExit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle_stop)
        except Exception:
            pass

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())

