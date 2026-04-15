#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import atexit
import asyncio
import html
import json
import os
import signal
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

import aiohttp
import websockets
import yaml

BINANCE_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"

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
TELEGRAM_POLL_OFFSET_FILE = RUNTIME / "telegram_update_offset.txt"


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

    if env_token:
        cfg.setdefault("telegram", {})
        cfg["telegram"]["bot_token"] = env_token
    if env_chat_id:
        cfg.setdefault("telegram", {})
        cfg["telegram"]["chat_id"] = env_chat_id

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
            "last_level_change_ts": 0.0,
            "pending_level": 0,
            "pending_since_ts": 0.0,
            "pending_snapshot": None,
            "hyper_cooldown_until": 0.0,
            "monster_mute_until": 0.0,
        })

        self.binance_cache = {}
        self.rank_cache = {}
        self.rank_cache_ts = 0
        self.bybit_symbols_cache = None
        self.bybit_symbols_cache_ts = 0
        self.stop_event = asyncio.Event()
        self.startup_telegram_sent = False
        self.market_events_30m = {"BINANCE": deque(), "BYBIT": deque()}
        self.control_state = {"awaiting_key": None}
        self.limit_button_map = {
            "1️⃣ Уровень 1": "signals.liq_level_1_usd",
            "2️⃣ Уровень 2": "signals.level_2_usd",
            "3️⃣ Уровень 3": "signals.level_3_usd",
            "4️⃣ Уровень 4": "signals.level_4_usd",
            "5️⃣ Уровень 5": "signals.level_5_usd",
            "6️⃣ Уровень 6": "signals.level_6_usd",
            "7️⃣ Уровень 7": "signals.level_7_usd",
            "💥 Hyper": "signals.hyper_usd",
            "🐋 Monster": "signals.monster_3h_usd",
            "⏱ Monster mute": "signals.monster_mute_sec",
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
            "signals.hyper_cooldown_sec": int,
            "signals.monster_3h_usd": int,
            "signals.monster_mute_sec": int,
            "signals.range_delay_sec": int,
            "signals.range_reset_sec": int,
            "signals.allowed_exchanges": str,
        }
        self.keyboard = {
            "keyboard": [
                [{"text": "🏆 BINANCE /30м"}, {"text": "🏆 BYBIT /30м"}],
                [{"text": "📊 Лимиты"}],
                [{"text": "🔄 Reload"}, {"text": "❓ Help"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
        }
        self.limit_menu = {
            "keyboard": [
                [{"text": "1️⃣ Уровень 1"}, {"text": "2️⃣ Уровень 2"}],
                [{"text": "3️⃣ Уровень 3"}, {"text": "4️⃣ Уровень 4"}],
                [{"text": "5️⃣ Уровень 5"}, {"text": "6️⃣ Уровень 6"}],
                [{"text": "7️⃣ Уровень 7"}, {"text": "💥 Hyper 5м"}],
                [{"text": "🐋 Monster 3ч"}, {"text": "⏱ Monster mute"}],
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

    # ========= runtime =========
    def write_health(self):
        try:
            HEALTH_FILE.write_text(json.dumps(self.health, ensure_ascii=False, indent=2), encoding="utf-8")
            HEARTBEAT_FILE.write_text(str(int(time.time())), encoding="utf-8")
        except Exception:
            pass

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

            await asyncio.sleep(5)

    # ========= format =========
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

    def sentiment_accounts(self, long_pct, short_pct):
        if long_pct is None or short_pct is None:
            return ""
        skew = abs(long_pct - short_pct)
        if skew >= 30:
            return self.level_marks(3)
        if skew >= 20:
            return self.level_marks(2)
        if skew >= 10:
            return self.level_marks(1)
        return ""

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

    def compact_links(self, exchange, symbol):
        cg = self.coinglass_link(exchange, symbol)
        by = self.bybit_link(symbol)
        return f'🔗 <a href="{cg}">CG</a> | <a href="{by}">BY</a>'

    # ========= helpers =========
    def prune(self, dq, sec, now):
        while dq and now - dq[0][0] > sec:
            dq.popleft()

    def is_blacklisted(self, symbol):
        bl = set(self.cfg.get("symbols", {}).get("blacklist", []))
        return symbol in bl or symbol.endswith("USDC")

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

    def compute_level(self, trigger_usd, sum15, _unused1=0, _unused2=0):
        if sum15 >= float(self.cfg["signals"].get("level_7_usd", 110000)):
            return 7
        if sum15 >= float(self.cfg["signals"].get("level_6_usd", 90000)):
            return 6
        if sum15 >= float(self.cfg["signals"].get("level_5_usd", 70000)):
            return 5
        if sum15 >= float(self.cfg["signals"].get("level_4_usd", 50000)):
            return 4
        if sum15 >= float(self.cfg["signals"].get("level_3_usd", 30000)):
            return 3
        if sum15 >= float(self.cfg["signals"].get("level_2_usd", 15000)):
            return 2
        if trigger_usd >= float(self.cfg["signals"].get("liq_level_1_usd", 9000)):
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
            8: "ГИПЕР ЛИКВИДАЦИЯ",
            9: "Монстр",
        }
        return labels.get(level, "нет")

    def should_reset_range(self, st, now):
        if not st["range_active"]:
            return False
        reset_sec = int(self.cfg["signals"].get("range_reset_sec", 600))
        return (now - st["last_level_change_ts"]) >= reset_sec

    def reset_range(self, state_key):
        st = self.symbol_state[state_key]
        st["range_active"] = False
        st["range_start_ts"] = 0.0
        st["range_exchange"] = ""
        st["last_sent_level"] = 0
        st["last_level_change_ts"] = 0.0
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None

    def open_range(self, state_key, ex, now):
        st = self.symbol_state[state_key]
        st["range_active"] = True
        st["range_start_ts"] = now
        st["range_exchange"] = ex
        st["last_sent_level"] = 0
        st["last_level_change_ts"] = now
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None

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
        return (
            "<b>Текущие лимиты</b>\n\n"
            f"Ликвидация 1: {int(s.get('liq_level_1_usd', 9000))}\n"
            f"Уровень 2: {int(s.get('level_2_usd', 15000))}\n"
            f"Уровень 3: {int(s.get('level_3_usd', 30000))}\n"
            f"Уровень 4: {int(s.get('level_4_usd', 50000))}\n"
            f"Уровень 5: {int(s.get('level_5_usd', 70000))}\n"
            f"Уровень 6: {int(s.get('level_6_usd', 90000))}\n"
            f"Уровень 7: {int(s.get('level_7_usd', 110000))}\n"
            f"Hyper: {int(s.get('hyper_usd', 150000))}\n"
            f"Super Hyper: {int(s.get('super_hyper_usd', 300000))}\n"
            f"Monster 3ч: {int(s.get('monster_3h_usd', 500000))}\n"
            f"Monster mute: {int(s.get('monster_mute_sec', 18000)) // 3600} ч\n"
            f"Буфер: {int(s.get('range_delay_sec', 30))} сек\n"
            f"Reset диапазона: {int(s.get('range_reset_sec', 900)) // 60} мин\n"
            f"Биржи сигналов: {ex}\n"
            f"Терминал: {int(f.get('min_terminal_usd', 10000))}\n"
            f"Мин. событие: {int(f.get('min_event_usd', 10000))}\n"
            f"Top 10 минимум: {int(f.get('top30_min_usd', 1000))}"
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

    def format_top30(self, ex):
        now = time.time()
        self.prune_market_events(ex, now)
        min_usd = float(self.cfg.get("filters", {}).get("top30_min_usd", 1000))
        rows = [r for r in self.market_events_30m[ex] if r["usd"] >= min_usd]
        rows = sorted(rows, key=lambda x: x["usd"], reverse=True)[:10]
        header = f"<b>Топ 10 ликвидаций за 30м — {ex}</b>\n<i>с момента запуска бота</i>"
        if not rows:
            return f"{header}\n\nПока пусто от {self.fmt_usd(min_usd)}."
        out = [header]
        for i, row in enumerate(rows, 1):
            sym = row['symbol']
            cg = self.coinglass_link(ex, sym)
            by = self.bybit_link(sym)
            out.append(f"{i}. {sym} — {self.fmt_usd(row['usd'])} [<a href=\"{cg}\">CG</a>] [<a href=\"{by}\">BY</a>]")
        return "\n".join(out)

    def help_text(self):
        return (
            "<b>Команды управления</b>\n\n"
            "/limits — показать лимиты\n"
            "/top30_binance — топ 10 ликвидаций Binance за 30 минут\n"
            "/top30_bybit — топ 10 ликвидаций Bybit за 30 минут\n"
            "/reload — перечитать config + runtime_overrides\n"
            "/set section.key value — изменить лимит\n"
            "Примеры:\n"
            "<code>/set signals.level_5_usd 80000</code>\n"
            "<code>/set signals.allowed_exchanges BINANCE,BYBIT</code>"
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

    async def handle_control_command(self, text):
        text = (text or "").strip()

        if self.control_state.get("awaiting_key"):
            if text == "↩️ Назад":
                self.control_state["awaiting_key"] = None
                return self.format_limits(), self.limit_menu
            key = self.control_state["awaiting_key"]
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

        if text in ("/limits", "📊 Лимиты"):
            return self.format_limits(), self.limit_menu
        if text in ("/top30_binance", "/top15_binance", "🏆 BINANCE /30м", "🏆 BINANCE /15м"):
            return self.format_top30("BINANCE"), self.keyboard
        if text in ("/top30_bybit", "/top15_bybit", "🏆 BYBIT /30м", "🏆 BYBIT /15м"):
            return self.format_top30("BYBIT"), self.keyboard
        if text in ("/reload", "🔄 Reload"):
            self.cfg = load()
            return "✅ Конфиг и overrides перечитаны.", self.keyboard
        if text in ("/help", "❓ Help", "/start", "↩️ Назад"):
            self.control_state["awaiting_key"] = None
            return self.help_text(), self.keyboard

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
                    reply, markup = await self.handle_control_command(text)
                    if reply:
                        await self.send(reply, reply_markup=markup or self.keyboard)
            except Exception as e:
                print(f"Telegram control error: {e!r}", flush=True)
                await asyncio.sleep(5)

    async def send(self, text, reply_markup=None):
        url = f"https://api.telegram.org/bot{self.cfg['telegram']['bot_token']}/sendMessage"
        payload = {
            "chat_id": self.cfg["telegram"]["chat_id"],
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
                    self.health["telegram_ok"] = True
                    self.health["last_telegram_ok_ts"] = time.time()
                    self.health["signals_sent"] += 1
                    self.write_health()

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
                    oi_hist = await resp.json()

                current_contracts = float(oi_now.get("openInterest", 0))
                current_usd = None
                pct_1m = None
                pct_5m = None

                if isinstance(oi_hist, list) and len(oi_hist) >= 1:
                    current_usd = float(oi_hist[-1].get("sumOpenInterestValue", 0))
                    if len(oi_hist) >= 2:
                        prev_5m = float(oi_hist[-2].get("sumOpenInterestValue", 0))
                        pct_5m = ((current_usd - prev_5m) / prev_5m) * 100 if prev_5m else None
                    current_hist_contracts = float(oi_hist[-1].get("sumOpenInterest", 0))
                    pct_1m = ((current_contracts - current_hist_contracts) / current_hist_contracts) * 100 if current_hist_contracts else None

                result = {"now": current_usd, "pct_1m": pct_1m, "pct_5m": pct_5m}
            except Exception:
                result = {"now": None, "pct_1m": None, "pct_5m": None}

        self._cache_set(("oi", symbol), result)
        return result

    async def fetch_binance_funding(self, symbol):
        cached = self._cache_get(("funding", symbol))
        if cached is not None:
            return cached

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(BINANCE_FUNDING, params={"symbol": symbol}, timeout=20) as resp:
                    data = await resp.json()
                result = float(data.get("lastFundingRate", 0)) * 100
            except Exception:
                result = None

        self._cache_set(("funding", symbol), result)
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
            "funding": await self.fetch_binance_funding(symbol),
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

    def render_blocks(self, stats):
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

        vol1_marks = self.directional_mark(r1.get("vol_pct"), 30, 80)
        vol4_marks = self.directional_mark(r4.get("vol_pct"), 30, 80)

        px1_marks = self.directional_mark(r1.get("price_pct"), 8, 20)
        px4_marks = self.directional_mark(r4.get("price_pct"), 10, 25)
        px24_marks = self.directional_mark(r24.get("price_pct"), 15, 35)

        oi5_marks = self.directional_mark(oi.get("pct_5m"), 3, 8)

        acct_marks = self.sentiment_accounts(long_pct, short_pct)
        funding_marks = self.sentiment_pct(funding, 0.03, 0.08, 0.15)

        return (
            f"<b>🏷 Капа-рейтинг:</b> {rank_text}\n\n"
            f"<b>💵 Объём:</b>\n"
            f"1ч: {self.fmt_usd(r1.get('vol_usd'))} | {vol1_marks} {self.fmt_pct(r1.get('vol_pct'))}\n"
            f"4ч: {self.fmt_usd(r4.get('vol_usd'))} | {vol4_marks} {self.fmt_pct(r4.get('vol_pct'))}\n\n"
            f"<b>📈 Рост цены:</b>\n"
            f"1ч: {px1_marks} {self.fmt_pct(r1.get('price_pct'))}\n"
            f"4ч: {px4_marks} {self.fmt_pct(r4.get('price_pct'))}\n"
            f"24ч: {px24_marks} {self.fmt_pct(r24.get('price_pct'))}\n\n"
            f"<b>📊 Открытый интерес:</b>\n"
            f"сейчас: {self.fmt_usd(oi.get('now'))}\n"
            f"5м: {oi5_marks} {self.fmt_pct(oi.get('pct_5m'))}\n\n"
            f"<b>👥 Аккаунты:</b>\n"
            f"{acct_marks} лонг: {self.fmt_pct(long_pct)} | шорт: {self.fmt_pct(short_pct)}\n\n"
            f"<b>🩸 Фандинг:</b>\n"
            f"{funding_marks} {self.fmt_pct(funding)}"
        )

    async def maybe_send_startup_checklist(self):
        if self.startup_telegram_sent:
            return
        if not (self.health.get("binance_connected") and self.health.get("bybit_connected")):
            return

        checklist = (
            "<b>🤖 LIQ BOT / V5.4_level_fix</b>\n\n"
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
        level = 9 if snapshot.get("super_hyper") else (8 if hyper else snapshot["level"])
        label = "SUPER_HYPER" if snapshot.get("super_hyper") else ("HYPER" if hyper else f"LVL{level}")
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

        if super_hyper:
            top_line = f"🚨 9️⃣ {symbol_safe} AGG"
            header = f"SUPER HYPER — ПОТОК — {self.fmt_usd(sum15)}"
        elif hyper:
            top_line = f"🚨 8️⃣ {symbol_safe} AGG"
            header = f"HYPER — ПОТОК — {self.fmt_usd(sum15)}"
        elif level == 1:
            top_line = f"🛑 1️⃣ {symbol_safe} {ex}"
            header = f"УРОВЕНЬ 1 — ПОТОК — {self.fmt_usd(sum15)}"
        else:
            icon = "🛑" if level <= 3 else ("🔥" if level <= 5 else "☄️")
            top_line = f"{icon} {self.level_badge(level)} {symbol_safe} AGG"
            header = f"УРОВЕНЬ {level} — ПОТОК — {self.fmt_usd(sum15)}"

        trigger_flow = f"💥 <b>{self.fmt_usd(sum15)} | {cnt} {self.events_word(cnt)}</b> 💥"

        return (
            f"<b>{top_line}</b>\n"
            f"<b>{header}</b>\n\n"
            f"Событие: {self.fmt_usd(trigger_usd)}\n"
            f"Поток 15м: {trigger_flow}\n\n"
            f"{self.render_blocks(stats)}\n\n"
            f"{self.compact_links(ex if level == 1 else 'BYBIT', symbol)}"
        )

    def maybe_queue_level(self, state_key, snapshot):
        st = self.symbol_state[state_key]
        level = snapshot["level"]
        if level <= 0:
            return
        current_pending = st["pending_level"]
        if level > st["last_sent_level"] and level >= current_pending:
            st["pending_level"] = level
            st["pending_since_ts"] = snapshot["now"]
            st["pending_snapshot"] = snapshot
            st["last_level_change_ts"] = snapshot["now"]

    async def maybe_send_pending(self, symbol, state_key):
        st = self.symbol_state[state_key]
        if st["pending_level"] <= st["last_sent_level"] or not st["pending_snapshot"]:
            return

        delay_sec = int(self.cfg["signals"].get("range_delay_sec", 30))
        now = time.time()
        if now - st["pending_since_ts"] < delay_sec:
            return

        snap = st["pending_snapshot"]
        stats = await self.get_symbol_stats(symbol)
        print(self.trigger_log_line(symbol, snap, hyper=False), flush=True)
        await self.send(self.msg_signal(symbol, snap, stats, hyper=False))
        st["last_sent_level"] = st["pending_level"]
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None

    async def maybe_send_hyper(self, symbol, state_key, snapshot):
        st = self.symbol_state[state_key]
        now = snapshot["now"]
        hyper_usd = float(self.cfg["signals"].get("hyper_usd", 150000))
        super_hyper_usd = float(self.cfg["signals"].get("super_hyper_usd", 300000))
        if snapshot["sum15"] < hyper_usd:
            return
        if now < st["hyper_cooldown_until"]:
            return

        stats = await self.get_symbol_stats(symbol)
        is_super = snapshot["sum15"] >= super_hyper_usd
        snap = dict(snapshot)
        snap["super_hyper"] = is_super
        print(self.trigger_log_line(symbol, snap, hyper=True), flush=True)
        await self.send(self.msg_signal(symbol, snap, stats, hyper=True))
        st["hyper_cooldown_until"] = now + int(self.cfg["signals"].get("hyper_cooldown_sec", 600))
        st["last_sent_level"] = max(st["last_sent_level"], 9 if is_super else 8)
        st["pending_level"] = 0
        st["pending_since_ts"] = 0.0
        st["pending_snapshot"] = None
        st["last_level_change_ts"] = now

    def update_monster(self, state_key, now, usd):
        dq = self.events_3h[state_key]
        dq.append((now, usd))
        while dq and now - dq[0][0] > 10800:
            dq.popleft()
        total = sum(v for _, v in dq)
        st = self.symbol_state[state_key]
        if total >= float(self.cfg["signals"].get("monster_3h_usd", 500000)):
            st["monster_mute_until"] = now + int(self.cfg["signals"].get("monster_mute_sec", 86400))
            self.reset_range(state_key)
            return True
        return now < st["monster_mute_until"]

    async def handle(self, ex, symbol, side, usd):
        if self.is_blacklisted(symbol):
            return

        now = time.time()
        if side == "short":
            self.market_events_30m[ex].append({"ts": now, "symbol": symbol, "usd": usd})
            self.prune_market_events(ex, now)

        bybit_symbols = await self.get_all_bybit_symbols()
        if symbol not in bybit_symbols:
            return

        allowed_exchanges = set(self.cfg.get("signals", {}).get("allowed_exchanges", ["BINANCE", "BYBIT"]))
        if ex not in allowed_exchanges:
            return

        local_key = ("FLOW_LOCAL", ex, symbol, side)
        agg15_key = ("FLOW_AGG15", symbol, side)
        state_key = ("STATE", symbol)

        self.events_1m[local_key].append((now, usd))
        self.events_15m[agg15_key].append((now, usd))

        self.prune(self.events_1m[local_key], 60, now)
        self.prune(self.events_15m[agg15_key], 900, now)

        local_sum1 = sum(v for _, v in self.events_1m[local_key])
        agg_sum15 = sum(v for _, v in self.events_15m[agg15_key])
        agg_cnt = len(self.events_15m[agg15_key])

        if side != "short":
            return

        if usd >= float(self.cfg["filters"]["min_terminal_usd"]):
            print(f"{ex:<7} {symbol:<18} SHORT {self.fmt_usd(usd)} | local1м={self.fmt_usd(local_sum1)} agg15м={self.fmt_usd(agg_sum15)}", flush=True)

        if self.update_monster(state_key, now, usd):
            return

        st = self.symbol_state[state_key]

        if self.should_reset_range(st, now):
            self.reset_range(state_key)

        entry_threshold = float(self.cfg["signals"].get("liq_level_1_usd", 9000))
        local_entry_hit = usd >= entry_threshold or local_sum1 >= entry_threshold

        if not st["range_active"]:
            if not local_entry_hit:
                return
            self.open_range(state_key, ex, now)
            entry_snapshot = {
                "now": now,
                "ex": ex,
                "trigger_usd": usd,
                "sum15": max(local_sum1, usd),
                "cnt": len(self.events_1m[local_key]),
                "level": 1,
            }
            self.maybe_queue_level(state_key, entry_snapshot)
            await self.maybe_send_pending(symbol, state_key)
            return

        if st["last_sent_level"] < 1:
            await self.maybe_send_pending(symbol, state_key)
            return

        level = self.compute_level(0, agg_sum15)
        if level < 2:
            await self.maybe_send_pending(symbol, state_key)
            return

        snapshot = {
            "now": now,
            "ex": ex,
            "trigger_usd": usd,
            "sum15": agg_sum15,
            "cnt": agg_cnt,
            "level": level,
        }

        await self.maybe_send_hyper(symbol, state_key, snapshot)

        if level > st["last_sent_level"]:
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
        print("✅ BOT STARTED / V5.4_level_fix", flush=True)
        await asyncio.gather(self.run_binance(), self.run_bybit(), self.watchdog_loop(), self.telegram_control_loop())


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
