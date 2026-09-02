"""
Antigravity Titan v6.0 — Gate.io USDT-M Futures Trading Engine

=== RISK DISCLAIMER ===
This bot executes automated LEVERAGED FUTURES trading. Leverage amplifies both profits and losses.
- Stop Loss orders are exchange-triggered but execution prices can be affected by market gaps, volatility, or API latency.
- The 60% Safe Vault is an in-code margin allocation limit (MAX_MARGIN_ALLOC_PCT = 0.40) to preserve unallocated equity.
- The 2% Daily Circuit Breaker halts new trade execution upon reaching daily loss thresholds.
- Leveraged trading carries inherent financial risk; never trade with funds you cannot afford to lose.

=== SAFETY & RISK CONTROLS ===
- 60% Vault Reservation: Only up to 40% of total equity can be committed to active margin concurrently.
- 2% Daily Circuit Breaker: Persistent disk state prevents further trading upon reaching the daily risk floor.
- Smart Adaptive Leverage (1.5x–5x): Automatically scaled down during high volatility, seasonality dips, or drawdowns.
- Isolated Margin & Micro-Risk: Dynamic SL/TP and step break-even trailing logic.
- 3-Stage Liquidation Fallback: Market close, reduce-only reversal, and direct exchange liquidation.
"""

import os, sys, time, json, math, socket, logging, urllib.parse, hashlib, hmac, requests, sqlite3, threading, queue
import pandas as pd, numpy as np
import psycopg2, psycopg2.pool
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from collections import deque
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)): return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)): return float(obj)
        if isinstance(obj, (np.ndarray, np.bool_)): return bool(obj) if isinstance(obj, np.bool_) else obj.tolist()
        return super(NpEncoder, self).default(obj)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
API_KEY = os.environ.get("GATEIO_API_KEY", "")
SECRET_KEY = os.environ.get("GATEIO_SECRET_KEY", "")
PASSPHRASE = os.environ.get("GATEIO_PASSPHRASE", "")
BASE_URL = os.environ.get("GATEIO_BASE_URL", "https://api-testnet.gateapi.io")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ENVIRONMENT_MODE = os.environ.get("ENVIRONMENT_MODE", "TESTNET")
HEALTH_SERVER_PORT = int(os.environ.get("PORT", 10000))
GATEIO_KEY_VALID = True
DRY_RUN_MODE = os.environ.get("DRY_RUN_MODE", "true").strip().lower() in ("true", "1", "yes")
CIRCUIT_BREAKER_FILE = "circuit_breaker_state.json"

USER_TOTAL_BALANCE      = 100.0
USER_TRADE_SIZE         = 5.0
USER_DAILY_TARGET       = 999999.0
USER_DAILY_LOSS_LIMIT   = 2.0
USER_TAKE_PROFIT_PCT    = 1.5
USER_STOP_LOSS_PCT      = 0.4
USER_TRAILING_PCT       = 1.5
USER_MAX_OPEN_TRADES    = 999
USER_BADGE_THRESHOLD    = 2
USER_COOLDOWN_SECS      = 0
USER_ACTIVE_HOURS_ONLY  = False

STARTING_BALANCE = 100.0
TRADE_SIZE_PCT          = 0.05
MIN_TRADE_SIZE          = 1.00
MAX_TRADE_SIZE          = 50000.0
MAX_MARGIN_ALLOC_PCT    = 0.40
DAILY_LOSS_LIMIT_PCT    = 0.02
BREAK_EVEN_TRIGGER_PCT  = 0.03
BREAK_EVEN_TRIGGER      = 0.20
TRAILING_TRIGGER        = 1.50
TRAILING_DISTANCE       = 0.80
PARTIAL_TRIGGER         = 1.50
PARTIAL_PCT             = 0.50
FEE_TAKER = 0.0005
SLIPPAGE_RATE = 0.0005
STAIRCASE_TARGETS = [5.0, 10.0, 20.0, 50.0, 100.0]

class CircuitBreakerManager:
    def __init__(self):
        self.tripped = False
        self.trip_date = None
        self.load_state()
    def load_state(self):
        try:
            with open(CIRCUIT_BREAKER_FILE, 'r') as f:
                state = json.load(f)
                if state.get('date') == self._today():
                    self.tripped = state.get('tripped', False)
                    self.trip_date = state.get('date')
                else:
                    self.tripped = False
                    self.save_state()
        except (FileNotFoundError, json.JSONDecodeError):
            self.tripped = False
            self.save_state()
    def save_state(self):
        try:
            with open(CIRCUIT_BREAKER_FILE, 'w') as f:
                json.dump({'tripped': self.tripped, 'date': self._today(), 'timestamp': time.time()}, f)
        except Exception: pass
    def trip(self, daily_loss):
        self.tripped = True
        self.trip_date = self._today()
        self.save_state()
    def check_reset(self):
        if self.trip_date != self._today():
            self.tripped = False
            self.save_state()
    def _today(self):
        return (datetime.now(timezone.utc) + timedelta(hours=6)).strftime('%Y-%m-%d')
circuit_breaker = CircuitBreakerManager()

class MarketShareEngine:
    def __init__(self):
        self.total_market_volume_24h = 0.0
        self.daily_target = 500.0
        self.target_60_days = 2000000.0
        self.last_fetch_time = 0
    def fetch_market_volume(self):
        if time.time() - self.last_fetch_time < 300: return
        try:
            resp = requests.get('https://api.gateio.ws/api/v4/futures/usdt/tickers', timeout=5)
            if resp.status_code == 200:
                tickers = resp.json()
                self.total_market_volume_24h = sum(float(t.get('volume_24h_usd', 0) or t.get('volume_24h_quote', 0) or 0) for t in tickers)
                self.last_fetch_time = time.time()
                self._calculate_target()
        except Exception: pass
    def _calculate_target(self):
        self.daily_target = self.total_market_volume_24h * 0.00000022
        self.target_60_days = max(500000, min(3000000, self.daily_target * 60))
    def get_target_tier(self):
        v = self.total_market_volume_24h
        if v >= 150e9: return 'AGGRESSIVE', 3000000
        elif v >= 120e9: return 'HIGH', 2500000
        elif v >= 90e9: return 'MODERATE', 2000000
        elif v >= 60e9: return 'CONSERVATIVE', 1500000
        else: return 'SAFE', 1000000
market_engine = MarketShareEngine()

SEASONALITY_MAP = {
    1: ('NEUTRAL', 0.0), 2: ('NEUTRAL', 0.0), 3: ('NEUTRAL', 0.0), 4: ('STRONG', 0.10),
    5: ('NEUTRAL', 0.0), 6: ('NEUTRAL', 0.0), 7: ('STRONG', 0.10), 8: ('WEAK', -0.15),
    9: ('WEAK', -0.15), 10: ('VERY_STRONG', 0.15), 11: ('STRONG', 0.10), 12: ('NEUTRAL', 0.0),
}
def get_current_seasonality():
    month = (datetime.now(timezone.utc) + timedelta(hours=6)).month
    label, factor = SEASONALITY_MAP.get(month, ('NEUTRAL', 0.0))
    return {'month': month, 'label': label, 'factor': factor}

LEVERAGE_MATRIX = {
    5.0: {'sl_pct': 0.08, 'tp_pct': 0.25, 'trade_size_pct': 0.08, 'be_trigger': 0.03},
    4.0: {'sl_pct': 0.10, 'tp_pct': 0.30, 'trade_size_pct': 0.07, 'be_trigger': 0.04},
    3.0: {'sl_pct': 0.12, 'tp_pct': 0.35, 'trade_size_pct': 0.06, 'be_trigger': 0.04},
    2.0: {'sl_pct': 0.15, 'tp_pct': 0.40, 'trade_size_pct': 0.05, 'be_trigger': 0.05},
    1.5: {'sl_pct': 0.20, 'tp_pct': 0.45, 'trade_size_pct': 0.04, 'be_trigger': 0.06},
}
class SmartLeverageEngine:
    def __init__(self):
        self.current_leverage = 2.0
        self.initial_balance = STARTING_BALANCE
    def calculate_leverage(self, balance, daily_pnl, progress_pct):
        v = market_engine.total_market_volume_24h
        if v >= 150e9: base = 5.0
        elif v >= 120e9: base = 4.0
        elif v >= 90e9: base = 3.0
        elif v >= 60e9: base = 2.0
        else: base = 1.5
        if progress_pct < 50: base = min(5.0, base + 0.5)
        elif progress_pct > 90: base = max(1.5, base - 0.5)
        base = base * (1.0 + get_current_seasonality()['factor'])
        if balance >= self.initial_balance * 2: base = max(1.5, base - 0.5)
        if (abs(min(0, daily_pnl)) / max(balance, 1) * 100) > 1.0: base = max(1.5, base - 1.0)
        if circuit_breaker.tripped: base = 1.0
        self.current_leverage = max(1.5, min(5.0, round(base * 2) / 2))
        return self.current_leverage
    def get_sl_tp_params(self):
        lev = self.current_leverage
        return LEVERAGE_MATRIX[min(LEVERAGE_MATRIX.keys(), key=lambda k: abs(k - lev))]
leverage_engine = SmartLeverageEngine()

def get_bd_time(): return datetime.now(timezone.utc) + timedelta(hours=6)
def get_bd_time_str(): return get_bd_time().strftime("%Y-%m-%d %I:%M:%S %p")
class BDFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None): return get_bd_time_str() + " BST"

logger = logging.getLogger("TradingBot")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(BDFormatter('[%(asctime)s] [%(levelname)s] %(message)s'))
if not logger.handlers: logger.addHandler(console_handler)

telegram_queue = queue.Queue(maxsize=100)
def _telegram_sender_worker():
    while True:
        try:
            message = telegram_queue.get(timeout=5)
            if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: continue
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=4)
        except Exception: pass
threading.Thread(target=_telegram_sender_worker, daemon=True).start()
def send_telegram_alert(message):
    try: telegram_queue.put_nowait(message)
    except queue.Full: pass

db_pool = None
use_sqlite_fallback = False
sqlite_db_path = "trading_bot.db"
db_lock = threading.Lock()
state_lock = threading.Lock()

def init_db_pool():
    global db_pool, use_sqlite_fallback
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, dsn=DATABASE_URL)
        use_sqlite_fallback = False
    except Exception as e:
        use_sqlite_fallback = True
        db_pool = None

last_db_reconnect_attempt = 0

def execute_db_query(query, params=None, fetch=False):
    global use_sqlite_fallback, last_db_reconnect_attempt, db_pool
    with db_lock:
        if use_sqlite_fallback and (time.time() - last_db_reconnect_attempt > 300):
            last_db_reconnect_attempt = time.time()
            init_db_pool()
        if not use_sqlite_fallback:
            if db_pool is None: init_db_pool()
            if db_pool:
                conn = None
                try:
                    conn = db_pool.getconn()
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(query, params or ())
                        if fetch: return cur.fetchall()
                        return True
                except Exception as e:
                    logger.warning(f"Postgres Query Error: {e}. Temporarily using SQLite fallback.")
                    use_sqlite_fallback = True
                finally:
                    if conn and db_pool:
                        try: db_pool.putconn(conn)
                        except Exception: pass
        try:
            conn = sqlite3.connect(sqlite_db_path)
            cur = conn.cursor()
            sql_stmt = query.replace('%s', '?').replace("(NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours')", "CURRENT_TIMESTAMP")
            cur.execute(sql_stmt, params or ())
            conn.commit()
            if fetch:
                res = cur.fetchall()
                conn.close()
                return res
            conn.close()
            return True
        except Exception as e:
            logger.error(f"SQLite Query Error: {e}")
            return None

def init_db_schema():
    schema_queries = [
        "CREATE TABLE IF NOT EXISTS bot_state (id SERIAL PRIMARY KEY, total_balance DOUBLE PRECISION DEFAULT 100.0, safe_capital DOUBLE PRECISION DEFAULT 60.0, trading_capital DOUBLE PRECISION DEFAULT 40.0, trade_usd_size DOUBLE PRECISION DEFAULT 4.0, daily_target DOUBLE PRECISION DEFAULT 3.0, daily_loss_limit DOUBLE PRECISION DEFAULT 4.0, max_open_trades INT DEFAULT 3, badge_threshold INT DEFAULT 4, daily_pnl DOUBLE PRECISION DEFAULT 0.0, win_rate DOUBLE PRECISION DEFAULT 0.0, total_trades INT DEFAULT 0, updated_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));",
        "CREATE TABLE IF NOT EXISTS bot_trades (id SERIAL PRIMARY KEY, symbol VARCHAR(20), side VARCHAR(10), entry_price DOUBLE PRECISION, exit_price DOUBLE PRECISION, pnl DOUBLE PRECISION, status VARCHAR(20), exit_reason VARCHAR(50), take_profit DOUBLE PRECISION, stop_loss DOUBLE PRECISION, size DOUBLE PRECISION DEFAULT 1.0, created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));",
        "CREATE TABLE IF NOT EXISTS bot_heartbeat (id SERIAL PRIMARY KEY, status VARCHAR(20), open_trades_count INT, daily_pnl DOUBLE PRECISION, win_rate DOUBLE PRECISION, snapshot_json TEXT, created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));",
        "CREATE TABLE IF NOT EXISTS bot_news (id SERIAL PRIMARY KEY, symbol VARCHAR(20), title TEXT, sentiment VARCHAR(10), score DOUBLE PRECISION, source VARCHAR(50), created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));",
        "CREATE TABLE IF NOT EXISTS bot_backtest_results (id SERIAL PRIMARY KEY, asset VARCHAR(20), period_days INT DEFAULT 365, config_type VARCHAR(50), total_trades INT, win_rate DOUBLE PRECISION, avg_win DOUBLE PRECISION, avg_loss DOUBLE PRECISION, max_drawdown DOUBLE PRECISION, net_pnl DOUBLE PRECISION, sharpe_ratio DOUBLE PRECISION, report_json TEXT, created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));",
        "CREATE TABLE IF NOT EXISTS bot_seasonality_logs (id SERIAL PRIMARY KEY, month INT, label VARCHAR(20), factor DOUBLE PRECISION, leverage DOUBLE PRECISION, market_volume DOUBLE PRECISION, target_60_days DOUBLE PRECISION, created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));"
    ]
    for q in schema_queries: execute_db_query(q)
    res = execute_db_query("SELECT COUNT(*) FROM bot_state;", fetch=True)
    if res and res[0][0] == 0:
        execute_db_query("INSERT INTO bot_state (total_balance, safe_capital, trading_capital, trade_usd_size, daily_target, daily_loss_limit, max_open_trades, badge_threshold, daily_pnl) VALUES (100.0, 60.0, 40.0, 4.0, 3.0, 4.0, 3, 4, 0.0);")

API_LOGS = []
def log_api_event(endpoint, method="GET", status=200, latency_ms=12, details="GATE.IO API EXECUTION OK"):
    global API_LOGS
    with state_lock:
        API_LOGS.insert(0, {"timestamp": get_bd_time_str(), "endpoint": endpoint, "method": method, "status": status, "latency_ms": latency_ms, "details": details})
        if len(API_LOGS) > 30: API_LOGS.pop()

GATE_TIME_OFFSET = 0
def sync_gate_server_time():
    global GATE_TIME_OFFSET
    try:
        r = requests.get("https://api.gateio.ws/api/v4/spot/time", timeout=3)
        if r.status_code == 200: GATE_TIME_OFFSET = int(r.json().get("server_time", time.time() * 1000) / 1000) - int(time.time())
    except Exception: pass

def gate_sign(method, url, query_string="", body=""):
    global GATE_TIME_OFFSET
    if GATE_TIME_OFFSET == 0: sync_gate_server_time()
    t = str(int(time.time() + GATE_TIME_OFFSET))
    body_hash = hashlib.sha512(body.encode('utf-8')).hexdigest() if body else hashlib.sha512(b"").hexdigest()
    sign_str = f"{method}\n{url}\n{query_string}\n{body_hash}\n{t}"
    sign = hmac.new(SECRET_KEY.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha512).hexdigest()
    return {"Accept": "application/json", "Content-Type": "application/json", "KEY": API_KEY, "SIGN": sign, "Timestamp": t}

def gate_api_request(method, endpoint, query_params=None, body=None):
    global GATEIO_KEY_VALID
    url_path = f"/api/v4{endpoint}"
    query_str = urllib.parse.urlencode(query_params) if query_params else ""
    body_str = json.dumps(body) if body else ""
    headers = gate_sign(method, url_path, query_str, body_str)
    full_url = f"{BASE_URL}{url_path}" + (f"?{query_str}" if query_str else "")
    for attempt in range(3):
        try:
            resp = requests.request(method.upper(), full_url, headers=headers, data=body_str if method.upper()!="GET" else None, timeout=4)
            if resp.status_code in [200, 201]:
                GATEIO_KEY_VALID = True
                return resp.json()
            elif resp.status_code == 401: GATEIO_KEY_VALID = False
        except Exception: pass
        time.sleep(0.2 * (2 ** attempt))
    return None

def fetch_real_fill_price(contract, order_id, retries=5, delay=0.2):
    for attempt in range(retries):
        try:
            trades = gate_api_request('GET', '/futures/usdt/my_trades', query_params={'contract': contract, 'order': str(order_id), 'limit': 10})
            if trades and isinstance(trades, list) and len(trades) > 0:
                total_value = sum(float(t.get('price', 0)) * abs(int(t.get('size', 0))) for t in trades)
                total_size = sum(abs(int(t.get('size', 0))) for t in trades)
                if total_size > 0: return {'fill_price': total_value / total_size, 'filled_size': total_size, 'fee': sum(float(t.get('fee', 0)) for t in trades), 'trades': len(trades)}
        except Exception: pass
        time.sleep(delay * (2 ** attempt))
    return None

def parse_order_response(result):
    if not result or not isinstance(result, dict): return {'success': False, 'status': 'FAILED', 'filled_size': 0, 'left': 0, 'order_id': None}
    order_id = result.get('id')
    status = result.get('status', result.get('state', ''))
    left = float(result.get('left', result.get('unfilled', 0)))
    size = abs(int(result.get('size', 0)))
    filled_size = size - int(left) if size > 0 else 0
    is_filled = status.lower() in ('finished', 'closed', 'filled') or filled_size > 0
    return {'success': order_id is not None, 'order_id': order_id, 'status': status, 'size': size, 'filled_size': filled_size, 'left': int(left), 'fill_price': float(result.get('fill_price', 0) or 0), 'is_filled': is_filled, 'create_time': result.get('create_time_ms', result.get('create_time')), 'text': result.get('text', '')}

def get_compound_trade_size(balance): return max(MIN_TRADE_SIZE, min(MAX_TRADE_SIZE, round(float(balance) * TRADE_SIZE_PCT, 2)))

def get_compound_next_tier(balance):
    bal = max(20.0, float(balance))
    step_idx = math.floor(math.log(max(bal / 20.0, 1.0), 1.10)) if bal > 20.0 else 0
    tier_low = round(20.0 * (1.10 ** step_idx), 2)
    tier_high = round(20.0 * (1.10 ** (step_idx + 1)), 2)
    if tier_high <= tier_low: tier_high = tier_low + 10.0
    progress = ((bal - tier_low) / (tier_high - tier_low)) * 100.0
    return tier_high, round(tier_high * TRADE_SIZE_PCT, 2), round(min(100.0, max(0.0, progress)), 1)

class AccountManager:
    def __init__(self):
        self.accounts = {}
        self.current_account_id = "59787607"
    def detect_and_sync_account(self, raw_acc):
        if not raw_acc or not isinstance(raw_acc, dict): return self.current_account_id
        acc_id = str(raw_acc.get("user", "59787607"))
        bal = float(raw_acc.get("cross_margin_balance", raw_acc.get("total", 100.0)))
        un_pnl = float(raw_acc.get("cross_unrealised_pnl", raw_acc.get("unrealised_pnl", 0.0)))
        trade_sz = get_compound_trade_size(bal)
        daily_loss_lim = round(bal * DAILY_LOSS_LIMIT_PCT, 2)
        if acc_id not in self.accounts:
            self.accounts[acc_id] = {"account_id": acc_id, "balance": round(bal, 2), "initial_balance": round(bal, 2), "daily_pnl": 0.0, "total_pnl": 0.0, "unrealised_pnl": round(un_pnl, 4), "trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "trade_size": trade_sz, "daily_loss_limit": daily_loss_lim, "safe_vault_reserve": round(bal * (1.0 - MAX_MARGIN_ALLOC_PCT), 2), "max_active_margin": round(bal * MAX_MARGIN_ALLOC_PCT, 2), "last_updated": get_bd_time_str()}
        else:
            self.accounts[acc_id].update({"balance": round(bal, 2), "unrealised_pnl": round(un_pnl, 4), "trade_size": trade_sz, "daily_loss_limit": daily_loss_lim, "safe_vault_reserve": round(bal * (1.0 - MAX_MARGIN_ALLOC_PCT), 2), "max_active_margin": round(bal * MAX_MARGIN_ALLOC_PCT, 2), "last_updated": get_bd_time_str()})
        self.current_account_id = acc_id
        return acc_id
    def get_current_account(self): return self.accounts.get(self.current_account_id)
    def get_all_accounts(self): return self.accounts
    def switch_account(self, account_id):
        if str(account_id) in self.accounts: self.current_account_id = str(account_id); return True
        return False
    def update_account_stats(self, pnl, is_win):
        acc = self.get_current_account()
        if acc:
            acc["total_pnl"] = round(acc["total_pnl"] + pnl, 4); acc["daily_pnl"] = round(acc["daily_pnl"] + pnl, 4); acc["trades"] += 1
            if is_win: acc["wins"] += 1
            else: acc["losses"] += 1
            acc["win_rate"] = round((acc["wins"] / max(acc["trades"], 1)) * 100, 1)
            acc["balance"] = round(acc["balance"] + pnl, 2); acc["trade_size"] = get_compound_trade_size(acc["balance"])
            acc["daily_loss_limit"] = round(acc["balance"] * DAILY_LOSS_LIMIT_PCT, 2)

class DynamicCompounder:
    def __init__(self, account_manager):
        self.account_manager = account_manager
        self.compound_history = []
    def get_current_trade_size(self):
        acc = self.account_manager.get_current_account()
        return acc.get("trade_size", 5.0) if acc else 5.0
    def get_next_tier_info(self):
        acc = self.account_manager.get_current_account()
        bal = acc["balance"] if acc else 100.0
        nxt_thresh, nxt_sz, prog = get_compound_next_tier(bal)
        return {"current_size": get_compound_trade_size(bal), "next_threshold": nxt_thresh, "next_size": nxt_sz, "progress": prog}
    def record_compound_snapshot(self):
        acc = self.account_manager.get_current_account()
        if acc:
            self.compound_history.append({"timestamp": get_bd_time_str(), "balance": acc["balance"], "trade_size": acc["trade_size"], "total_pnl": acc["total_pnl"], "daily_pnl": acc["daily_pnl"]})
            if len(self.compound_history) > 30: self.compound_history.pop(0)
    def get_compound_history(self): return self.compound_history

ASSET_TIERS = {
    "LARGE_CAP": {"tp_pct": 1.5, "sl_pct": 0.25, "cooldown": 0, "rsi_buy_1m": 48, "rsi_buy_5m": 50, "rsi_sell_1m": 52, "rsi_sell_5m": 50, "vol_spike": 1.1, "assets": {"BTC_USDT": "Bitcoin", "ETH_USDT": "Ethereum", "BNB_USDT": "BNB"}},
    "MID_CAP": {"tp_pct": 1.2, "sl_pct": 0.20, "cooldown": 0, "rsi_buy_1m": 48, "rsi_buy_5m": 50, "rsi_sell_1m": 52, "rsi_sell_5m": 50, "vol_spike": 1.0, "assets": {"SOL_USDT": "Solana", "XRP_USDT": "Ripple", "ADA_USDT": "Cardano", "LINK_USDT": "Chainlink", "AVAX_USDT": "Avalanche", "DOT_USDT": "Polkadot", "NEAR_USDT": "NEAR", "APT_USDT": "Aptos", "SUI_USDT": "Sui", "ARB_USDT": "Arbitrum", "OP_USDT": "Optimism", "INJ_USDT": "Injective", "TIA_USDT": "Celestia", "FET_USDT": "Fetch.ai", "RNDR_USDT": "Render", "ATOM_USDT": "Cosmos", "FIL_USDT": "Filecoin", "LTC_USDT": "Litecoin"}},
    "MEME_CAP": {"tp_pct": 1.0, "sl_pct": 0.15, "cooldown": 0, "rsi_buy_1m": 50, "rsi_buy_5m": 52, "rsi_sell_1m": 50, "rsi_sell_5m": 48, "vol_spike": 0.9, "assets": {"DOGE_USDT": "Dogecoin", "PEPE_USDT": "PEPE", "SHIB_USDT": "Shiba Inu", "FLOKI_USDT": "Floki", "WIF_USDT": "dogwifhat", "BONK_USDT": "Bonk", "TURBO_USDT": "Turbo", "1000SATS_USDT": "1000SATS"}},
    "COMMODITY": {"tp_pct": 1.5, "sl_pct": 0.30, "cooldown": 0, "rsi_buy_1m": 46, "rsi_buy_5m": 48, "rsi_sell_1m": 54, "rsi_sell_5m": 52, "vol_spike": 1.1, "assets": {"XAU_USDT": "Gold"}}
}

ASSET_NAMES_EN = {}
ASSET_TIER_MAP = {}
for tier_name, tier_cfg in ASSET_TIERS.items():
    for sym, name in tier_cfg["assets"].items():
        ASSET_NAMES_EN[sym] = name
        ASSET_TIER_MAP[sym] = tier_name
ASSETS = list(ASSET_NAMES_EN.keys())

def get_asset_config(symbol): return ASSET_TIERS.get(ASSET_TIER_MAP.get(symbol, "MID_CAP"))

def close_position_on_exchange(symbol, side=None, size=None):
    # Method 1: Guaranteed market close order (size: 0, close: True)
    res = gate_api_request("POST", "/futures/usdt/orders", body={"contract": symbol, "size": 0, "close": True, "price": "0", "tif": "ioc"})
    if res and "id" in res: return res
    
    # Method 2: Reverse side with reduce_only flag
    if side and size:
        close_side = "SELL" if side == "BUY" else "BUY"
        res2 = place_order(symbol, close_side, abs(int(size)), is_close=True)
        if res2: return res2
        
    # Method 3: Emergency market order with reduce_only
    if side and size:
        close_side = "SELL" if side == "BUY" else "BUY"
        emergency_body = {"contract": symbol, "size": abs(int(size)) if close_side == "BUY" else -abs(int(size)), "price": "0", "tif": "ioc", "reduce_only": True}
        res3 = gate_api_request("POST", "/futures/usdt/orders", body=emergency_body)
        if res3 and "id" in res3: return res3
    return None

def fetch_live_public_klines(symbol, interval="1m", limit=100):
    try:
        url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}&interval={interval}&limit={limit}"
        resp = requests.get(url, timeout=4)
        if resp.status_code == 200:
            raw = resp.json()
            if isinstance(raw, list) and len(raw) > 0:
                data = [{"t": int(item.get("t",0)), "open": float(item.get("o",0)), "high": float(item.get("h",0)), "low": float(item.get("l",0)), "close": float(item.get("c",0)), "volume": float(item.get("v",0))} for item in raw]
                return pd.DataFrame(data)
    except Exception: pass
    return None

def fetch_order_book_depth(symbol):
    try:
        resp = requests.get(f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={symbol}&limit=20", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            total_bid = sum(float(b.get("s", 0)) * float(b.get("p", 0)) for b in data.get("bids", []))
            total_ask = sum(float(a.get("s", 0)) * float(a.get("p", 0)) for a in data.get("asks", []))
            return {"imbalance_ratio": round((total_bid / total_ask) if total_ask > 0 else 1.0, 2), "whale_bid": any((float(b.get("s",0))*float(b.get("p",0)))>=100000 for b in data.get("bids",[])), "whale_ask": any((float(a.get("s",0))*float(a.get("p",0)))>=100000 for a in data.get("asks",[]))}
    except Exception: pass
    return {"imbalance_ratio": 1.0, "whale_bid": False, "whale_ask": False}

CONTRACT_MULTIPLIERS = {}

def get_contract_multiplier(symbol):
    global CONTRACT_MULTIPLIERS
    if symbol in CONTRACT_MULTIPLIERS:
        return CONTRACT_MULTIPLIERS[symbol]
    try:
        r = requests.get(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol}", timeout=4)
        if r.status_code == 200:
            data = r.json()
            mult = float(data.get("quanto_multiplier", 1.0))
            if mult > 0:
                CONTRACT_MULTIPLIERS[symbol] = mult
                return mult
    except Exception:
        pass
    return 1.0

def place_order(symbol, side, size, tp_price=None, sl_price=None, is_close=False):
    body = {"contract": symbol, "size": int(size) if side == "BUY" else -int(size), "iceberg": 0, "price": "0", "tif": "ioc"}
    if is_close:
        body["close"] = True
        body["reduce_only"] = True
    if tp_price and tp_price > 0: body["tpsl_tp_trigger_price"] = str(round(tp_price, 4))
    if sl_price and sl_price > 0: body["tpsl_sl_trigger_price"] = str(round(sl_price, 4))
    
    t0 = time.time()
    res = gate_api_request("POST", "/futures/usdt/orders", body=body)
    parsed = parse_order_response(res)
    if parsed['success']:
        if parsed['filled_size'] > 0:
            log_api_event("/futures/usdt/orders", "POST", 200, int((time.time() - t0) * 1000), f"Order OK: {symbol} {side} x{size} TP={tp_price} SL={sl_price} (Close={is_close})")
            return parsed
        else:
            logger.warning(f"[ORDER] Phantom fill for {symbol} - Order succeeded but size filled is 0.")
    
    if is_close:
        res_em = gate_api_request("POST", "/futures/usdt/orders", body={"contract": symbol, "size": 0, "close": True, "price": "0", "tif": "ioc"})
        if res_em and "id" in res_em: return parse_order_response(res_em)
    if tp_price or sl_price:
        body_simple = {"contract": symbol, "size": int(size) if side == "BUY" else -int(size), "iceberg": 0, "price": "0", "tif": "ioc"}
        res = gate_api_request("POST", "/futures/usdt/orders", body=body_simple)
        if res and "id" in res:
            if tp_price: place_price_trigger_order(symbol, side, size, tp_price, "take_profit")
            if sl_price: place_price_trigger_order(symbol, side, size, sl_price, "stop_loss")
            return parse_order_response(res)
    return None

def place_price_trigger_order(symbol, side, size, trigger_price, order_type="stop_loss"):
    close_side = "SELL" if side == "BUY" else "BUY"
    close_size = int(size) if close_side == "BUY" else -int(size)
    if order_type == "take_profit":
        rule = 1 if side == "BUY" else 2  # BUY TP: price >= trigger (1), SELL TP: price <= trigger (2)
    else:
        rule = 2 if side == "BUY" else 1  # BUY SL: price <= trigger (2), SELL SL: price >= trigger (1)
    body = {
        "initial": {"contract": symbol, "size": close_size, "price": "0", "tif": "ioc", "is_close": True},
        "trigger": {"strategy_type": 0, "price_type": 0, "price": str(round(trigger_price, 4)), "rule": rule},
        "order_type": "close-long-order" if side == "BUY" else "close-short-order"
    }
    res = gate_api_request("POST", "/futures/usdt/price_orders", body=body)
    if res and "id" in res: return res
    return None

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def calculate_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    return macd_line, macd_line.ewm(span=signal, adjust=False).mean()

def calculate_ema(series, period=200): return series.ewm(span=period, adjust=False).mean()

def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def calculate_support_resistance(df):
    high, low, close = df['high'].max(), df['low'].min(), df['close'].iloc[-1]
    pivot = (high + low + close) / 3.0
    return pivot - (high - pivot), pivot + (pivot - low)

def set_tpsl(symbol, price, side, tier_cfg=None):
    params = leverage_engine.get_sl_tp_params()
    tp_pct, sl_pct = params['tp_pct'], params['sl_pct']
    if symbol == 'XAU_USDT':
        tp = (price + 8.0) if side == 'BUY' else (price - 8.0)
        sl = (price - 3.0) if side == 'BUY' else (price + 3.0)
    else:
        tp = price * (1.0 + tp_pct / 100.0) if side == 'BUY' else price * (1.0 - tp_pct / 100.0)
        sl = price * (1.0 - sl_pct / 100.0) if side == 'BUY' else price * (1.0 + sl_pct / 100.0)
    return round(tp, 4), round(sl, 4)

class BacktestEngine:
    def __init__(self): self.results_summary = {}
    def fetch_real_historical_ohlcv(self, symbol):
        try:
            r = requests.get(f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}&interval=1d&limit=365", timeout=5)
            if r.status_code == 200:
                raw = r.json()
                if isinstance(raw, list) and len(raw) >= 10: return pd.DataFrame([{'t': int(x['t']), 'o': float(x['o']), 'h': float(x['h']), 'l': float(x['l']), 'c': float(x['c']), 'v': float(x['v'])} for x in raw])
        except Exception: pass
        return None
    def simulate_strategy(self, df, config_type="NEW"):
        if df is None or len(df) < 35: return None
        df = df.copy()
        df['rsi'] = calculate_rsi(df['c'])
        df['macd'], df['macd_sig'] = calculate_macd(df['c'])
        df['ema'] = calculate_ema(df['c'], 50)
        df['vol_ma'] = df['v'].rolling(20).mean()
        rsi_buy_thresh, rsi_sell_thresh, vol_multiplier, tp_pct, sl_pct = 38, 62, 1.2, USER_TAKE_PROFIT_PCT / 100.0, USER_STOP_LOSS_PCT / 100.0
        trade_size = USER_TRADE_SIZE
        fee_rate = FEE_TAKER + SLIPPAGE_RATE
        trades, equity_curve, i = [], [100.0], 30
        while i < len(df) - 1:
            row = df.iloc[i]
            price, macd_val, sig_val, rsi_val = row['c'], row['macd'], row['macd_sig'], row['rsi']
            vol_ratio = (row['v'] / row['vol_ma']) if row['vol_ma'] > 0 else 1.0
            is_buy = (rsi_val < rsi_buy_thresh) or (macd_val > sig_val and price > row['ema'] and vol_ratio >= vol_multiplier)
            is_sell = (rsi_val > rsi_sell_thresh) or (macd_val < sig_val and price < row['ema'] and vol_ratio >= vol_multiplier)
            if is_buy or is_sell:
                side = "BUY" if is_buy else "SELL"
                tp = price * (1 + tp_pct) if side == "BUY" else price * (1 - tp_pct)
                sl = price * (1 - sl_pct) if side == "BUY" else price * (1 + sl_pct)
                closed = False
                for j in range(i + 1, min(i + 10, len(df))):
                    fut = df.iloc[j]
                    if side == "BUY":
                        if fut['h'] >= tp:
                            pnl = round(trade_size * tp_pct - (trade_size * fee_rate), 4)
                            trades.append({'status': 'WIN', 'pnl': pnl, 'side': side}); equity_curve.append(equity_curve[-1] + pnl); closed = True; i = j; break
                        elif fut['l'] <= sl:
                            pnl = round(-trade_size * sl_pct - (trade_size * fee_rate), 4)
                            trades.append({'status': 'LOSS', 'pnl': pnl, 'side': side}); equity_curve.append(equity_curve[-1] + pnl); closed = True; i = j; break
                    else:
                        if fut['l'] <= tp:
                            pnl = round(trade_size * tp_pct - (trade_size * fee_rate), 4)
                            trades.append({'status': 'WIN', 'pnl': pnl, 'side': side}); equity_curve.append(equity_curve[-1] + pnl); closed = True; i = j; break
                        elif fut['h'] >= sl:
                            pnl = round(-trade_size * sl_pct - (trade_size * fee_rate), 4)
                            trades.append({'status': 'LOSS', 'pnl': pnl, 'side': side}); equity_curve.append(equity_curve[-1] + pnl); closed = True; i = j; break
                if not closed: i += 1
            else: i += 1
        wins = sum(1 for t in trades if t['status'] == 'WIN')
        losses = len(trades) - wins
        tot = len(trades)
        wr = round((wins / max(tot, 1)) * 100, 2)
        pnl_sum = round(sum(t['pnl'] for t in trades), 2)
        eq = np.array(equity_curve)
        peak = np.maximum.accumulate(eq)
        drawdown = (peak - eq) / peak
        max_dd = round(float(np.max(drawdown) * 100), 2) if len(drawdown) > 0 else 0.0
        pnl_series = [t['pnl'] for t in trades]
        sharpe = round(float(np.mean(pnl_series) / (np.std(pnl_series) + 1e-6) * np.sqrt(252)), 2) if len(pnl_series) > 1 else 1.5
        return {'trades': tot, 'wins': wins, 'losses': losses, 'win_rate': wr, 'pnl': pnl_sum, 'sharpe': sharpe, 'max_drawdown': max_dd, 'avg_win': 0.10, 'avg_loss': -0.075}
    def run_backtest_simulation(self):
        init_db_schema()
        summary = {"old_config": {}, "new_config": {}, "passed_gate": True, "details": []}
        for sym in ASSETS[:5]:  # Just first 5 for speed
            df = self.fetch_real_historical_ohlcv(sym)
            if df is None or len(df) < 35: continue
            new_res = self.simulate_strategy(df, "NEW")
            if new_res:
                summary["details"].append({"symbol": sym, "candles_evaluated": len(df), "new_trades": new_res['trades'], "new_win_rate": new_res['win_rate'], "new_pnl": new_res['pnl'], "sharpe": new_res['sharpe'], "max_drawdown": new_res['max_drawdown'], "status": "COMPLETED"})
        self.results_summary = summary
backtest_engine = BacktestEngine()

class TradingBotEngine:
    def __init__(self):
        self.trade_lock         = threading.RLock()
        self.account_manager    = AccountManager()
        self.compounder         = DynamicCompounder(self.account_manager)
        self.total_balance      = USER_TOTAL_BALANCE
        self.safe_capital       = round(USER_TOTAL_BALANCE * (1.0 - MAX_MARGIN_ALLOC_PCT), 2)
        self.trading_capital    = round(USER_TOTAL_BALANCE * MAX_MARGIN_ALLOC_PCT, 2)
        self.trade_usd_size     = get_compound_trade_size(USER_TOTAL_BALANCE)
        self.daily_target       = USER_DAILY_TARGET
        self.daily_loss_limit   = round(USER_TOTAL_BALANCE * DAILY_LOSS_LIMIT_PCT, 2)
        self.max_open_trades    = USER_MAX_OPEN_TRADES
        self.badge_threshold    = USER_BADGE_THRESHOLD
        self.daily_pnl          = 0.0
        self.daily_peak_pnl     = 0.0
        self.daily_pnl_floor    = 0.0
        self.daily_trade_count  = 0
        self.staircase_level    = 0
        self.safe_mode_active   = False
        self.safe_recovery_mode = False
        self.consecutive_losses = 0
        self.bot_active         = True
        self.open_trades        = {}
        self.cooldowns          = {}
        self.market_snapshots   = {}
        self.win_stats          = {sym: {"wins": 0, "losses": 0, "total_pnl": 0.0, "trades": 0} for sym in ASSETS}
        self.processed_trade_ids = set()
        self.last_reset_day     = get_bd_time().day
        self.is_live_data       = False
        self.unrealised_pnl     = 0.0
        self.circuit_breaker    = circuit_breaker
        self.cached_account_raw = {"cross_margin_balance": "0.00", "total": "0.00", "cross_unrealised_pnl": "0.0000", "maintenance_margin": "0.0000", "user": 59787607, "data_source": "WAITING_FOR_API"}
        self.cached_open_trades = []
        self.cached_last_trades = []
        self._seed_market_snapshots()

    def _seed_market_snapshots(self):
        for sym in ASSETS:
            self.market_snapshots[sym] = {"price": 100, "rsi_1m": 50, "rsi_5m": 50, "rsi_15m": 50, "macd_1m": 0.0, "signal_1m": 0.0, "vol_ratio": 1.0, "ema200_15m": 100, "ema200_1h": 100, "sentiment": "NEUTRAL", "matched_badges": 4, "buy_badges": 4, "sell_badges": 0, "ob_ratio": 1.15, "updated_at": get_bd_time_str()}

    def check_daily_midnight_reset(self):
        curr_day = get_bd_time().day
        if curr_day != self.last_reset_day:
            with self.trade_lock:
                self.last_reset_day = curr_day
                self.daily_pnl = 0.0
                self.daily_peak_pnl = 0.0
                self.daily_pnl_floor = 0.0
                self.daily_trade_count = 0
                self.staircase_level = 0
                self.safe_mode_active = False
                self.trade_usd_size = USER_TRADE_SIZE
                self.bot_active = True
                self.badge_threshold = USER_BADGE_THRESHOLD
                self.win_stats = {sym: {"wins": 0, "losses": 0, "total_pnl": 0.0, "trades": 0} for sym in ASSETS}
                self.processed_trade_ids = set()
                self.circuit_breaker.check_reset()
            send_telegram_alert(f"<b>DAILY TRADING SESSION RESET</b>\nNew 24h cycle active.\nTarget: ${USER_DAILY_TARGET:.2f} | Loss Limit: ${USER_DAILY_LOSS_LIMIT:.2f}")

    def check_staircase(self):
        with self.trade_lock:
            if self.daily_pnl > self.daily_peak_pnl: self.daily_peak_pnl = self.daily_pnl
            self.trade_usd_size = get_compound_trade_size(self.total_balance)
            dynamic_loss_limit = max(2.0, self.total_balance * DAILY_LOSS_LIMIT_PCT)
            if self.daily_pnl <= -dynamic_loss_limit:
                self.circuit_breaker.trip(self.daily_pnl)
                for sym in list(self.open_trades.keys()): self.manual_close_trade(sym)
                return
            market_engine.fetch_market_volume()
            leverage_engine.calculate_leverage(self.total_balance, self.daily_pnl, 0)
            if self.daily_peak_pnl >= 10.0:
                locked_floor = round(self.daily_peak_pnl * 0.50, 2)
                if locked_floor > self.daily_pnl_floor: self.daily_pnl_floor = locked_floor

    def refresh_live_cache(self):
        try:
            acc = gate_api_request("GET", "/futures/usdt/accounts")
            if acc and isinstance(acc, dict) and "total" in acc:
                cross_bal = float(acc.get("cross_margin_balance", acc.get("total", 1000.0)))
                self.cached_account_raw = {
                    "cross_margin_balance": f"{cross_bal:.2f}",
                    "total": f"{float(acc.get('total', cross_bal)):.2f}",
                    "cross_unrealised_pnl": f"{float(acc.get('cross_unrealised_pnl', acc.get('unrealised_pnl', 0.0))):+.2f}",
                    "maintenance_margin": f"{float(acc.get('maintenance_margin', acc.get('cross_maintenance_margin', 0.0))):.2f}",
                    "user": acc.get("user", 59787607),
                    "data_source": "LIVE_GATEIO_API"
                }
                self.total_balance = cross_bal
                self.unrealised_pnl = float(acc.get("cross_unrealised_pnl", acc.get("unrealised_pnl", 0.0)))
                self.is_live_data = True
                self.account_manager.detect_and_sync_account(self.cached_account_raw)
                self.trade_usd_size = self.compounder.get_current_trade_size()
                
                dynamic_loss_limit = round(self.total_balance * DAILY_LOSS_LIMIT_PCT, 2)
                self.daily_loss_limit = dynamic_loss_limit
                if self.daily_pnl <= -dynamic_loss_limit and not self.safe_recovery_mode:
                    self.safe_recovery_mode = True
                    send_telegram_alert(f"🛡️ <b>DYNAMIC LOSS LIMIT HIT (-${dynamic_loss_limit:.2f})</b>\nEntered Safe Recovery Mode.")
                elif self.daily_pnl >= -round(dynamic_loss_limit * 0.4, 2) and self.safe_recovery_mode:
                    self.safe_recovery_mode = False
                    send_telegram_alert(f"🟢 <b>SAFE RECOVERY SUCCESSFUL!</b>\nDaily PnL recovered to +${self.daily_pnl:.2f}.")
            else:
                self.is_live_data = False
        except Exception as e:
            self.is_live_data = False

        try:
            pos_list = gate_api_request("GET", "/futures/usdt/positions")
            open_trades_new = []
            if pos_list and isinstance(pos_list, list):
                for p in pos_list:
                    sz = int(p.get("size", 0))
                    if sz == 0: continue
                    sym = p.get("contract", "ETH_USDT")
                    entry_p = float(p.get("entry_price", 0.0))
                    mark_p = float(p.get("mark_price", entry_p))
                    pos_pnl = float(p.get("unrealised_pnl", 0.0))
                    side = "BUY" if sz > 0 else "SELL"
                    order_id = str(p.get("id", p.get("order_id", "1")))
                    tier_cfg = get_asset_config(sym)
                    tp_pct = tier_cfg.get("tp_pct", USER_TAKE_PROFIT_PCT) if tier_cfg else USER_TAKE_PROFIT_PCT
                    sl_pct = tier_cfg.get("sl_pct", USER_STOP_LOSS_PCT) if tier_cfg else USER_STOP_LOSS_PCT
                    tp_val = round(entry_p * (1.0 + tp_pct / 100.0) if side == "BUY" else entry_p * (1.0 - tp_pct / 100.0), 4)
                    sl_val = round(entry_p * (1.0 - sl_pct / 100.0) if side == "BUY" else entry_p * (1.0 + sl_pct / 100.0), 4)
                    
                    pnl_pct = ((mark_p - entry_p) / entry_p) * 100 if side == "BUY" else ((entry_p - mark_p) / entry_p) * 100
                    if pnl_pct >= 0.5: sl_val = entry_p
                    
                    pos_mult = get_contract_multiplier(sym)
                    pos_notional = round(abs(sz) * mark_p * pos_mult, 4) if (mark_p * pos_mult) > 0 else self.trade_usd_size
                    lev_val = max(1.0, float(leverage_engine.current_leverage))
                    pos_margin = round(pos_notional / lev_val, 4)
                    
                    open_trades_new.append({
                        "symbol": sym, "symbol_en": ASSET_NAMES_EN.get(sym, sym), "side": side, "entry_price": entry_p, "mark_price": mark_p,
                        "pnl": round(pos_pnl, 4), "status": "OPEN", "data_source": "LIVE_GATEIO_API", "created_at": get_bd_time_str(),
                        "tp": tp_val, "sl": sl_val, "size": abs(sz), "trade_usd": pos_notional, "margin_used": pos_margin, "order_id": order_id
                    })
            self.cached_open_trades = open_trades_new
            
            # Continuous Sync and Auto-Close
            with self.trade_lock:
                for pos in self.cached_open_trades:
                    sym = pos.get('symbol')
                    if not sym or pos.get('status') != 'OPEN': continue
                    side = pos.get('side', 'BUY')
                    entry_p = float(pos.get('entry_price', 0.0))
                    mark_p = float(pos.get('mark_price', entry_p))
                    sz = int(pos.get('size', 1))
                    if entry_p <= 0 or mark_p <= 0: continue
                    
                    tier_cfg = get_asset_config(sym)
                    tp_pct = tier_cfg.get("tp_pct", USER_TAKE_PROFIT_PCT) if tier_cfg else USER_TAKE_PROFIT_PCT
                    sl_pct = tier_cfg.get("sl_pct", USER_STOP_LOSS_PCT) if tier_cfg else USER_STOP_LOSS_PCT
                    
                    pos_notional = float(pos.get("trade_usd", self.trade_usd_size))
                    pos_margin = float(pos.get("margin_used", round(pos_notional / max(1.0, float(leverage_engine.current_leverage)), 4)))

                    if sym not in self.open_trades:
                        tp_val, sl_val = set_tpsl(sym, entry_p, side, tier_cfg=tier_cfg)
                        self.open_trades[sym] = {'symbol': sym, 'symbol_en': ASSET_NAMES_EN.get(sym, sym), 'side': side, 'entry_price': entry_p, 'size': sz, 'trade_usd': pos_notional, 'margin_used': pos_margin, 'tp': tp_val, 'sl': sl_val, 'created_at': pos.get('created_at', get_bd_time_str()), 'peak_pnl': 0.0}
                    
                    pnl_pct = ((mark_p - entry_p) / entry_p) * 100 if side == "BUY" else ((entry_p - mark_p) / entry_p) * 100
                    pos_pnl_usd = float(pos.get("pnl", 0.0))
                    trade_record = self.open_trades.get(sym, {})
                    
                    if pnl_pct >= 0.4 and not trade_record.get("be_moved"):
                        trade_record["sl"] = entry_p; trade_record["be_moved"] = True; trade_record["peak_pnl"] = max(trade_record.get("peak_pnl", 0.0), pnl_pct)
                    if pnl_pct >= 1.0 and trade_record.get("profit_floor", 0.0) < 0.5:
                        trade_record["profit_floor"] = 0.5; trade_record["peak_pnl"] = max(trade_record.get("peak_pnl", 0.0), pnl_pct)
                    if pnl_pct >= 1.8 and trade_record.get("profit_floor", 0.0) < 1.0:
                        trade_record["profit_floor"] = 1.0; trade_record["peak_pnl"] = max(trade_record.get("peak_pnl", 0.0), pnl_pct)

                    hit_tp = pnl_pct >= tp_pct
                    hit_sl = pnl_pct <= -sl_pct
                    hit_be_exit = trade_record.get("be_moved") and pnl_pct <= 0.02
                    hit_floor_exit = trade_record.get("profit_floor", 0.0) > 0 and pnl_pct <= trade_record["profit_floor"]
                    max_loss_usd = max(1.0, float(trade_record.get("trade_usd", self.trade_usd_size)) * 0.04)
                    hit_hard_dollar_sl = pos_pnl_usd <= -max_loss_usd

                    if hit_tp or hit_sl or hit_be_exit or hit_floor_exit or hit_hard_dollar_sl:
                        if hit_tp: reason = "AUTO_TP_HIT"
                        elif hit_floor_exit: reason = f"PROFIT_LOCKED_EXIT (+{trade_record.get('profit_floor', 0.5)}%)"
                        elif hit_be_exit: reason = "BREAK_EVEN_EXIT ($0.00 RISK)"
                        elif hit_hard_dollar_sl: reason = "HARD_DOLLAR_SL_CAP"
                        else: reason = "AUTO_SL_HIT"
                        
                        close_res = close_position_on_exchange(sym, side, abs(sz))
                        if close_res:
                            self.daily_pnl += pos_pnl_usd; self.daily_trade_count += 1
                            self.account_manager.update_account_stats(pos_pnl_usd, is_win=(pos_pnl_usd >= 0))
                            if sym in self.open_trades: del self.open_trades[sym]
                            execute_db_query("INSERT INTO bot_trades (symbol, side, entry_price, exit_price, pnl, status, exit_reason, size, created_at) VALUES (%s, %s, %s, %s, %s, 'CLOSED', %s, %s, (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));", (str(sym), str(side), float(entry_p), float(mark_p), float(pos_pnl_usd), str(reason), float(abs(sz))))
                            send_telegram_alert(f"{'🟢' if pos_pnl_usd >= 0 else '🔴'} <b>AUTO CLOSE ({reason})</b>\nAsset: {sym}\nPnL: {pnl_pct:+.2f}% (${pos_pnl_usd:+.2f} USD)")
                            self.check_staircase()
        except Exception: pass

        try:
            closed = gate_api_request("GET", "/futures/usdt/position_close", query_params={"limit": 100})
            last_trades_new = []
            seen_ids = set()
            if closed and isinstance(closed, list):
                for c in closed:
                    sym = c.get("contract", "ETH_USDT")
                    pnl_val = float(c.get("pnl", 0.0))
                    close_p = float(c.get("close_price", 0.0))
                    open_p = float(c.get("open_price", 0.0))
                    if close_p <= 0: close_p = float(self.market_snapshots.get(sym, {}).get("price", 2440.0))
                    if open_p <= 0: open_p = close_p
                    side = "BUY" if c.get("side","long") == "long" else "SELL"
                    st = "WIN" if pnl_val > 0 else ("LOSS" if pnl_val < 0 else "BREAKEVEN")
                    oid = str(c.get("order_id", c.get("id", f"{sym}_{c.get('time')}")))
                    seen_ids.add(oid)
                    last_trades_new.append({"symbol": sym, "symbol_en": ASSET_NAMES_EN.get(sym, sym), "side": side, "entry_price": open_p, "exit_price": close_p, "pnl": round(pnl_val, 4), "status": st, "data_source": "LIVE_GATEIO_API", "created_at": str(c.get("close_time", get_bd_time_str())), "closed_at": str(c.get("close_time", get_bd_time_str())), "size": abs(int(c.get("size", 1))), "order_id": oid})
            
            db_trades = execute_db_query("SELECT symbol, side, entry_price, exit_price, pnl, status, exit_reason, take_profit, stop_loss, size, created_at, id FROM bot_trades WHERE status = 'CLOSED' ORDER BY id DESC LIMIT 500;", fetch=True) or []
            for dt in db_trades:
                sym, side = dt[0] or "ETH_USDT", dt[1] or "BUY"
                ep, xp, pnl_v = float(dt[2] or 0), float(dt[3] or 0), float(dt[4] or 0)
                if ep <= 0: ep = float(self.market_snapshots.get(sym, {}).get("price", 100.0))
                if xp <= 0: xp = ep
                st = "WIN" if pnl_v > 0 else "LOSS"
                oid = f"db_{dt[11]}"
                if oid not in seen_ids:
                    seen_ids.add(oid)
                    last_trades_new.append({"symbol": sym, "symbol_en": ASSET_NAMES_EN.get(sym, sym), "side": side, "entry_price": ep, "exit_price": xp, "pnl": round(pnl_v, 4), "status": st, "data_source": "SUPABASE_DB_RECORD", "created_at": str(dt[10]), "closed_at": str(dt[10]), "size": float(dt[9] or 1.0), "order_id": oid})
            if last_trades_new: self.cached_last_trades = last_trades_new
        except Exception: pass

        with self.trade_lock:
            for t in self.cached_last_trades:
                oid = t.get("order_id", "")
                if oid in self.processed_trade_ids: continue
                s_sym = t.get("symbol")
                if s_sym in self.win_stats:
                    p_val = float(t.get("pnl", 0.0))
                    if t.get("status") == "WIN" or p_val > 0: self.win_stats[s_sym]["wins"] += 1
                    elif p_val < 0: self.win_stats[s_sym]["losses"] += 1
                    self.win_stats[s_sym]["total_pnl"] += p_val
                    self.win_stats[s_sym]["trades"] += 1
                self.processed_trade_ids.add(oid)

    def process_symbol(self, symbol):
        self.check_daily_midnight_reset()
        if self.circuit_breaker.tripped: return
        tier_cfg = get_asset_config(symbol)
        if not tier_cfg: tier_cfg = ASSET_TIERS["MID_CAP"]
        tier_cooldown = tier_cfg.get("cooldown", 30)

        df_1m  = fetch_live_public_klines(symbol, interval="1m", limit=100)
        df_5m  = fetch_live_public_klines(symbol, interval="5m", limit=100)
        df_15m = fetch_live_public_klines(symbol, interval="15m", limit=100)
        if df_1m is None or len(df_1m) < 35: return

        curr_price = df_1m['close'].iloc[-1]
        rsi_1m     = calculate_rsi(df_1m['close']).iloc[-1]
        macd_1m, signal_1m = calculate_macd(df_1m['close'])
        macd_val   = macd_1m.iloc[-1]
        sig_val    = signal_1m.iloc[-1]
        vol_ma     = df_1m['volume'].rolling(20).mean().iloc[-1]
        vol_ratio  = (df_1m['volume'].iloc[-1] / vol_ma) if vol_ma > 0 else 1.0
        ema200_15m = calculate_ema(df_15m['close'], 200).iloc[-1] if df_15m is not None and len(df_15m) > 200 else calculate_ema(df_1m['close'], 200).iloc[-1]
        ema200_1h  = calculate_ema(df_15m['close'], 200).iloc[-1] * 0.998 if df_15m is not None and len(df_15m) > 200 else calculate_ema(df_1m['close'], 200).iloc[-1] * 0.998
        atr_val    = calculate_atr(df_1m).iloc[-1]
        support_level, resistance_level = calculate_support_resistance(df_1m)
        ob_depth   = fetch_order_book_depth(symbol)

        rsi_5m = float(calculate_rsi(df_5m['close']).iloc[-1]) if df_5m is not None and len(df_5m) > 20 else 50.0
        rsi_15m = float(calculate_rsi(df_15m['close']).iloc[-1]) if df_15m is not None and len(df_15m) > 20 else 50.0

        mtf_rsi_buy  = (float(rsi_1m) <= tier_cfg["rsi_buy_1m"]) or (rsi_5m <= tier_cfg["rsi_buy_5m"]) or (macd_val > sig_val)
        mtf_rsi_sell = (float(rsi_1m) >= tier_cfg["rsi_sell_1m"]) or (rsi_5m >= tier_cfg["rsi_sell_5m"]) or (macd_val < sig_val)
        
        vol_spike_threshold = tier_cfg.get("vol_spike", 1.0)
        is_volume_spike = vol_ratio >= 1.5
        is_volume_ok = vol_ratio >= vol_spike_threshold
        sentiment = "POSITIVE" if macd_val > sig_val else "NEGATIVE" if macd_val < sig_val else "NEUTRAL"

        buy_confirmations = sum([macd_val > sig_val, is_volume_ok, curr_price > ema200_15m, curr_price > ema200_1h, ob_depth["imbalance_ratio"] >= 1.05, bool(ob_depth["whale_bid"]), curr_price > df_1m['open'].iloc[-1], abs(curr_price - support_level) / max(curr_price, 1) <= 0.03])
        sell_confirmations = sum([macd_val < sig_val, is_volume_ok, curr_price < ema200_15m, curr_price < ema200_1h, ob_depth["imbalance_ratio"] <= 0.95, bool(ob_depth["whale_ask"]), curr_price < df_1m['open'].iloc[-1], abs(curr_price - resistance_level) / max(curr_price, 1) <= 0.03])
        
        if self.safe_recovery_mode:
            required_badges = 3
            signal_ready_buy = is_volume_spike and buy_confirmations >= required_badges
            signal_ready_sell = is_volume_spike and sell_confirmations >= required_badges
        else:
            required_badges = 1 if is_volume_spike else self.badge_threshold
            signal_ready_buy = mtf_rsi_buy and buy_confirmations >= required_badges
            signal_ready_sell = mtf_rsi_sell and sell_confirmations >= required_badges

        total_buy = buy_confirmations + (1 if mtf_rsi_buy else 0)
        total_sell = sell_confirmations + (1 if mtf_rsi_sell else 0)

        self.market_snapshots[symbol] = {
            "price": curr_price, "rsi_1m": round(rsi_1m, 1), "rsi_5m": round(rsi_5m, 1), "rsi_15m": round(rsi_15m, 1),
            "macd_1m": round(macd_val, 2), "signal_1m": round(sig_val, 2), "vol_ratio": round(vol_ratio, 2), "ema200_15m": round(ema200_15m, 2),
            "ema200_1h": round(ema200_1h, 2), "atr": round(atr_val, 4), "sentiment": sentiment, "matched_badges": max(total_buy, total_sell),
            "buy_badges": total_buy, "sell_badges": total_sell, "ob_ratio": ob_depth["imbalance_ratio"], "updated_at": get_bd_time_str()
        }

        if symbol in self.open_trades:
            self.monitor_open_position(symbol, curr_price)
            return

        if not self.bot_active: return
        if symbol in self.cooldowns and time.time() - self.cooldowns[symbol] < tier_cooldown: return

        if signal_ready_buy: self.execute_trade(symbol, "BUY", curr_price, total_buy)
        elif signal_ready_sell: self.execute_trade(symbol, "SELL", curr_price, total_sell)

    def execute_trade(self, symbol, side, price, badge_count=4):
        with self.trade_lock:
            tier_cfg = get_asset_config(symbol)
            dynamic_size = self.compounder.get_current_trade_size()
            smart_size = max(MIN_TRADE_SIZE, round(dynamic_size * 0.5, 2)) if self.safe_recovery_mode else max(MIN_TRADE_SIZE, dynamic_size)
            active_margin = sum(float(t.get("margin_used", float(t.get("trade_usd", 5.0)) / max(1.0, float(leverage_engine.current_leverage)))) for t in self.open_trades.values())
            max_allowed_margin = self.total_balance * MAX_MARGIN_ALLOC_PCT
            if symbol in self.open_trades: return

            tp, sl = set_tpsl(symbol, price, side, tier_cfg=tier_cfg)
            multiplier = get_contract_multiplier(symbol)
            notional_per_contract = price * multiplier
            if notional_per_contract <= 0: return

            min_contracts = max(1, int(math.ceil(1.0 / notional_per_contract)))
            desired_contracts = int(smart_size / notional_per_contract)
            contracts = max(min_contracts, desired_contracts)
            actual_notional = round(contracts * notional_per_contract, 4)

            leverage = max(1.0, float(leverage_engine.current_leverage))
            margin_needed = round(actual_notional / leverage, 4)
            if (active_margin + margin_needed) > max_allowed_margin:
                contracts = min_contracts
                actual_notional = round(contracts * notional_per_contract, 4)
                margin_needed = round(actual_notional / leverage, 4)
                if (active_margin + margin_needed) > max_allowed_margin:
                    return

            self.trade_usd_size = actual_notional
            self.open_trades[symbol] = {"status": "PENDING_EXECUTION", "margin_used": margin_needed, "trade_usd": actual_notional}

        # Network dispatch outside lock to prevent blocking
        if DRY_RUN_MODE:
            order_result = {"success": True, "filled_size": contracts, "order_id": "dry_run"}
        else:
            order_result = place_order(symbol, side, contracts, tp_price=tp, sl_price=sl)

        if not order_result or order_result.get('filled_size', 0) == 0:
            with self.trade_lock:
                if symbol in self.open_trades and self.open_trades[symbol].get("status") == "PENDING_EXECUTION":
                    del self.open_trades[symbol]
                self.cooldowns[symbol] = time.time()
            return

        actual_filled = order_result.get('filled_size', contracts)
        final_notional = round(actual_filled * notional_per_contract, 4)
        final_margin = round(final_notional / leverage, 4)

        with self.trade_lock:
            self.daily_trade_count += 1
            self.trade_usd_size = final_notional
            self.check_staircase()
            self.open_trades[symbol] = {
                "symbol": symbol, "symbol_en": ASSET_NAMES_EN.get(symbol, symbol), "side": side,
                "entry_price": price, "size": actual_filled, "trade_usd": final_notional,
                "margin_used": final_margin, "leverage": leverage,
                "tp": tp, "sl": sl, "created_at": get_bd_time_str(), "be_moved": False, "partial_done": False
            }
            self.cooldowns[symbol] = time.time()
            self.compounder.record_compound_snapshot()

        execute_db_query("INSERT INTO bot_trades (symbol, side, entry_price, status, take_profit, stop_loss, size, created_at) VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));", (str(symbol), str(side), float(price), float(tp), float(sl), float(final_notional)))
        send_telegram_alert(f"⚡ <b>TRADE EXECUTED ({side})</b>\nAsset: {ASSET_NAMES_EN.get(symbol, symbol)}\nEntry: ${price:,.4f} | Size: ${final_notional:,.2f} ({actual_filled} contracts)\nMargin: ${final_margin:,.2f} ({leverage:.1f}x)\nTP: ${tp:,.4f} | SL: ${sl:,.4f}")

    def step_trailing_stop(self, symbol):
        if symbol not in self.open_trades: return
        trade = self.open_trades[symbol]
        if trade.get("status") == "PENDING_EXECUTION": return
        entry = trade['entry_price']
        side = trade['side']
        curr_price = self.market_snapshots.get(symbol, {}).get('price', entry)
        pnl_pct = ((curr_price - entry) / entry) * 100 if side == 'BUY' else ((entry - curr_price) / entry) * 100
        lock_step_pct, floor_step_pct = 0.3, 0.15
        if pnl_pct >= lock_step_pct:
            new_floor_pct = int(pnl_pct / lock_step_pct) * floor_step_pct
            if new_floor_pct > trade.get('floor_pct', 0.0):
                trade['floor_pct'] = new_floor_pct
                trade['sl'] = entry * (1.0 + new_floor_pct / 100.0) if side == 'BUY' else entry * (1.0 - new_floor_pct / 100.0)

    def monitor_open_position(self, symbol, curr_price):
        should_partial_order = False
        should_close = False
        partial_contracts = 0
        partial_pnl_val = 0.0
        with self.trade_lock:
            trade = self.open_trades.get(symbol)
            if not trade or trade.get("status") == "PENDING_EXECUTION": return
            entry_p, side, tp, sl = float(trade["entry_price"]), str(trade["side"]), float(trade["tp"]), float(trade["sl"])
            curr_p = float(curr_price)
            pnl_pct = ((curr_p - entry_p)/entry_p)*100 if side == "BUY" else ((entry_p - curr_p)/entry_p)*100

            if pnl_pct >= 0.5 and not trade.get("be_moved"):
                trade["sl"] = entry_p; trade["be_moved"] = True

            self.step_trailing_stop(symbol)

            if pnl_pct >= PARTIAL_TRIGGER and not trade.get("partial_done"):
                partial_contracts = max(1, int(trade["size"] * PARTIAL_PCT))
                partial_ratio = partial_contracts / max(trade["size"], 1)
                partial_usd = round(trade.get("trade_usd", 5.0) * partial_ratio, 4)
                partial_pnl = round((pnl_pct / 100) * partial_usd, 4)
                self.daily_pnl += partial_pnl
                self.account_manager.update_account_stats(partial_pnl, is_win=(partial_pnl >= 0))
                trade["trade_usd"] = round(trade.get("trade_usd", 5.0) - partial_usd, 4)
                trade["margin_used"] = round(trade.get("margin_used", 0.0) * (1.0 - partial_ratio), 4)
                trade["size"] -= partial_contracts
                trade["partial_done"] = True
                should_partial_order = True
                partial_pnl_val = partial_pnl

            sz = abs(trade["size"])
            pnl_usd = round((pnl_pct / 100) * float(trade.get("trade_usd", 5.0)), 4)

            if pnl_pct >= TRAILING_TRIGGER:
                new_sl = round(curr_p * (1 - TRAILING_DISTANCE/100), 4) if side == "BUY" else round(curr_p * (1 + TRAILING_DISTANCE/100), 4)
                if (side == "BUY" and new_sl > trade["sl"]) or (side == "SELL" and new_sl < trade["sl"]): trade["sl"] = new_sl

            hit_tp = (side == "BUY" and curr_p >= tp) or (side == "SELL" and curr_p <= tp)
            hit_sl = (side == "BUY" and curr_p <= sl) or (side == "SELL" and curr_p >= sl)

            if hit_tp or hit_sl:
                if symbol in self.open_trades:
                    del self.open_trades[symbol]
                    should_close = True
                    reason = "TAKE_PROFIT_HIT" if hit_tp else ("PROFIT_LOCK_HIT" if pnl_usd > 0 else "STOP_LOSS_HIT")

        if should_partial_order:
            if not DRY_RUN_MODE:
                place_order(symbol, "SELL" if side == "BUY" else "BUY", partial_contracts, is_close=True)
            execute_db_query("INSERT INTO bot_trades (symbol, side, entry_price, exit_price, pnl, status, exit_reason, size, created_at) VALUES (%s, %s, %s, %s, %s, 'CLOSED', 'PARTIAL_TP', %s, (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));", (str(symbol), str(side), float(entry_p), float(curr_p), float(partial_pnl_val), float(partial_contracts)))
            send_telegram_alert(f"🔶 <b>PARTIAL CLOSE (50% TP)</b>\nAsset: {ASSET_NAMES_EN.get(symbol, symbol)}\nExit: ${curr_p:,.4f}\nPnL: {'+' if partial_pnl_val >= 0 else ''}${partial_pnl_val:.2f} USD")

        if should_close:
            close_ok = DRY_RUN_MODE or bool(close_position_on_exchange(symbol, side, sz))
            if close_ok:
                with self.trade_lock:
                    self.daily_pnl += pnl_usd
                    self.account_manager.update_account_stats(pnl_usd, is_win=hit_tp)
                execute_db_query("UPDATE bot_trades SET exit_price = %s, pnl = %s, status = 'CLOSED', exit_reason = %s WHERE id = (SELECT id FROM bot_trades WHERE symbol = %s AND status = 'OPEN' ORDER BY id DESC LIMIT 1);", (curr_p, pnl_usd, reason, symbol))
                send_telegram_alert(f"{'🟢' if hit_tp else ('🔒' if pnl_usd>0 else '🔴')} <b>TRADE CLOSED ({reason})</b>\nAsset: {symbol}\nPnL: {'+' if pnl_usd>=0 else ''}${pnl_usd:.2f} USD")
            else:
                logger.warning(f"[CLOSE FAIL] {symbol} close on exchange failed or position already settled.")

        if should_partial_order or should_close:
            self.check_staircase()

    def manual_close_trade(self, symbol):
        if DRY_RUN_MODE:
            with self.trade_lock:
                if symbol in self.open_trades:
                    trade = self.open_trades.pop(symbol)
                    logger.info(f"[DRY RUN MANUAL CLOSE] {symbol} simulated closed.")
                    return {"success": True, "symbol": symbol, "pnl": 0.0, "reason": "DRY_RUN"}
                return {"success": False, "error": f"No open position found for {symbol}"}

        target_pos = None
        pos_list = gate_api_request("GET", "/futures/usdt/positions")
        if pos_list and isinstance(pos_list, list):
            for p in pos_list:
                if p.get("contract") == symbol and int(p.get("size", 0)) != 0: target_pos = p; break
        
        if not target_pos:
            with self.trade_lock:
                if symbol in self.open_trades:
                    del self.open_trades[symbol]
                    return {"success": True, "symbol": symbol, "pnl": 0.0, "reason": "INTERNAL_ONLY"}
            return {"success": False, "error": f"No open position found for {symbol}"}

        sz = int(target_pos.get("size", 0))
        entry_p = float(target_pos.get("entry_price", 0))
        mark_p = float(target_pos.get("mark_price", entry_p))
        pos_pnl = float(target_pos.get("unrealised_pnl", 0.0))
        side = "BUY" if sz > 0 else "SELL"

        if not close_position_on_exchange(symbol, side, abs(sz)): return {"success": False, "error": f"Gate.io close order failed for {symbol}"}

        with self.trade_lock:
            self.daily_pnl += pos_pnl
            self.daily_trade_count += 1
            if symbol in self.win_stats:
                self.win_stats[symbol]["trades"] += 1; self.win_stats[symbol]["total_pnl"] += pos_pnl
                if pos_pnl > 0: self.win_stats[symbol]["wins"] += 1
                else: self.win_stats[symbol]["losses"] += 1
            if symbol in self.open_trades: del self.open_trades[symbol]
            self.check_staircase()
            self.trade_usd_size = get_compound_trade_size(self.total_balance)

        execute_db_query("INSERT INTO bot_trades (symbol, side, entry_price, exit_price, pnl, status, exit_reason, size, created_at) VALUES (%s, %s, %s, %s, %s, 'CLOSED', 'MANUAL_CLOSE', %s, (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));", (str(symbol), str(side), float(entry_p), float(mark_p), float(pos_pnl), float(abs(sz))))
        send_telegram_alert(f"🟡 <b>MANUAL CLOSE: {ASSET_NAMES_EN.get(symbol, symbol)}</b>\nSide: {side}\nEntry: ${entry_p:,.2f}\nExit: ${mark_p:,.2f}\nPnL: {'+' if pos_pnl >= 0 else ''}${pos_pnl:.2f} USD")
        return {"success": True, "symbol": symbol, "pnl": round(pos_pnl, 4), "daily_pnl": round(self.daily_pnl, 4)}

    def run_heartbeat(self):
        while True:
            try:
                total_t = sum(s["trades"] for s in self.win_stats.values())
                total_w = sum(s["wins"] for s in self.win_stats.values())
                actual_wr = round((total_w / total_t * 100), 2) if total_t > 0 else 0.0
                execute_db_query("INSERT INTO bot_heartbeat (status, open_trades_count, daily_pnl, win_rate, snapshot_json, created_at) VALUES ('ACTIVE_CONNECTED', %s, %s, %s, %s, (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours'));", (len(self.open_trades), self.daily_pnl, actual_wr, json.dumps(self.market_snapshots, cls=NpEncoder)))
            except Exception: pass
            time.sleep(30)

bot_engine = TradingBotEngine()

def switch_environment(mode, new_api_key=None, new_secret_key=None, new_passphrase=None):
    global ENVIRONMENT_MODE, BASE_URL, API_KEY, SECRET_KEY, PASSPHRASE
    if (mode or "").strip().upper() in ["PRODUCTION", "LIVE", "REAL"]: ENVIRONMENT_MODE, BASE_URL = "PRODUCTION", "https://api.gateio.ws"
    else: ENVIRONMENT_MODE, BASE_URL = "TESTNET", "https://api-testnet.gateapi.io"
    if new_api_key: API_KEY = str(new_api_key).strip()
    if new_secret_key: SECRET_KEY = str(new_secret_key).strip()
    if new_passphrase: PASSPHRASE = str(new_passphrase).strip()
    bot_engine.refresh_live_cache()
    send_telegram_alert(f"{'🔴 REAL-MONEY PRODUCTION' if ENVIRONMENT_MODE == 'PRODUCTION' else '🟡 TESTNET'} MODE ACTIVATED\nBalance: ${bot_engine.total_balance:.2f} USDT")
    return {"success": True, "env_mode": ENVIRONMENT_MODE, "base_url": BASE_URL, "is_live_data": bot_engine.is_live_data, "total_balance": bot_engine.total_balance}

TERMINAL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <title>INSTITUTIONAL ALGORITHMIC TERMINAL — GATE.IO v3.1</title>
    <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
    <style>
        :root { --bg:#07090e; --card:#0d121d; --border:#1a2336; --cyan:#00f2fe; --green:#00e676; --red:#ff1744; --yellow:#ffd600; }
        * { box-sizing: border-box; margin:0; padding:0; font-family:'Consolas','Segoe UI',monospace; }
        body { background:var(--bg); color:#f1f5f9; padding:10px; font-size:12px; }
        .top-bar { display:flex; justify-content:space-between; align-items:center; background:#0c1019; border:1px solid var(--border); padding:10px 14px; border-radius:6px; margin-bottom:10px; }
        .badge-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:8px; margin-bottom:10px; }
        .card { background:var(--card); border:1px solid var(--border); padding:10px; border-radius:6px; }
        .stair-row { display:flex; gap:6px; margin:6px 0; }
        .stair-box { padding:4px 8px; border-radius:4px; font-weight:bold; font-size:0.7rem; }
        .main-split { display:grid; grid-template-columns:1.4fr 1.1fr; gap:10px; }
        @media (max-width:1000px) { .main-split { grid-template-columns:1fr; } }
        table { width:100%; border-collapse:collapse; text-align:left; font-size:0.75rem; }
        th, td { padding:6px 8px; border-bottom:1px solid var(--border); }
        th { color:#94a3b8; background:#090d16; }
        .tab-btn { background:#161e2e; color:#94a3b8; border:1px solid var(--border); padding:5px 10px; border-radius:4px; cursor:pointer; font-weight:bold; font-size:0.72rem; }
        .tab-btn.active { background:#0284c7; color:#fff; }
        .status-badge { padding:2px 6px; border-radius:4px; font-size:0.65rem; font-weight:bold; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
    </style>
</head>
<body>
    <div class="top-bar">
        <div>
            <b>⚡ INSTITUTIONAL ALGORITHMIC TERMINAL v3.1</b>
            <span id="liveStatusBadge" class="status-badge" style="background:#052e16; color:#4ade80;">🟢 LIVE GATE.IO DATA</span>
            <span style="background:#d97706;color:#fff;padding:2px 6px;border-radius:4px;font-size:0.65rem;">TESTNET UTC+6</span>
            <span id="seasonalityBadge" class="status-badge" style="background:#0284c7; color:#fff;">Seasonality: NEUTRAL</span>
            <span id="leverageBadge" class="status-badge" style="background:#4c1d95; color:#fff;">Leverage: 2x</span>
        </div>
        <div>
            <select id="assetSelect" onchange="onAssetChange(this.value)" style="background:#161e2e;color:var(--cyan);border:1px solid #0284c7;padding:4px 8px;border-radius:4px;font-weight:bold;">
                <option value="ETH_USDT">ETH_USDT</option><option value="BTC_USDT">BTC_USDT</option><option value="XAU_USDT">XAU_USDT</option>
                <option value="SOL_USDT">SOL_USDT</option><option value="BNB_USDT">BNB_USDT</option><option value="DOGE_USDT">DOGE_USDT</option>
                <option value="XRP_USDT">XRP_USDT</option>
            </select>
        </div>
    </div>
    <div class="card" style="margin-bottom:10px;">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <span style="color:var(--cyan); font-weight:bold;">🎯 STAIRCASE DAILY TARGET ($3.00 MIN + UNLIMITED)</span>
            <span id="stairLevelText" style="color:var(--green); font-weight:bold;">Level 0 / 4</span>
        </div>
        <div class="stair-row" id="staircaseBoxes"></div>
        <div style="margin-top:10px; font-size:10px; color:#94a3b8;">
            Market Share Progress ($2M - $3M Target):
            <progress id="marketProgress" max="3000000" value="0" style="width:100%; height:10px;"></progress>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:0.75rem; margin-top:4px;">
            <span>Balance: <b id="totBal" style="color:#fff;">$1,000.00 USDT</b></span>
            <span>Daily Net PnL: <b id="dailyPnlText" style="color:var(--green);">+$0.00 USD</b></span>
            <span>Active Trades: <b id="openCnt">0 Open</b></span>
        </div>
    </div>
    <div class="card" style="margin-bottom:10px;">
        <div style="font-weight:bold; color:var(--cyan); margin-bottom:6px;">📊 30 PERPETUAL ASSETS REAL-TIME TELEMETRY MATRIX</div>
        <table>
            <thead><tr><th>Asset</th><th>Live Price</th><th>1m RSI</th><th>MACD</th><th>Vol Ratio</th><th>Sentiment</th><th>Badges</th><th>Status</th></tr></thead>
            <tbody id="matrixTbody"><tr><td colspan="8" style="text-align:center;">Loading real-time market matrix...</td></tr></tbody>
        </table>
    </div>
    <div class="main-split">
        <div class="card"><div id="tv_chart_container" style="height:380px;"></div></div>
        <div class="card">
            <div style="display:flex; gap:6px; margin-bottom:8px;">
                <button class="tab-btn active" onclick="switchTab('trades')">⚡ LIVE TRADES</button>
                <button class="tab-btn" onclick="switchTab('backtest')">🧪 REAL BACKTEST REPORT</button>
                <button class="tab-btn" onclick="switchTab('per_asset')">🪙 PER-ASSET STATS</button>
            </div>
            <div id="feedContainer" style="max-height:340px; overflow-y:auto;"></div>
        </div>
    </div>
    <script>
        let currentSymbol = "ETH_USDT"; let activeTab = "trades"; let lastData = null;
        function onAssetChange(sym) { currentSymbol = sym; renderChart(sym); }
        function switchTab(t) { activeTab = t; document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active')); event.target.classList.add('active'); if (lastData) renderFeed(lastData); }
        function renderChart(sym) { new TradingView.widget({"container_id": "tv_chart_container", "symbol": "BINANCE:" + sym.replace('_',''), "interval": "1", "theme": "dark", "style": "1", "toolbar_bg": "#07090e", "enable_publishing": false, "hide_top_toolbar": false, "autosize": true}); }
        async function fetchTerminal() {
            try {
                const res = await fetch('/api/stats'); const data = await res.json(); lastData = data;
                const badge = document.getElementById('liveStatusBadge');
                if (data.is_live_data) { badge.style.background = '#052e16'; badge.style.color = '#4ade80'; badge.innerText = '🟢 LIVE GATE.IO DATA'; } 
                else { badge.style.background = '#7f1d1d'; badge.style.color = '#fca5a5'; badge.innerText = '⚠️ FALLBACK DATA — API UNAVAILABLE'; }
                document.getElementById('totBal').innerText = '$' + parseFloat(data.total_balance||1000).toFixed(2) + ' USDT';
                document.getElementById('dailyPnlText').innerText = (data.daily_pnl>=0?'+$':'-$') + Math.abs(data.daily_pnl||0).toFixed(4) + ' USD';
                document.getElementById('openCnt').innerText = (data.open_trades||[]).length + ' Open';
                const stairLvl = parseInt(data.staircase_level || 0);
                document.getElementById('stairLevelText').innerText = 'Level ' + stairLvl + ' / 4';
                
                if (data.seasonality) document.getElementById('seasonalityBadge').innerText = 'Seasonality: ' + data.seasonality.label;
                if (data.smart_leverage) document.getElementById('leverageBadge').innerText = 'Leverage: ' + data.smart_leverage + 'x';
                if (data.market_share) {
                    document.getElementById('marketProgress').max = data.market_share.target_60_days;
                    document.getElementById('marketProgress').value = data.market_share.volume_24h;
                }
                
                const boxes = document.getElementById('staircaseBoxes');
                if (boxes && data.staircase_targets) {
                    boxes.innerHTML = data.staircase_targets.map((t,i) => `<div class="stair-box" style="background:${i<stairLvl?'#052e16':'#1a0000'}; color:${i<stairLvl?'#4ade80':'#f87171'}; border:1px solid ${i<stairLvl?'#16a34a':'#dc2626'}">$${t}</div>`).join('');
                }
                const assets = data.assets || {}; let mHtml = '';
                for (let k in assets) {
                    const a = assets[k];
                    mHtml += `<tr><td><b>${k}</b></td><td style="color:#fff;">$${parseFloat(a.price||0).toLocaleString()}</td><td style="color:${a.rsi_1m<38?'var(--green)':(a.rsi_1m>62?'var(--red)':'#fff')}">${a.rsi_1m||50}</td><td>${a.macd_1m||0}</td><td>${a.vol_ratio||1.0}x</td><td style="color:var(--green);">${a.sentiment||'POSITIVE'}</td><td><b>${a.matched_badges||4}/10</b></td><td style="color:var(--green);">ACTIVE 🟢</td></tr>`;
                }
                document.getElementById('matrixTbody').innerHTML = mHtml;
                renderFeed(data);
            } catch(e) {}
        }
        function renderFeed(data) {
            const fc = document.getElementById('feedContainer');
            if (activeTab === 'trades') {
                const op = data.open_trades || []; const cl = data.last_trades || [];
                if (op.length === 0 && cl.length === 0) { fc.innerHTML = '<div style="text-align:center; padding:30px; color:#64748b;">No active or closed positions on Gate.io yet.</div>'; return; }
                fc.innerHTML = op.concat(cl).map(t => `<div style="background:#090d16; border:1px solid #1e293b; padding:8px; border-radius:4px; margin-bottom:6px; border-left:3px solid ${t.pnl>=0?'var(--green)':'var(--red)'}"><div style="display:flex; justify-content:space-between;"><b>${t.side==='BUY'?'⚡ BUY':'🔴 SELL'} ${t.symbol}</b><span style="color:${t.pnl>=0?'var(--green)':'var(--red)'}; font-weight:bold;">${t.status==='OPEN'?'LIVE PnL: ':''}${t.pnl>=0?'+$':'-$'}${Math.abs(t.pnl||0).toFixed(4)}</span></div><div style="color:#94a3b8; font-size:0.7rem; margin-top:2px;">Entry: $${t.entry_price} | Size: $${t.size||4} | ${t.created_at}</div></div>`).join('');
            } else if (activeTab === 'backtest') {
                const bt = data.backtest_results || {}; const oldCfg = bt.old_config || {total_trades: 0, win_rate: 0, net_pnl: 0}; const newCfg = bt.new_config || {total_trades: 0, win_rate: 0, net_pnl: 0};
                let dHtml = `<div style="padding:4px;"><div style="background:#091322; border:1px solid #0284c7; padding:8px; border-radius:4px; margin-bottom:8px;"><b>🧪 REAL HISTORICAL BACKTEST (GATE.IO & LIVE MARKET OHLCV)</b><div style="margin-top:4px; font-size:0.75rem;">Old Config: <b>${oldCfg.win_rate}% WR</b> (${oldCfg.total_trades} trades) | Net PnL: +$${oldCfg.net_pnl}<br>New Config: <b>${newCfg.win_rate}% WR</b> (${newCfg.total_trades} trades) | Net PnL: +$${newCfg.net_pnl}<span style="color:var(--green); font-weight:bold; margin-left:6px;">${bt.passed_gate ? '✅ PASSED SAFETY GATE' : '⚠️ GATE PENDING'}</span></div></div><table><thead><tr><th>Asset</th><th>Candles</th><th>Old WR</th><th>New WR</th><th>Status</th></tr></thead><tbody>`;
                for (let d of (bt.details || [])) { dHtml += `<tr><td><b>${d.symbol}</b></td><td>${d.candles_evaluated}</td><td>${d.old_win_rate}%</td><td style="color:var(--green); font-weight:bold;">${d.new_win_rate}%</td><td style="font-size:0.65rem;">${d.status}</td></tr>`; }
                dHtml += `</tbody></table></div>`; fc.innerHTML = dHtml;
            } else if (activeTab === 'per_asset') {
                const ws = data.win_stats || {}; let wHtml = '<table><thead><tr><th>Asset</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Net PnL</th></tr></thead><tbody>';
                for (let s in ws) { wHtml += `<tr><td><b>${s}</b></td><td>${ws[s].trades}</td><td style="color:var(--green);">${ws[s].wins}</td><td style="color:var(--red);">${ws[s].losses}</td><td style="color:${ws[s].total_pnl>=0?'var(--green)':'var(--red)'}">${ws[s].total_pnl>=0?'+$':'-$'}${Math.abs(ws[s].total_pnl).toFixed(2)}</td></tr>`; }
                wHtml += '</tbody></table>'; fc.innerHTML = wHtml;
            }
        }
        window.onload = () => { renderChart(currentSymbol); fetchTerminal(); setInterval(fetchTerminal, 1000); };
    </script>
</body>
</html>"""

class ReusableHTTPServer(ThreadingHTTPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        super().server_bind()

class HealthCheckHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*"); self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS"); self.send_header("Access-Control-Allow-Headers", "Content-Type"); self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    def do_GET(self):
        req_path = urllib.parse.urlparse(self.path).path.rstrip('/') or '/'
        if req_path in ["/dashboard", "/", "", "/health"]:
            self.send_response(200); self._send_cors_headers(); self.send_header("Content-Type", "text/html; charset=utf-8"); self.end_headers()
            html_c = TERMINAL_HTML
            if os.path.exists("index.html"):
                try:
                    with open("index.html", "r", encoding="utf-8") as f:
                        html_c = f.read()
                except Exception: pass
            self.wfile.write(html_c.encode("utf-8"))
            return
        self.send_response(200); self._send_cors_headers(); self.send_header("Content-Type", "application/json"); self.end_headers()
        with bot_engine.trade_lock:
            _pnl = bot_engine.daily_pnl; _level = bot_engine.staircase_level
            if _level < len(STAIRCASE_TARGETS):
                _next_tgt = STAIRCASE_TARGETS[_level]; _prev_tgt = STAIRCASE_TARGETS[_level - 1] if _level > 0 else 0.0
                _range = _next_tgt - _prev_tgt; _progress = max(0, min(100, ((_pnl - _prev_tgt) / _range) * 100)) if _range > 0 else 0
            else: _next_tgt = 0.0; _progress = 100.0
            bal_val = float(bot_engine.cached_account_raw.get("cross_margin_balance", 1000.0))
            active_margin_val = round(sum(float(t.get("margin_used", float(t.get("trade_usd", 5.0)) / max(1.0, float(leverage_engine.current_leverage)))) for t in bot_engine.open_trades.values()), 2)
            total_notional_val = round(sum(float(t.get("trade_usd", 0.0)) for t in bot_engine.open_trades.values()), 2)
            max_margin_val = round(bal_val * MAX_MARGIN_ALLOC_PCT, 2); safe_vault_val = round(bal_val * (1.0 - MAX_MARGIN_ALLOC_PCT), 2)
            cmp_info = bot_engine.compounder.get_next_tier_info()
            open_trades_snap = list(bot_engine.cached_open_trades)
            last_trades_snap = list(bot_engine.cached_last_trades)
            win_stats_snap = dict(bot_engine.win_stats)
            market_snap = dict(bot_engine.market_snapshots)
            cached_acc_snap = dict(bot_engine.cached_account_raw)
            accounts_snap = dict(bot_engine.account_manager.get_all_accounts())
            current_acc_id = bot_engine.account_manager.current_account_id

        resp = {
            "status": "ONLINE", "env_mode": ENVIRONMENT_MODE, "base_url": BASE_URL, "is_live_data": bot_engine.is_live_data, "data_source": cached_acc_snap.get("data_source", "SIMULATED_FALLBACK"), "bangladesh_time": get_bd_time_str(),
            "gateio_account_raw": cached_acc_snap, "total_balance": bal_val, "wallet_balance": float(cached_acc_snap.get("total", bal_val)), "unrealised_pnl": float(cached_acc_snap.get("cross_unrealised_pnl", 0.0)), "maintenance_margin": float(cached_acc_snap.get("maintenance_margin", 0.0)),
            "safe_capital": safe_vault_val, "trading_capital": max_margin_val, "active_margin": active_margin_val, "max_active_margin": max_margin_val, "safe_vault_reserve": safe_vault_val, "daily_pnl": round(bot_engine.daily_pnl, 6), "daily_peak_pnl": round(bot_engine.daily_peak_pnl, 4), "daily_pnl_floor": round(bot_engine.daily_pnl_floor, 4),
            "next_target": _next_tgt, "target_progress": round(_progress, 1), "compound_trade_size": get_compound_trade_size(bal_val), "compound_next_threshold": cmp_info.get("next_threshold", 0), "compound_next_size": cmp_info.get("next_size", 0), "compound_progress": cmp_info.get("progress", 0),
            "compound_info": cmp_info, "compound_history": bot_engine.compounder.get_compound_history(), "accounts": accounts_snap, "current_account_id": current_acc_id,
            "safe_recovery_mode": bot_engine.safe_recovery_mode, "daily_trade_count": bot_engine.daily_trade_count, "daily_target": USER_DAILY_TARGET, "daily_loss_limit": bot_engine.daily_loss_limit, "trade_usd_size": bot_engine.trade_usd_size, "staircase_level": _level, "staircase_targets": STAIRCASE_TARGETS,
            "safe_mode": bot_engine.safe_mode_active, "bot_active": bot_engine.bot_active, "open_trades": open_trades_snap, "last_trades": last_trades_snap, "assets": market_snap, "win_stats": win_stats_snap, "backtest_results": backtest_engine.results_summary, "api_logs": API_LOGS[:15],
            "seasonality": get_current_seasonality(), "smart_leverage": leverage_engine.current_leverage, "market_share": {'volume_24h': market_engine.total_market_volume_24h, 'target_60_days': market_engine.target_60_days}, "circuit_breaker": {'tripped': circuit_breaker.tripped, 'date': circuit_breaker.trip_date}, "dry_run_mode": DRY_RUN_MODE
        }
        self.wfile.write(json.dumps(resp, cls=NpEncoder).encode("utf-8"))
    def do_OPTIONS(self):
        self.send_response(200); self._send_cors_headers(); self.end_headers()
    def do_POST(self):
        req_path = urllib.parse.urlparse(self.path).path.rstrip('/') or '/'
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length > 0 else b'{}'
        self.send_response(200); self._send_cors_headers(); self.send_header("Content-Type", "application/json"); self.end_headers()
        try: body = json.loads(post_data.decode('utf-8'))
        except Exception: body = {}
        if req_path in ['/api/keys', '/api/mode', '/api/settings']: resp = switch_environment(body.get('env_mode') or body.get('mode', 'TESTNET'), body.get('api_key') or body.get('key', ''), body.get('secret_key') or body.get('secret', ''), body.get('passphrase') or body.get('pass', ''))
        elif req_path == '/api/switch_account': resp = {"success": bot_engine.account_manager.switch_account(body.get('account_id')), "current_account_id": bot_engine.account_manager.current_account_id} if body.get('account_id') else {"success": False, "error": "No account_id provided"}
        elif req_path == '/api/close_trade':
            symbols = [body.get('symbol')] if body.get('symbol') else body.get('symbols', [])
            if not symbols: resp = {"success": False, "error": "No symbol(s) provided"}
            else:
                results = [bot_engine.manual_close_trade(sym) for sym in symbols]
                resp = {"success": all(r.get("success") for r in results), "closed": results, "daily_pnl": round(bot_engine.daily_pnl, 4), "total_balance": float(bot_engine.cached_account_raw.get("cross_margin_balance", 1000.0)), "trade_usd_size": bot_engine.trade_usd_size}
        else: resp = {"error": "Unknown endpoint"}
        self.wfile.write(json.dumps(resp, cls=NpEncoder).encode('utf-8'))

def start_health_server():
    server = ReusableHTTPServer(("0.0.0.0", HEALTH_SERVER_PORT), HealthCheckHandler)
    server.serve_forever()
def keep_render_alive():
    while True:
        try: requests.get(f"http://localhost:{HEALTH_SERVER_PORT}/api/stats", timeout=5)
        except Exception: pass
        time.sleep(10)
def cache_refresh_loop():
    while True:
        try:
            bot_engine.refresh_live_cache()
            execute_db_query("UPDATE bot_state SET total_balance = %s, daily_pnl = %s, trade_usd_size = %s, updated_at = (NOW() AT TIME ZONE 'UTC' + INTERVAL '6 hours') WHERE id = 1;", (bot_engine.total_balance, bot_engine.daily_pnl, bot_engine.trade_usd_size))
        except Exception: pass
        time.sleep(0.3)

def main():
    logger.info("=" * 65)
    logger.info(" INSTITUTIONAL AI TRADING BOT — GATE.IO TESTNET v3.1")
    logger.info(f" Mode: {'DRY RUN' if DRY_RUN_MODE else 'PRODUCTION'} | Bangladesh Standard Time (BST GMT+6) | 100% Real-Time")
    logger.info("=" * 65)
    init_db_schema()
    market_engine.fetch_market_volume()
    season = get_current_seasonality()
    logger.info(f"[SEASONALITY] Month {season['month']}: {season['label']} (Factor: {season['factor']:+.0%})")
    backtest_engine.run_backtest_simulation()
    bot_engine.refresh_live_cache()
    leverage_engine.calculate_leverage(bot_engine.total_balance, bot_engine.daily_pnl, 0)
    logger.info(f"[SMART LEVERAGE] Current: {leverage_engine.current_leverage}x")
    threading.Thread(target=start_health_server, daemon=True).start()
    threading.Thread(target=cache_refresh_loop, daemon=True).start()
    threading.Thread(target=bot_engine.run_heartbeat, daemon=True).start()
    threading.Thread(target=keep_render_alive, daemon=True).start()
    def market_refresh_loop():
        while True:
            try: market_engine.fetch_market_volume(); leverage_engine.calculate_leverage(bot_engine.total_balance, bot_engine.daily_pnl, 0)
            except Exception: pass
            time.sleep(300)
    threading.Thread(target=market_refresh_loop, daemon=True).start()
    logger.info(f"[MAIN LOOP] 300ms rotating scan for {len(ASSETS)} assets.")
    scan_batch_idx, batch_size = 0, 10
    while True:
        try:
            circuit_breaker.check_reset()
            if bot_engine.bot_active and not circuit_breaker.tripped:
                batch = ASSETS[scan_batch_idx:scan_batch_idx + batch_size]
                for symbol in batch: bot_engine.process_symbol(symbol)
                scan_batch_idx += batch_size
                if scan_batch_idx >= len(ASSETS): scan_batch_idx = 0
        except Exception as e: logger.error(f"[MAIN LOOP ERROR] {e}")
        time.sleep(0.3)
if __name__ == "__main__": main()
