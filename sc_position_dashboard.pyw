"""
AMT Trade Journal — Automated Trade Journal & Position Viewer for Sierra Chart

Connects to multiple Sierra Chart instances via DTC Protocol (WebSocket + JSON)
and displays all open positions in a unified dark-themed tkinter dashboard.

Dependencies: pip install websocket-client
"""

import calendar as cal_mod
import json
import os
import sqlite3
import statistics
import sys
import threading
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

try:
    import websocket
except ImportError:
    print("Missing dependency. Install with:\n  pip install websocket-client")
    sys.exit(1)

import zipfile

import tkinter as tk
from tkinter import messagebox, ttk

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# 'accounts' is optional — if set, only those accounts are tracked.
# If omitted or empty, ALL accounts on that instance are shown.
_DEFAULT_INSTANCES = {
    'Sierra Chart':  {'host': '127.0.0.1', 'port': 11050},
}

# Mutable — populated from config file at startup
SC_INSTANCES = {}

# Config file path (same directory as script)
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sc_instances.json')


def load_config():
    """Load instance config from JSON file. Seeds defaults on first run."""
    global SC_INSTANCES, USER_TIMEZONE
    if not os.path.exists(CONFIG_PATH):
        SC_INSTANCES = seed_default_config()
        return SC_INSTANCES
    try:
        with open(CONFIG_PATH, 'r') as f:
            data = json.load(f)
        instances = data.get('instances', {})
        # Migrate: ensure each instance has 'enabled' and 'accounts' keys
        for name, cfg in instances.items():
            cfg.setdefault('enabled', True)
            cfg.setdefault('accounts', [])
        SC_INSTANCES = instances
        USER_TIMEZONE = data.get('timezone', 'US/Central')
        return SC_INSTANCES
    except (json.JSONDecodeError, OSError) as e:
        print(f"Error loading config: {e}. Using defaults.")
        SC_INSTANCES = seed_default_config()
        return SC_INSTANCES


def save_config(instances=None):
    """Write instance config to JSON file."""
    if instances is None:
        instances = SC_INSTANCES
    data = {'timezone': USER_TIMEZONE, 'instances': instances}
    try:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(data, f, indent=2)
    except OSError as e:
        print(f"Error saving config: {e}")


def seed_default_config():
    """Convert hardcoded _DEFAULT_INSTANCES to config format and save."""
    instances = {}
    for name, cfg in _DEFAULT_INSTANCES.items():
        instances[name] = {
            'host': cfg['host'],
            'port': cfg['port'],
            'accounts': cfg.get('accounts', []),
            'enabled': True,
        }
    save_config(instances)
    return instances

CONTRACT_MULTIPLIERS = {
    # Indices
    'ES': 50, 'MES': 5, 'NQ': 20, 'MNQ': 2, 'RTY': 50, 'M2K': 5,
    'YM': 5, 'MYM': 0.5,
    # Energy
    'CL': 1000, 'MCL': 100, 'NG': 10000, 'MNG': 1000,
    # Metals
    'GC': 100, 'MGC': 10, 'SI': 5000, 'SIL': 1000, 'HG': 25000, 'MHG': 2500,
    'PL': 50, 'PA': 100,
    # Bonds / Rates
    'ZB': 1000, 'ZN': 1000, 'ZF': 1000, 'ZT': 2000,
    # Grains
    'ZC': 50, 'ZS': 50, 'ZW': 50, 'ZM': 100, 'ZL': 600,
    # Softs
    'KC': 375, 'SB': 1120, 'CT': 500, 'CC': 10,
    # Meats
    'HE': 400, 'LE': 400,
    # Currencies
    '6E': 125000, '6J': 12500000, '6B': 62500, '6A': 100000, '6C': 100000, '6S': 125000,
}

SYMBOL_GROUPS = {
    'S&P 500':    ['ES', 'MES'],
    'Nasdaq':     ['NQ', 'MNQ'],
    'Russell':    ['RTY', 'M2K'],
    'Dow':        ['YM', 'MYM'],
    'Crude Oil':  ['CL', 'MCL'],
    'Nat Gas':    ['NG', 'MNG'],
    'Gold':       ['GC', 'MGC'],
    'Silver':     ['SI', 'SIL'],
    'Copper':     ['HG', 'MHG'],
    'Platinum':   ['PL'],
    'Palladium':  ['PA'],
    'Bonds 30Y':  ['ZB'],
    'Notes 10Y':  ['ZN'],
    'Notes 5Y':   ['ZF'],
    'Notes 2Y':   ['ZT'],
    'Corn':       ['ZC'],
    'Soybeans':   ['ZS'],
    'Wheat':      ['ZW'],
    'Soy Meal':   ['ZM'],
    'Soy Oil':    ['ZL'],
    'Coffee':     ['KC'],
    'Sugar':      ['SB'],
    'Cotton':     ['CT'],
    'Cocoa':      ['CC'],
    'Hogs':       ['HE'],
    'Cattle':     ['LE'],
    'Euro FX':    ['6E'],
    'Yen':        ['6J'],
    'Pound':      ['6B'],
    'Aussie':     ['6A'],
    'Canadian':   ['6C'],
    'Swiss':      ['6S'],
}

MICRO_SYMBOLS = {'MES', 'MNQ', 'M2K', 'MYM', 'MCL', 'MNG', 'MGC', 'SIL', 'MHG'}
FULL_SYMBOLS  = {'ES', 'NQ', 'RTY', 'YM', 'CL', 'NG', 'GC', 'SI', 'HG', 'PL', 'PA',
                 'ZB', 'ZN', 'ZF', 'ZT', 'ZC', 'ZS', 'ZW', 'ZM', 'ZL',
                 'KC', 'SB', 'CT', 'CC', 'HE', 'LE', '6E', '6J', '6B', '6A', '6C', '6S'}

# All symbols that belong to a known group
_ALL_GROUPED_SYMBOLS = set()
for _syms in SYMBOL_GROUPS.values():
    _ALL_GROUPED_SYMBOLS.update(_syms)

# Reverse lookup: base symbol → group name
_SYMBOL_TO_GROUP = {}
for _grp, _syms in SYMBOL_GROUPS.items():
    for _s in _syms:
        _SYMBOL_TO_GROUP[_s] = _grp


def get_symbol_group(base_symbol):
    """Return the product group name for a base symbol, or the symbol itself."""
    return _SYMBOL_TO_GROUP.get(base_symbol, base_symbol)


RECONNECT_DELAY = 5       # seconds between reconnect attempts
HEARTBEAT_INTERVAL = 10   # seconds between heartbeats
GUI_REFRESH_MS = 500       # milliseconds between GUI refreshes (no positions)
GUI_REFRESH_FAST_MS = 100  # milliseconds between GUI refreshes (positions open)

FILLS_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sc_fills.db')
FILLS_START_TIMESTAMP = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp())
PERF_REFRESH_MS = 2000    # Performance tab refresh interval
DTC_PRICE_DIVISOR = 100   # SC DTC JSON sends prices as price * 100
POSITION_POLL_SECONDS = 3   # re-request positions/balances every 3s
FILLS_POLL_SECONDS    = 30  # re-request fills every 30s

# Session/timezone for breakdown tab
USER_TIMEZONE = 'US/Central'   # Updated from config at startup; handles DST automatically
SESSION_DEFS = [           # (label, start_hour, start_min, end_hour, end_min) in local time
    ('Morning',    7,  0,  9,  0),
    ('Midday',     9,  0, 12,  0),
    ('Afternoon', 12,  0, 14,  0),
]  # Anything outside these → 'Overnight'
DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# ─────────────────────────────────────────────────────────────────────────────
# DTC PROTOCOL CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

DTC_ENCODING_REQUEST            = 6
DTC_ENCODING_RESPONSE           = 7
DTC_LOGON_REQUEST               = 1
DTC_LOGON_RESPONSE              = 2
DTC_HEARTBEAT                   = 3
DTC_TRADE_ACCOUNTS_REQUEST      = 400
DTC_TRADE_ACCOUNT_RESPONSE      = 401
DTC_CURRENT_POSITIONS_REQUEST   = 305
DTC_POSITION_UPDATE             = 306
DTC_ACCOUNT_BALANCE_REQUEST     = 601
DTC_ACCOUNT_BALANCE_UPDATE      = 600
DTC_HISTORICAL_ORDER_FILLS_REQ  = 303
DTC_HISTORICAL_ORDER_FILL_RESP  = 304

DTC_JSON_COMPACT_ENCODING       = 3

# ─────────────────────────────────────────────────────────────────────────────
# GUI CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

BG_PRIMARY   = '#0d1117'
BG_ALT       = '#161b22'
BG_HEADER    = '#21262d'
FG_TEXT       = '#c9d1d9'
FG_ACCENT    = '#58a6ff'
FG_GREEN     = '#3fb950'
FG_RED       = '#f97583'
FG_DIM       = '#8b949e'
FONT_FAMILY  = 'Segoe UI'
FONT_SIZE    = 10

# ─────────────────────────────────────────────────────────────────────────────
# DATA STORE
# ─────────────────────────────────────────────────────────────────────────────

DATA_LOCK = threading.Lock()


def init_data_store():
    """Create the shared data store dict."""
    store = {
        'positions': {},       # key: (instance, account, symbol) → dict
        'accounts': defaultdict(list),  # key: instance_name → [account, ...]
        'fills': defaultdict(list),     # key: (instance, account, symbol) → [fill, ...]
        'status': {},          # key: instance_name → 'Connected' | 'Disconnected' | 'Connecting...'
        'balances': {},        # key: (instance, account) → {cash_balance, securities_value, ...}
        'ws_refs': {},         # key: instance_name → ws object (for cleanup)
        'shutdown': False,
        'perf_dirty': True,    # signals performance tab to re-render
        'perf_acct_filter': 'All Accounts',  # current account filter selection
        'perf_date_range': 'All Time',  # current date range filter
        'disabled': set(),  # instance names manually disconnected (skip auto-reconnect)
        'today_dirty': True,  # signals Today tab to re-render
        '_poll_tick': 0,      # counter for periodic refresh cycles
    }
    for name in SC_INSTANCES:
        store['status'][name] = 'Disconnected'
    return store

# ─────────────────────────────────────────────────────────────────────────────
# FILL PERSISTENCE (SQLite)
# ─────────────────────────────────────────────────────────────────────────────


def init_fills_db():
    """Create the fills database and table if they don't exist."""
    conn = sqlite3.connect(FILLS_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance TEXT, account TEXT, symbol TEXT,
            price REAL, quantity INTEGER, buy_sell TEXT, date_time TEXT,
            UNIQUE(instance, account, symbol, date_time, price, quantity, buy_sell)
        )
    """)
    conn.commit()
    conn.close()


def save_fill_to_db(instance, account, symbol, fill):
    """Persist a single fill to SQLite. INSERT OR IGNORE handles dedup."""
    try:
        conn = sqlite3.connect(FILLS_DB_PATH)
        conn.execute(
            "INSERT OR IGNORE INTO fills (instance, account, symbol, price, quantity, buy_sell, date_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (instance, account, symbol,
             fill['price'], fill['quantity'], fill['buy_sell'], fill['date_time'])
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def load_fills_from_db():
    """Load all fills from DB into the store fills dict format. Normalizes BuySell/DateTime."""
    fills = defaultdict(list)
    try:
        conn = sqlite3.connect(FILLS_DB_PATH)
        cursor = conn.execute(
            "SELECT instance, account, symbol, price, quantity, buy_sell, date_time "
            "FROM fills ORDER BY date_time"
        )
        for row in cursor:
            key = (row[0], row[1], row[2])
            fills[key].append(normalize_fill({
                'price': row[3],
                'quantity': row[4],
                'buy_sell': row[5],
                'date_time': row[6],
            }))
        conn.close()
    except Exception:
        pass
    return fills


# ─────────────────────────────────────────────────────────────────────────────
# DTC MESSAGE HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def make_dtc_message(msg_type, **fields):
    """Build a null-terminated JSON DTC message."""
    msg = {'Type': msg_type}
    msg.update(fields)
    return json.dumps(msg, separators=(',', ':')) + '\x00'


def safe_parse_messages(raw):
    """Parse one or more null-terminated JSON messages from a raw WebSocket frame."""
    results = []
    for chunk in raw.split('\x00'):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            results.append(json.loads(chunk))
        except (json.JSONDecodeError, ValueError):
            pass
    return results


def get_base_symbol(symbol_str):
    """Extract base symbol from a contract string like 'ESH26', 'ESH6.CME', 'MCLJ6.NYMEX'.
    Strips the trailing futures month code letter (F,G,H,J,K,M,N,Q,U,V,X,Z)."""
    if not symbol_str:
        return ''
    clean = symbol_str.strip().split()[0] if ' ' in symbol_str else symbol_str.strip()
    alpha = ''
    for ch in clean:
        if ch.isalpha():
            alpha += ch
        else:
            break
    alpha = alpha.upper()
    # Try full alpha prefix first, then strip trailing month code
    if alpha in CONTRACT_MULTIPLIERS:
        return alpha
    if len(alpha) > 1 and alpha[:-1] in CONTRACT_MULTIPLIERS:
        return alpha[:-1]
    return alpha


_warned_symbols = set()

def get_multiplier(symbol_str):
    """Look up point value for a symbol."""
    base = get_base_symbol(symbol_str)
    if base not in CONTRACT_MULTIPLIERS and base not in _warned_symbols:
        _warned_symbols.add(base)
        print(f"WARNING: No CONTRACT_MULTIPLIER for '{base}' — P&L will be wrong (using 1). Add it to CONTRACT_MULTIPLIERS.")
    return CONTRACT_MULTIPLIERS.get(base, 1)


def normalize_buy_sell(raw):
    """Normalize DTC BuySell to 'Buy' or 'Sell'. Handles int, str int, string names."""
    s = str(raw).strip().lower()
    if s in ('1', 'buy', 'b'):
        return 'Buy'
    if s in ('2', 'sell', 's'):
        return 'Sell'
    return raw  # fallback — pass through


def normalize_datetime(raw):
    """Convert DTC DateTime (Unix timestamp int/str) to ISO string 'YYYY-MM-DD HH:MM:SS'.
    If already ISO-like, pass through."""
    if raw is None or raw == '':
        return ''
    # If it's a number or looks like a pure numeric string → Unix timestamp
    try:
        ts = float(raw)
        if ts > 1_000_000_000:  # clearly a Unix timestamp
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError, OSError):
        pass
    return str(raw)


def normalize_fill(fill):
    """Normalize a fill dict's BuySell, DateTime, and Price fields in-place, return it."""
    fill['buy_sell'] = normalize_buy_sell(fill.get('buy_sell', ''))
    fill['date_time'] = normalize_datetime(fill.get('date_time', ''))
    # SC DTC JSON sends prices scaled by 100 — divide to get real price
    price = fill.get('price', 0.0)
    if isinstance(price, (int, float)):
        fill['price'] = price / DTC_PRICE_DIVISOR
    return fill


# ─────────────────────────────────────────────────────────────────────────────
# DTC MESSAGE HANDLERS
# ─────────────────────────────────────────────────────────────────────────────


def handle_position_update(instance_name, msg, store):
    """Process a POSITION_UPDATE (Type 306) message."""
    account = msg.get('TradeAccount', '')
    symbol = msg.get('Symbol', '')
    qty = msg.get('Quantity', 0)
    avg_price = msg.get('AveragePrice', 0.0)
    # SC DTC JSON sends prices scaled by 100 — divide to get real price
    if isinstance(avg_price, (int, float)):
        avg_price = avg_price / DTC_PRICE_DIVISOR

    if not account or not symbol:
        return

    pos_key = (instance_name, account, symbol)

    with DATA_LOCK:
        if qty == 0:
            store['positions'].pop(pos_key, None)
            store['perf_dirty'] = True
            store['today_dirty'] = True
        else:
            store['positions'][pos_key] = {
                'instance': instance_name,
                'account': account,
                'symbol': symbol,
                'quantity': qty,
                'avg_price': avg_price,
                'open_pnl': msg.get('OpenProfitLoss', 0.0),
                'margin_req': msg.get('MarginRequirement', 0.0),
            }


def handle_fill_response(instance_name, msg, store):
    """Process a HISTORICAL_ORDER_FILL_RESPONSE (Type 304) message."""
    account = msg.get('TradeAccount', '')
    symbol = msg.get('Symbol', '')
    if not account or not symbol:
        return

    fill_key = (instance_name, account, symbol)

    # Raw fill for DB persistence (preserves original DTC values for dedup)
    raw_fill = {
        'price': msg.get('Price', .0),
        'quantity': msg.get('Quantity', 0),
        'buy_sell': str(msg.get('BuySell', '')),
        'date_time': str(msg.get('DateTime', '')),
    }
    save_fill_to_db(instance_name, account, symbol, raw_fill)

    # Normalized fill for in-memory use
    fill_entry = normalize_fill({
        'price': msg.get('Price', .0),
        'quantity': msg.get('Quantity', 0),
        'buy_sell': msg.get('BuySell', ''),
        'date_time': msg.get('DateTime', ''),
    })

    with DATA_LOCK:
        # Avoid in-memory duplicates (compare on normalized datetime + price)
        existing = store['fills'][fill_key]
        is_dup = any(
            f['date_time'] == fill_entry['date_time'] and
            f['quantity'] == fill_entry['quantity'] and
            f['buy_sell'] == fill_entry['buy_sell']
            for f in existing
        )
        if not is_dup:
            existing.append(fill_entry)
        store['perf_dirty'] = True


def compute_closed_pnl_for_key(fill_key, store):
    """Compute closed PnL from fills using FIFO matching. Best-effort."""
    fills = store['fills'].get(fill_key, [])
    if not fills:
        return 0.0

    symbol = fill_key[2]
    multiplier = get_multiplier(symbol)
    closed_pnl = 0.0
    open_lots = []  # list of (qty_signed, price)

    for f in sorted(fills, key=lambda x: x.get('date_time', '')):
        qty = f.get('quantity', 0)
        price = f.get('price', 0.0)
        bs = f.get('buy_sell', '')

        if normalize_buy_sell(bs) == 'Sell':
            qty = -qty

        # Match against open lots
        remaining = qty
        new_open = []
        for (olot_qty, olot_price) in open_lots:
            if remaining == 0:
                new_open.append((olot_qty, olot_price))
                continue
            # Opposite sides can close
            if (olot_qty > 0 and remaining < 0) or (olot_qty < 0 and remaining > 0):
                close_qty = min(abs(olot_qty), abs(remaining))
                pnl_per = (price - olot_price) if olot_qty > 0 else (olot_price - price)
                closed_pnl += pnl_per * close_qty * multiplier
                leftover_lot = olot_qty + (close_qty if olot_qty < 0 else -close_qty)
                if leftover_lot != 0:
                    new_open.append((leftover_lot, olot_price))
                remaining += close_qty if remaining < 0 else -close_qty
            else:
                new_open.append((olot_qty, olot_price))

        if remaining != 0:
            new_open.append((remaining, price))
        open_lots = new_open

    return closed_pnl

# ─────────────────────────────────────────────────────────────────────────────
# TRADE MATCHING & STATS
# ─────────────────────────────────────────────────────────────────────────────


def compute_round_trip_trades(fills, symbol, instance='', account=''):
    """FIFO match fills into paired entry/exit round-trip trades.

    Returns list of dicts with entry/exit time, prices, side, qty, P&L.
    """
    multiplier = get_multiplier(symbol)
    sorted_fills = sorted(fills, key=lambda x: x.get('date_time', ''))
    open_lots = []  # list of {'qty': signed_qty, 'price': float, 'time': str, 'side': str}
    trades = []

    for f in sorted_fills:
        qty = f.get('quantity', 0)
        price = f.get('price', 0.0)
        bs = normalize_buy_sell(f.get('buy_sell', ''))
        dt = f.get('date_time', '')

        side = 'Long' if bs == 'Buy' else 'Short'
        signed_qty = qty if side == 'Long' else -qty

        remaining = signed_qty
        new_open = []

        for lot in open_lots:
            if remaining == 0:
                new_open.append(lot)
                continue

            # Can close if opposite sides
            if (lot['qty'] > 0 and remaining < 0) or (lot['qty'] < 0 and remaining > 0):
                close_qty = min(abs(lot['qty']), abs(remaining))
                if lot['qty'] > 0:
                    pnl_pts = price - lot['price']
                else:
                    pnl_pts = lot['price'] - price

                # Parse exit date for trade_date (YYYY-MM-DD from ISO string)
                trade_date = dt[:10] if len(dt) >= 10 else dt

                trades.append({
                    'entry_time': lot['time'],
                    'exit_time': dt,
                    'entry_price': lot['price'],
                    'exit_price': price,
                    'side': lot['side'],
                    'qty': close_qty,
                    'symbol': symbol,
                    'instance': instance,
                    'account': account,
                    'pnl_points': pnl_pts,
                    'pnl_dollars': pnl_pts * close_qty * multiplier,
                    'trade_date': trade_date,
                })

                leftover = lot['qty'] + (close_qty if lot['qty'] < 0 else -close_qty)
                if leftover != 0:
                    new_open.append({**lot, 'qty': leftover})
                remaining += close_qty if remaining < 0 else -close_qty
            else:
                new_open.append(lot)

        if remaining != 0:
            new_open.append({'qty': remaining, 'price': price, 'time': dt, 'side': side})
        open_lots = new_open

    return trades


def aggregate_all_round_trips(store, acct_filter=None):
    """Gather round-trip trades across all instances/accounts/symbols.

    acct_filter: None or 'All Accounts' for all, a set of 'Instance: Account' labels,
                 or a single 'Instance: Account' string.
    """
    with DATA_LOCK:
        fills_snapshot = {k: list(v) for k, v in store['fills'].items()}

    all_trades = []
    for (inst, acct, sym), fills in fills_snapshot.items():
        # Apply account filter
        if acct_filter and acct_filter != 'All Accounts':
            filter_label = f"{inst}: {acct}"
            if isinstance(acct_filter, set):
                if filter_label not in acct_filter:
                    continue
            elif filter_label != acct_filter:
                continue
        all_trades.extend(compute_round_trip_trades(fills, sym, inst, acct))

    # Sort by exit time descending (most recent first)
    all_trades.sort(key=lambda t: t.get('exit_time', ''), reverse=True)
    return all_trades


def compute_performance_stats(trades):
    """Compute performance stats from a list of round-trip trades."""
    stats = {
        'total_trades': 0, 'wins': 0, 'losses': 0, 'win_rate': 0.0,
        'net_pnl': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0,
        'max_win': 0.0, 'max_loss': 0.0, 'profit_factor': 0.0,
        'max_drawdown': 0.0, 'sharpe': 0.0, 'sortino': 0.0, 'kelly': 0.0,
        'long_count': 0, 'long_wins': 0, 'long_pnl': 0.0,
        'short_count': 0, 'short_wins': 0, 'short_pnl': 0.0,
        'daily_pnl': {}, 'cumulative_pnl': [],
        'avg_duration': 0.0, 'avg_win_duration': 0.0, 'avg_loss_duration': 0.0,
        'hourly_pnl': {}, 'max_win_streak': 0, 'max_loss_streak': 0,
    }

    if not trades:
        return stats

    wins_list = []
    losses_list = []
    daily_pnl = defaultdict(float)
    cumulative = 0.0
    cumulative_pnl = []
    peak = 0.0
    max_dd = 0.0

    # Process in chronological order for cumulative stats
    for t in reversed(trades):
        pnl = t['pnl_dollars']
        cumulative += pnl
        cumulative_pnl.append(cumulative)

        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

        if pnl > 0:
            wins_list.append(pnl)
        elif pnl < 0:
            losses_list.append(pnl)

        date_key = t.get('trade_date', '')
        if date_key:
            daily_pnl[date_key] += pnl

        if t['side'] == 'Long':
            stats['long_count'] += 1
            stats['long_pnl'] += pnl
            if pnl > 0:
                stats['long_wins'] += 1
        else:
            stats['short_count'] += 1
            stats['short_pnl'] += pnl
            if pnl > 0:
                stats['short_wins'] += 1

    stats['total_trades'] = len(trades)
    stats['wins'] = len(wins_list)
    stats['losses'] = len(losses_list)
    stats['win_rate'] = len(wins_list) / len(trades) * 100 if trades else 0
    stats['net_pnl'] = cumulative
    stats['avg_win'] = statistics.mean(wins_list) if wins_list else 0.0
    stats['avg_loss'] = statistics.mean(losses_list) if losses_list else 0.0
    stats['max_win'] = max(wins_list) if wins_list else 0.0
    stats['max_loss'] = min(losses_list) if losses_list else 0.0
    stats['max_drawdown'] = max_dd

    gross_wins = sum(wins_list)
    gross_losses = abs(sum(losses_list))
    stats['profit_factor'] = gross_wins / gross_losses if gross_losses > 0 else float('inf') if gross_wins > 0 else 0.0

    # Sharpe & Sortino — daily returns, annualized
    daily_returns = list(daily_pnl.values())
    if len(daily_returns) >= 2:
        avg_ret = statistics.mean(daily_returns)
        std_ret = statistics.stdev(daily_returns)
        stats['sharpe'] = (avg_ret / std_ret) * (252 ** 0.5) if std_ret > 0 else 0.0

        neg_returns = [r for r in daily_returns if r < 0]
        if neg_returns:
            downside_std = (sum(r ** 2 for r in neg_returns) / len(daily_returns)) ** 0.5
            stats['sortino'] = (avg_ret / downside_std) * (252 ** 0.5) if downside_std > 0 else 0.0

    # Kelly Criterion: K% = W - (1-W)/R  where W=win_rate, R=avg_win/avg_loss
    if stats['avg_win'] > 0 and stats['avg_loss'] != 0:
        w = stats['win_rate'] / 100
        r = stats['avg_win'] / abs(stats['avg_loss'])
        stats['kelly'] = (w - (1 - w) / r) * 100 if r > 0 else 0.0
    else:
        stats['kelly'] = 0.0

    stats['daily_pnl'] = dict(daily_pnl)
    stats['cumulative_pnl'] = cumulative_pnl

    # Duration stats — parse entry/exit ISO strings, compute timedelta
    all_durations = []
    win_durations = []
    loss_durations = []
    for t in trades:
        entry_str = t.get('entry_time', '')
        exit_str = t.get('exit_time', '')
        if len(entry_str) >= 19 and len(exit_str) >= 19:
            try:
                entry_dt = datetime.strptime(entry_str[:19], '%Y-%m-%d %H:%M:%S')
                exit_dt = datetime.strptime(exit_str[:19], '%Y-%m-%d %H:%M:%S')
                dur_secs = (exit_dt - entry_dt).total_seconds()
                if dur_secs > 0:
                    all_durations.append(dur_secs)
                    if t['pnl_dollars'] > 0:
                        win_durations.append(dur_secs)
                    elif t['pnl_dollars'] < 0:
                        loss_durations.append(dur_secs)
            except (ValueError, TypeError):
                pass
    stats['avg_duration'] = statistics.mean(all_durations) if all_durations else 0.0
    stats['avg_win_duration'] = statistics.mean(win_durations) if win_durations else 0.0
    stats['avg_loss_duration'] = statistics.mean(loss_durations) if loss_durations else 0.0

    # Win/loss streak stats (chronological order = reversed trades list)
    max_win_streak = cur_win = 0
    max_loss_streak = cur_loss = 0
    for t in reversed(trades):
        if t['pnl_dollars'] > 0:
            cur_win += 1
            cur_loss = 0
            max_win_streak = max(max_win_streak, cur_win)
        elif t['pnl_dollars'] < 0:
            cur_loss += 1
            cur_win = 0
            max_loss_streak = max(max_loss_streak, cur_loss)
        else:
            cur_win = cur_loss = 0
    stats['max_win_streak'] = max_win_streak
    stats['max_loss_streak'] = max_loss_streak

    # Hourly P&L — aggregate by exit hour (local timezone)
    hourly_pnl = defaultdict(float)
    for t in trades:
        _, hour, _ = _exit_time_to_local(t.get('exit_time', ''))
        if hour is not None:
            hourly_pnl[hour] += t['pnl_dollars']
    stats['hourly_pnl'] = dict(hourly_pnl)

    return stats


def _utc_to_local_dt(utc_time_str):
    """Convert a UTC ISO string to a local datetime, or None on failure."""
    try:
        dt_utc = datetime.strptime(utc_time_str[:19], '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo('UTC'))
        return dt_utc.astimezone(ZoneInfo(USER_TIMEZONE))
    except (ValueError, TypeError, KeyError):
        return None


def _exit_time_to_local(exit_time_str):
    """Parse an exit time ISO string and return (weekday_index, hour, minute) in user timezone."""
    dt_local = _utc_to_local_dt(exit_time_str)
    if dt_local:
        return dt_local.weekday(), dt_local.hour, dt_local.minute
    return None, None, None


def _format_local_time(utc_time_str):
    """Convert UTC ISO string to (date_str, time_str) in user timezone."""
    dt_local = _utc_to_local_dt(utc_time_str)
    if dt_local:
        return dt_local.strftime('%Y-%m-%d'), dt_local.strftime('%H:%M:%S')
    date_part = utc_time_str[:10] if len(utc_time_str) >= 10 else utc_time_str
    time_part = utc_time_str[11:19] if len(utc_time_str) >= 19 else ''
    return date_part, time_part


def _get_session(hour, minute):
    """Return session label for a given CT hour:minute."""
    if hour is None:
        return 'Unknown'
    t = hour * 60 + minute
    for label, sh, sm, eh, em in SESSION_DEFS:
        if sh * 60 + sm <= t < eh * 60 + em:
            return label
    return 'Overnight'


def _breakdown_row(trades_in_bucket):
    """Compute a breakdown row dict from a list of trades."""
    wins = [t for t in trades_in_bucket if t['pnl_dollars'] > 0]
    losses = [t for t in trades_in_bucket if t['pnl_dollars'] < 0]
    total = len(trades_in_bucket)
    win_pnl = sum(t['pnl_dollars'] for t in wins)
    loss_pnl = sum(t['pnl_dollars'] for t in losses)
    net_pnl = win_pnl + loss_pnl
    avg_win = win_pnl / len(wins) if wins else 0
    avg_loss = loss_pnl / len(losses) if losses else 0
    win_pct = len(wins) / total * 100 if total else 0
    ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
    return {
        'total': total,
        'win_count': len(wins), 'win_pnl': win_pnl, 'avg_win': avg_win,
        'loss_count': len(losses), 'loss_pnl': loss_pnl, 'avg_loss': avg_loss,
        'net_pnl': net_pnl, 'win_pct': win_pct, 'ratio': ratio,
    }



def compute_breakdown_stats(trades, sym_filter='All Symbols'):
    """Compute session, day-of-week, symbol, monthly, and day×hour breakdown stats.

    sym_filter controls symbol grouping:
      - 'All Symbols': group by product group name (e.g. ES+MES → 'S&P 500')
      - A group name (e.g. 'S&P 500'): break down by individual symbol within that group
      - An individual symbol: single row for that symbol
    """
    session_buckets = defaultdict(list)
    dow_buckets = defaultdict(list)
    symbol_buckets = defaultdict(list)
    monthly_buckets = defaultdict(list)
    dayhour_pnl = defaultdict(float)

    # Determine symbol bucketing strategy
    group_by_product = (sym_filter == 'All Symbols')

    for t in trades:
        weekday, hour, minute = _exit_time_to_local(t.get('exit_time', ''))
        session = _get_session(hour, minute)
        session_buckets[session].append(t)
        if weekday is not None:
            dow_buckets[weekday].append(t)
            if hour is not None:
                dayhour_pnl[(weekday, hour)] += t['pnl_dollars']

        # Symbol bucket — group by product name or individual symbol
        base = get_base_symbol(t.get('symbol', ''))
        if base:
            if group_by_product:
                label = get_symbol_group(base)
            else:
                label = base
            symbol_buckets[label].append(t)

        # Monthly bucket (YYYY-MM from trade_date)
        td = t.get('trade_date', '')
        if len(td) >= 7:
            monthly_buckets[td[:7]].append(t)

    # Build session rows in defined order, plus Overnight
    session_order = [s[0] for s in SESSION_DEFS] + ['Overnight']
    session_rows = []
    for label in session_order:
        bucket = session_buckets.get(label, [])
        if bucket:
            row = _breakdown_row(bucket)
            row['label'] = label
            session_rows.append(row)

    # Build day-of-week rows Mon→Sun
    dow_rows = []
    for i, name in enumerate(DAY_NAMES):
        bucket = dow_buckets.get(i, [])
        if bucket:
            row = _breakdown_row(bucket)
            row['label'] = name
            dow_rows.append(row)

    # Build symbol rows sorted by net P&L descending
    symbol_rows = []
    for sym, bucket in symbol_buckets.items():
        row = _breakdown_row(bucket)
        row['label'] = sym
        symbol_rows.append(row)
    symbol_rows.sort(key=lambda r: r['net_pnl'], reverse=True)

    # Build monthly rows sorted reverse chronological
    monthly_rows = []
    for month_key, bucket in monthly_buckets.items():
        row = _breakdown_row(bucket)
        row['label'] = month_key
        monthly_rows.append(row)
    monthly_rows.sort(key=lambda r: r['label'], reverse=True)

    return {
        'session': session_rows, 'dow': dow_rows,
        'symbol': symbol_rows, 'monthly': monthly_rows,
        'dayhour_pnl': dict(dayhour_pnl),
    }


# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET CONNECTION
# ─────────────────────────────────────────────────────────────────────────────


def start_instance_connection(instance_name, cfg, store):
    """Launch a WebSocket connection thread for one SC instance."""
    host = cfg['host']
    port = cfg['port']
    url = f"ws://{host}:{port}"

    accounts_received = set()

    def on_open(ws):
        with DATA_LOCK:
            store['status'][instance_name] = 'Connected'
            store['ws_refs'][instance_name] = ws
        # Step 1: Send encoding request
        ws.send(make_dtc_message(
            DTC_ENCODING_REQUEST,
            ProtocolVersion=8,
            Encoding=DTC_JSON_COMPACT_ENCODING,
            ProtocolType="DTC",
        ))

    def on_message(ws, raw):
        messages = safe_parse_messages(raw)
        for msg in messages:
            msg_type = msg.get('Type', 0)
            if msg_type == DTC_ENCODING_RESPONSE:
                # Send logon
                ws.send(make_dtc_message(
                    DTC_LOGON_REQUEST,
                    ProtocolVersion=8,
                    HeartbeatIntervalInSeconds=HEARTBEAT_INTERVAL,
                    ClientName="SC_Dashboard",
                    TradeMode=1,
                ))

            elif msg_type == DTC_LOGON_RESPONSE:
                result = msg.get('Result', 0)
                if result == 1:
                    # Request trade accounts
                    ws.send(make_dtc_message(DTC_TRADE_ACCOUNTS_REQUEST, RequestID=1))
                else:
                    desc = msg.get('ResultText', 'Unknown error')
                    print(f"[{instance_name}] Logon failed: {desc}")

            elif msg_type == DTC_TRADE_ACCOUNT_RESPONSE:
                account = msg.get('TradeAccount', '')
                # Filter: skip accounts not in the whitelist (if set)
                acct_filter = cfg.get('accounts', [])
                if acct_filter and account not in acct_filter:
                    continue
                if account and account not in accounts_received:
                    accounts_received.add(account)
                    with DATA_LOCK:
                        if account not in store['accounts'][instance_name]:
                            store['accounts'][instance_name].append(account)
                    # Request positions for this account
                    ws.send(make_dtc_message(
                        DTC_CURRENT_POSITIONS_REQUEST,
                        TradeAccount=account,
                        RequestID=hash((instance_name, account, 'pos')) & 0x7FFFFFFF,
                    ))
                    # Request historical fills back to 1/1/2025
                    days_back = (datetime.now(timezone.utc) - datetime(2025, 1, 1, tzinfo=timezone.utc)).days + 1
                    ws.send(make_dtc_message(
                        DTC_HISTORICAL_ORDER_FILLS_REQ,
                        TradeAccount=account,
                        RequestID=hash((instance_name, account, 'fills')) & 0x7FFFFFFF,
                        NumberOfDays=max(days_back, 1),
                        StartDateTime=FILLS_START_TIMESTAMP,
                    ))
                    # Request account balance
                    ws.send(make_dtc_message(
                        DTC_ACCOUNT_BALANCE_REQUEST,
                        TradeAccount=account,
                        RequestID=hash((instance_name, account, 'bal')) & 0x7FFFFFFF,
                    ))

            elif msg_type == DTC_POSITION_UPDATE:
                handle_position_update(instance_name, msg, store)

            elif msg_type == DTC_HISTORICAL_ORDER_FILL_RESP:
                handle_fill_response(instance_name, msg, store)

            elif msg_type == DTC_HEARTBEAT:
                pass  # Server heartbeat, no action needed

            elif msg_type == DTC_ACCOUNT_BALANCE_UPDATE:
                acct = msg.get('TradeAccount', '')
                if acct:
                    bal_key = (instance_name, acct)
                    with DATA_LOCK:
                        store['balances'][bal_key] = {
                            'cash_balance': msg.get('CashBalance', 0.0),
                            'balance_available': msg.get('BalanceAvailableForNewPositions', 0.0),
                            'securities_value': msg.get('SecuritiesValue', 0.0),
                            'open_pl': msg.get('OpenPositionsProfitLoss', 0.0),
                        }

    def on_error(ws, error):
        pass  # on_close will handle reconnect

    def on_close(ws, close_status, close_msg):
        with DATA_LOCK:
            store['status'][instance_name] = 'Disconnected'
            store['ws_refs'].pop(instance_name, None)
        # Auto-reconnect after delay (skip if manually disabled)
        if not store['shutdown'] and instance_name not in store.get('disabled', set()):
            t = threading.Timer(RECONNECT_DELAY, start_instance_connection,
                                args=(instance_name, cfg, store))
            t.daemon = True
            t.start()

    def run_ws():
        with DATA_LOCK:
            store['status'][instance_name] = 'Connecting...'
        try:
            ws_app = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws_app.run_forever(ping_interval=0)
        except Exception:
            with DATA_LOCK:
                store['status'][instance_name] = 'Disconnected'
            if not store['shutdown'] and instance_name not in store.get('disabled', set()):
                t = threading.Timer(RECONNECT_DELAY, start_instance_connection,
                                    args=(instance_name, cfg, store))
                t.daemon = True
                t.start()

    thread = threading.Thread(target=run_ws, name=f"ws-{instance_name}", daemon=True)
    thread.start()
    return thread


def disconnect_instance(name, store):
    """Manually disconnect an instance and prevent auto-reconnect."""
    with DATA_LOCK:
        store['disabled'].add(name)
        ws = store['ws_refs'].pop(name, None)
        store['status'][name] = 'Disconnected'
    if ws:
        try:
            ws.close()
        except Exception:
            pass


def cleanup_instance_store(name, store):
    """Remove all store entries keyed by a removed instance."""
    with DATA_LOCK:
        store['disabled'].discard(name)
        store['ws_refs'].pop(name, None)
        store['status'].pop(name, None)
        store['accounts'].pop(name, None)
        # Remove positions, fills, balances keyed by this instance
        for key in list(store['positions']):
            if key[0] == name:
                del store['positions'][key]
        for key in list(store['fills']):
            if key[0] == name:
                del store['fills'][key]
        for key in list(store['balances']):
            if key[0] == name:
                del store['balances'][key]
        store['perf_dirty'] = True


# ─────────────────────────────────────────────────────────────────────────────
# GUI
# ─────────────────────────────────────────────────────────────────────────────

STAT_TOOLTIPS = {
    'total_trades': "Total number of completed round-trip trades\n"
                    "in the selected period.",
    'win_rate': "Percentage of trades that were profitable.\n"
                ">50% with good risk/reward = solid edge.",
    'profit_factor': "Gross wins / gross losses.\n"
                     ">1 = profitable. >2 = strong. >3 = exceptional.",
    'net_pnl': "Total realized profit or loss\n"
               "across all trades in the selected period.",
    'avg_win': "Average dollar amount of winning trades.",
    'avg_loss': "Average dollar amount of losing trades.\n"
                "Compare to Avg Win for risk/reward ratio.",
    'max_drawdown': "Largest peak-to-trough decline\n"
                    "in cumulative P&L. Measures worst\n"
                    "losing streak in dollar terms.",
    'max_win': "Largest single winning trade.",
    'max_loss': "Largest single losing trade.",
    'sharpe': "Sharpe Ratio — risk-adjusted return.\n"
              "(Avg daily P&L / StdDev daily P&L) × √252\n"
              ">1 good, >2 great, >3 exceptional.\n"
              "Needs 30+ trading days for reliable values.",
    'sortino': "Sortino Ratio — like Sharpe but only\n"
               "penalizes downside volatility.\n"
               "(Avg daily P&L / Downside StdDev) × √252\n"
               "Higher is better. Ignores upside swings.\n"
               "Needs 30+ trading days for reliable values.",
    'kelly': "Kelly Criterion — optimal risk % per trade.\n"
             "K = W − (1−W)/R\n"
             "W = win rate, R = avg win / avg loss.\n"
             "Positive = edge exists. >20% = strong edge.\n"
             "Needs 30+ trades for reliable values.",
    'ls_split': "Long/Short split — number of long vs\n"
                "short trades in the selected period.",
    'avg_duration': "Average time held across all trades.",
    'avg_win_duration': "Average time held for winning trades.\n"
                        "Compare to Avg Loss Dur for patterns.",
    'avg_loss_duration': "Average time held for losing trades.\n"
                         "Long hold times on losses may indicate\n"
                         "reluctance to cut losers.",
    'max_win_streak': "Most consecutive winning trades.",
    'max_loss_streak': "Most consecutive losing trades.",
}


def _add_tooltip(widget, text):
    """Attach a hover tooltip to a widget."""
    tip = None

    def show(event):
        nonlocal tip
        tip = tk.Toplevel(widget)
        tip.wm_overrideredirect(True)
        tip.wm_attributes('-topmost', True)
        x = event.x_root + 12
        y = event.y_root + 10
        tip.wm_geometry(f'+{x}+{y}')
        lbl = tk.Label(tip, text=text, bg='#1c2128', fg=FG_TEXT,
                       font=(FONT_FAMILY, 9), padx=8, pady=6,
                       relief='solid', borderwidth=1, justify=tk.LEFT)
        lbl.pack()

    def hide(event):
        nonlocal tip
        if tip:
            tip.destroy()
            tip = None

    widget.bind('<Enter>', show)
    widget.bind('<Leave>', hide)


class MultiAccountSelector:
    """Menubutton with checkboxes for multi-account selection."""

    def __init__(self, parent, store, on_change=None):
        self._store = store
        self._on_change = on_change
        self._accounts = []  # list of 'Instance: Account' strings
        self._vars = {}      # label → BooleanVar
        self._all_var = tk.BooleanVar(value=True)
        self._updating = False  # guard against recursive callbacks

        self.btn = tk.Menubutton(parent, text='All Accounts',
                                 bg=BG_HEADER, fg=FG_TEXT,
                                 activebackground='#30363d', activeforeground=FG_TEXT,
                                 font=(FONT_FAMILY, 10), relief='flat',
                                 padx=8, pady=3, indicatoron=False,
                                 highlightthickness=0, borderwidth=1)
        self.menu = tk.Menu(self.btn, tearoff=False,
                            bg=BG_HEADER, fg=FG_TEXT,
                            activebackground='#30363d', activeforeground=FG_ACCENT,
                            font=(FONT_FAMILY, 10), relief='flat',
                            borderwidth=1, selectcolor=FG_ACCENT)
        self.btn.configure(menu=self.menu)
        self._rebuild_menu()

    def pack(self, **kwargs):
        self.btn.pack(**kwargs)

    def pack_forget(self):
        self.btn.pack_forget()

    def _rebuild_menu(self):
        self.menu.delete(0, tk.END)
        self.menu.add_checkbutton(label='All Accounts', variable=self._all_var,
                                  command=self._on_all_toggled)
        if self._accounts:
            self.menu.add_separator()
        for label in self._accounts:
            var = self._vars.get(label)
            if var is None:
                var = tk.BooleanVar(value=False)
                self._vars[label] = var
            self.menu.add_checkbutton(label=label, variable=var,
                                      command=lambda l=label: self._on_item_toggled(l))

    def _on_all_toggled(self):
        if self._updating:
            return
        self._updating = True
        if self._all_var.get():
            for var in self._vars.values():
                var.set(False)
        else:
            # Don't allow unchecking All if nothing else is checked
            self._all_var.set(True)
        self._updating = False
        self._update_text()
        self._fire_change()

    def _on_item_toggled(self, label):
        if self._updating:
            return
        self._updating = True
        # If any individual is checked, uncheck "All"
        any_checked = any(v.get() for v in self._vars.values())
        if any_checked:
            self._all_var.set(False)
        else:
            # Nothing checked — revert to All
            self._all_var.set(True)
        self._updating = False
        self._update_text()
        self._fire_change()

    def _update_text(self):
        if self._all_var.get():
            self.btn.configure(text='All Accounts')
            return
        selected = [l for l, v in self._vars.items() if v.get()]
        if len(selected) == 0:
            self.btn.configure(text='All Accounts')
        elif len(selected) == 1:
            self.btn.configure(text=selected[0])
        else:
            self.btn.configure(text=f'{len(selected)} Accounts')

    def _fire_change(self):
        filt = self.get_filter()
        self._store['perf_acct_filter'] = filt
        self._store['perf_dirty'] = True
        if self._on_change:
            self._on_change(filt)

    def get_filter(self):
        """Return 'All Accounts' or a set of selected labels."""
        if self._all_var.get():
            return 'All Accounts'
        selected = {l for l, v in self._vars.items() if v.get()}
        return selected if selected else 'All Accounts'

    def set_filter(self, value):
        """Set filter from 'All Accounts' string or a set of labels. No callback fired."""
        self._updating = True
        if value == 'All Accounts' or not value:
            self._all_var.set(True)
            for var in self._vars.values():
                var.set(False)
        elif isinstance(value, set):
            self._all_var.set(False)
            for label, var in self._vars.items():
                var.set(label in value)
        self._updating = False
        self._update_text()

    def update_options(self, account_list):
        """Refresh available account options (preserves current selection)."""
        if sorted(account_list) == sorted(self._accounts):
            return
        current_filter = self.get_filter()
        # Remove vars for accounts no longer present
        old_labels = set(self._vars.keys())
        new_labels = set(account_list)
        for removed in old_labels - new_labels:
            del self._vars[removed]
        # Add vars for new accounts
        for label in account_list:
            if label not in self._vars:
                self._vars[label] = tk.BooleanVar(value=False)
        self._accounts = list(account_list)
        self._rebuild_menu()
        # Restore selection
        self.set_filter(current_filter)


def build_gui(store):
    """Build the tkinter GUI with Notebook tabs for Positions and Performance."""
    root = tk.Tk()
    root.title("AMT Trade Journal")
    root.configure(bg=BG_PRIMARY)
    root.geometry("1200x700")
    root.minsize(900, 500)

    # Window icon
    icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'amt_logo.ico')
    if os.path.isfile(icon_path):
        root.iconbitmap(icon_path)

    # --- Style ---
    style = ttk.Style()
    style.theme_use('clam')

    style.configure('Dashboard.Treeview',
                    background=BG_PRIMARY,
                    foreground=FG_TEXT,
                    fieldbackground=BG_PRIMARY,
                    font=(FONT_FAMILY, FONT_SIZE),
                    rowheight=28,
                    borderwidth=0)
    style.configure('Dashboard.Treeview.Heading',
                    background=BG_HEADER,
                    foreground=FG_ACCENT,
                    font=(FONT_FAMILY, FONT_SIZE, 'bold'),
                    borderwidth=0,
                    relief='flat')
    style.map('Dashboard.Treeview',
              background=[('selected', '#30363d')],
              foreground=[('selected', FG_TEXT)])
    style.map('Dashboard.Treeview.Heading',
              background=[('active', BG_HEADER)])

    # Notebook styling
    style.configure('Dark.TNotebook', background=BG_PRIMARY, borderwidth=0)
    style.configure('Dark.TNotebook.Tab',
                    background=BG_HEADER, foreground=FG_DIM,
                    font=(FONT_FAMILY, 10, 'bold'),
                    padding=[14, 6])
    style.map('Dark.TNotebook.Tab',
              background=[('selected', BG_PRIMARY)],
              foreground=[('selected', FG_ACCENT)])

    # Combobox styling
    style.configure('Dark.TCombobox',
                    fieldbackground=BG_HEADER, background=BG_HEADER,
                    foreground=FG_TEXT, selectbackground=BG_HEADER,
                    selectforeground=FG_ACCENT,
                    font=(FONT_FAMILY, 10))
    style.map('Dark.TCombobox',
              fieldbackground=[('readonly', BG_HEADER)],
              foreground=[('readonly', FG_TEXT)])

    # Button styles for Connection Manager dialogs
    style.configure('CM.TButton',
                    background=BG_HEADER, foreground=FG_TEXT,
                    font=(FONT_FAMILY, 10), padding=(14, 4))
    style.map('CM.TButton',
              background=[('active', BG_ALT)],
              foreground=[('active', FG_TEXT)])
    style.configure('CMAccent.TButton',
                    background=BG_HEADER, foreground=FG_ACCENT,
                    font=(FONT_FAMILY, 10, 'bold'), padding=(14, 4))
    style.map('CMAccent.TButton',
              background=[('active', BG_ALT)],
              foreground=[('active', FG_ACCENT)])

    # --- Status bar (top — shared) ---
    status_frame = tk.Frame(root, bg=BG_ALT, padx=10, pady=6)
    status_frame.pack(fill=tk.X, side=tk.TOP)

    status_labels = {}
    balance_labels = {}
    instance_frames = {}
    for name in SC_INSTANCES:
        frame = tk.Frame(status_frame, bg=BG_ALT)
        frame.pack(side=tk.LEFT, padx=(0, 16))
        lbl = tk.Label(frame,
                       text=f"{name}: Disconnected",
                       bg=BG_ALT,
                       fg=FG_RED,
                       font=(FONT_FAMILY, 9))
        lbl.pack()
        bal_lbl = tk.Label(frame,
                           text="",
                           bg=BG_ALT,
                           fg=FG_DIM,
                           font=(FONT_FAMILY, 8))
        bal_lbl.pack()
        status_labels[name] = lbl
        balance_labels[name] = bal_lbl
        instance_frames[name] = frame

    # Gear button — opens Connection Manager
    gear_btn = tk.Label(status_frame, text='\u2699', bg=BG_ALT, fg=FG_DIM,
                        font=(FONT_FAMILY, 14), cursor='hand2')
    gear_btn.pack(side=tk.RIGHT, padx=(8, 0))
    gear_btn.bind('<Enter>', lambda e: gear_btn.configure(fg=FG_ACCENT))
    gear_btn.bind('<Leave>', lambda e: gear_btn.configure(fg=FG_DIM))
    gear_btn.bind('<Button-1>', lambda e: open_connection_manager(root, store))

    # Store GUI refs so dialog can rebuild status bar
    store['gui'] = {
        'root': root,
        'status_frame': status_frame,
        'status_labels': status_labels,
        'balance_labels': balance_labels,
        'instance_frames': instance_frames,
        'gear_btn': gear_btn,
    }

    # --- Totals bar (bottom — pack BEFORE notebook so it claims space first) ---
    totals_frame = tk.Frame(root, bg=BG_ALT, padx=10, pady=8)
    totals_frame.pack(fill=tk.X, side=tk.BOTTOM)

    totals_label = tk.Label(totals_frame,
                            text="Open PnL: $0.00  |  Positions: 0",
                            bg=BG_ALT,
                            fg=FG_TEXT,
                            font=(FONT_FAMILY, FONT_SIZE, 'bold'))
    totals_label.pack(side=tk.LEFT)

    web_lbl = tk.Label(totals_frame, text='amtbalance.com', bg=BG_ALT, fg=FG_DIM,
                       font=(FONT_FAMILY, 9), cursor='hand2')
    web_lbl.pack(side=tk.RIGHT)
    web_lbl.bind('<Enter>', lambda e: web_lbl.configure(fg=FG_ACCENT))
    web_lbl.bind('<Leave>', lambda e: web_lbl.configure(fg=FG_DIM))
    web_lbl.bind('<Button-1>', lambda e: __import__('webbrowser').open('https://amtbalance.com'))

    # --- Notebook ---
    notebook = ttk.Notebook(root, style='Dark.TNotebook')
    notebook.pack(fill=tk.BOTH, expand=True, padx=4, pady=(4, 0))

    # ===================== POSITIONS TAB =====================
    pos_tab = tk.Frame(notebook, bg=BG_PRIMARY)
    notebook.add(pos_tab, text='  Positions  ')

    columns = ('instance', 'account', 'symbol', 'qty', 'avg_price', 'open_pnl', 'closed_pnl')
    tree_frame = tk.Frame(pos_tab, bg=BG_PRIMARY)
    tree_frame.pack(fill=tk.BOTH, expand=True)

    scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
    tree = ttk.Treeview(tree_frame, columns=columns, show='headings',
                        style='Dashboard.Treeview',
                        yscrollcommand=scrollbar.set)
    scrollbar.configure(command=tree.yview)

    headings = {
        'instance': ('Instance', 90),
        'account': ('Account', 160),
        'symbol': ('Symbol', 120),
        'qty': ('Qty', 60),
        'avg_price': ('Avg Price', 110),
        'open_pnl': ('Open PnL', 110),
        'closed_pnl': ('Closed PnL', 110),
    }
    for col, (label, width) in headings.items():
        tree.heading(col, text=label, anchor=tk.W)
        anchor = tk.E if col in ('qty', 'avg_price', 'open_pnl', 'closed_pnl') else tk.W
        tree.column(col, width=width, minwidth=50, anchor=anchor)

    tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    tree.tag_configure('profit', foreground=FG_GREEN)
    tree.tag_configure('loss', foreground=FG_RED)
    tree.tag_configure('flat', foreground=FG_TEXT)
    tree.tag_configure('alt_profit', background=BG_ALT, foreground=FG_GREEN)
    tree.tag_configure('alt_loss', background=BG_ALT, foreground=FG_RED)
    tree.tag_configure('alt_flat', background=BG_ALT, foreground=FG_TEXT)

    # ===================== TODAY TAB =====================
    today_tab = tk.Frame(notebook, bg=BG_PRIMARY)
    notebook.add(today_tab, text='  Today  ')

    # --- Stats cards (2 rows of 5) ---
    today_stats_frame = tk.Frame(today_tab, bg=BG_HEADER, padx=10, pady=8)
    today_stats_frame.pack(fill=tk.X, pady=(0, 4))

    today_stat_cards = {}
    today_stat_defs = [
        ('Trades', 'total_trades'), ('Win Rate', 'win_rate'), ('Profit Factor', 'profit_factor'),
        ('Net P&L', 'net_pnl'), ('Avg Win', 'avg_win'),
        ('Avg Loss', 'avg_loss'), ('Max Win', 'max_win'), ('Max Loss', 'max_loss'),
        ('L/S Split', 'ls_split'), ('Streaks', 'streaks'),
    ]
    for i, (label, key) in enumerate(today_stat_defs):
        row, col = divmod(i, 5)
        card = tk.Frame(today_stats_frame, bg=BG_HEADER, padx=8, pady=2)
        card.grid(row=row, column=col, sticky='ew', padx=4, pady=2)
        today_stats_frame.columnconfigure(col, weight=1)
        has_tip = key in STAT_TOOLTIPS
        name_text = f"{label} \u24D8" if has_tip else label
        name_lbl = tk.Label(card, text=name_text, bg=BG_HEADER,
                            fg=FG_ACCENT if has_tip else FG_DIM,
                            font=(FONT_FAMILY, 8))
        name_lbl.pack()
        if has_tip:
            _add_tooltip(name_lbl, STAT_TOOLTIPS[key])
        val_lbl = tk.Label(card, text='—', bg=BG_HEADER, fg=FG_TEXT,
                           font=(FONT_FAMILY, 12, 'bold'))
        val_lbl.pack()
        today_stat_cards[key] = val_lbl

    # --- Content area: trade log (left) + charts (right) ---
    today_content = tk.PanedWindow(today_tab, orient=tk.HORIZONTAL, bg=BG_PRIMARY,
                                   sashwidth=4, sashrelief=tk.FLAT)
    today_content.pack(fill=tk.BOTH, expand=True)

    # Left: Trade log
    today_log_frame = tk.Frame(today_content, bg=BG_PRIMARY)
    today_content.add(today_log_frame, minsize=400, stretch='always')

    today_log_cols = ('date', 'time', 'symbol', 'side', 'qty', 'entry', 'exit', 'pnl', 'dur')
    today_log_scroll = ttk.Scrollbar(today_log_frame, orient=tk.VERTICAL)
    today_trade_tree = ttk.Treeview(today_log_frame, columns=today_log_cols, show='headings',
                                    style='Dashboard.Treeview',
                                    yscrollcommand=today_log_scroll.set)
    today_log_scroll.configure(command=today_trade_tree.yview)

    today_log_headings = {
        'date': ('Date', 85), 'time': ('Time', 70), 'symbol': ('Symbol', 80),
        'side': ('Side', 50), 'qty': ('Qty', 40), 'entry': ('Entry', 85),
        'exit': ('Exit', 85), 'pnl': ('P&L', 95), 'dur': ('Dur', 65),
    }
    for col, (label, width) in today_log_headings.items():
        today_trade_tree.heading(col, text=label, anchor=tk.W)
        anchor = tk.E if col in ('qty', 'entry', 'exit', 'pnl', 'dur') else tk.W
        today_trade_tree.column(col, width=width, minwidth=35, anchor=anchor)

    today_trade_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    today_log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    today_trade_tree.tag_configure('profit', foreground=FG_GREEN)
    today_trade_tree.tag_configure('loss', foreground=FG_RED)
    today_trade_tree.tag_configure('flat', foreground=FG_TEXT)
    today_trade_tree.tag_configure('alt_profit', background=BG_ALT, foreground=FG_GREEN)
    today_trade_tree.tag_configure('alt_loss', background=BG_ALT, foreground=FG_RED)
    today_trade_tree.tag_configure('alt_flat', background=BG_ALT, foreground=FG_TEXT)

    # Right: equity curve on top, duration chart on bottom
    today_right = tk.Frame(today_content, bg=BG_PRIMARY)
    today_content.add(today_right, minsize=350, stretch='always')

    today_eq_canvas = tk.Canvas(today_right, bg=BG_PRIMARY, highlightthickness=0)
    today_eq_canvas.pack(fill=tk.BOTH, expand=True)

    today_dur_canvas = tk.Canvas(today_right, bg=BG_PRIMARY, highlightthickness=0)
    today_dur_canvas.pack(fill=tk.BOTH, expand=True)

    today_widgets = {
        'stat_cards': today_stat_cards,
        'trade_tree': today_trade_tree,
        'eq_canvas': today_eq_canvas,
        'dur_canvas': today_dur_canvas,
    }

    # ===================== PERFORMANCE TAB =====================
    perf_tab = tk.Frame(notebook, bg=BG_PRIMARY)
    notebook.add(perf_tab, text='  Performance  ')

    # --- Account filter bar ---
    filter_frame = tk.Frame(perf_tab, bg=BG_PRIMARY, padx=10, pady=6)
    filter_frame.pack(fill=tk.X)

    tk.Label(filter_frame, text='Account:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(0, 6))

    acct_selector = MultiAccountSelector(filter_frame, store)
    acct_selector.pack(side=tk.LEFT)

    # Date range dropdown
    tk.Label(filter_frame, text='Range:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))

    date_range_options = ['All Time', 'Today', 'This Week', 'Last 7 Days',
                          'This Month', 'Last 30 Days', 'Custom']
    date_var = tk.StringVar(value='All Time')
    date_combo = ttk.Combobox(filter_frame, textvariable=date_var,
                              state='readonly', style='Dark.TCombobox', width=14)
    date_combo['values'] = date_range_options
    date_combo.pack(side=tk.LEFT)

    # Custom date range entries (hidden by default)
    custom_frame = tk.Frame(filter_frame, bg=BG_PRIMARY)

    tk.Label(custom_frame, text='From:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(0, 4))
    custom_start_var = tk.StringVar(value=datetime.now().strftime('%Y-%m-%d'))
    custom_start = tk.Entry(custom_frame, textvariable=custom_start_var,
                            bg=BG_HEADER, fg=FG_TEXT, insertbackground=FG_TEXT,
                            font=(FONT_FAMILY, 9), width=10, relief='flat',
                            highlightthickness=1, highlightbackground='#30363d')
    custom_start.pack(side=tk.LEFT)

    tk.Label(custom_frame, text='To:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(8, 4))
    custom_end_var = tk.StringVar(value=datetime.now().strftime('%Y-%m-%d'))
    custom_end = tk.Entry(custom_frame, textvariable=custom_end_var,
                          bg=BG_HEADER, fg=FG_TEXT, insertbackground=FG_TEXT,
                          font=(FONT_FAMILY, 9), width=10, relief='flat',
                          highlightthickness=1, highlightbackground='#30363d')
    custom_end.pack(side=tk.LEFT)

    def apply_custom_range(event=None):
        store['perf_custom_start'] = custom_start_var.get()
        store['perf_custom_end'] = custom_end_var.get()
        store['perf_dirty'] = True

    custom_apply_btn = tk.Button(custom_frame, text='Apply', bg=BG_HEADER, fg=FG_ACCENT,
                                 font=(FONT_FAMILY, 9), relief='flat', padx=8, pady=1,
                                 activebackground='#30363d', activeforeground=FG_ACCENT,
                                 command=apply_custom_range)
    custom_apply_btn.pack(side=tk.LEFT, padx=(8, 0))
    custom_start.bind('<Return>', apply_custom_range)
    custom_end.bind('<Return>', apply_custom_range)

    def on_date_range_change(event=None):
        store['perf_date_range'] = date_var.get()
        if date_var.get() == 'Custom':
            custom_frame.pack(side=tk.LEFT, padx=(8, 0))
            apply_custom_range()
        else:
            custom_frame.pack_forget()
            store['perf_dirty'] = True
    date_combo.bind('<<ComboboxSelected>>', on_date_range_change)

    # Refresh button — re-requests positions, balances, fills + forces perf redraw
    def on_refresh():
        with DATA_LOCK:
            for name in SC_INSTANCES:
                ws = store['ws_refs'].get(name)
                if ws and store['status'].get(name) == 'Connected':
                    for acct in store['accounts'].get(name, []):
                        try:
                            ws.send(make_dtc_message(
                                DTC_ACCOUNT_BALANCE_REQUEST,
                                TradeAccount=acct,
                                RequestID=hash((name, acct, 'bal', 'refresh')) & 0x7FFFFFFF,
                            ))
                            ws.send(make_dtc_message(
                                DTC_CURRENT_POSITIONS_REQUEST,
                                TradeAccount=acct,
                                RequestID=hash((name, acct, 'pos', 'refresh')) & 0x7FFFFFFF,
                            ))
                            days_back = (datetime.now(timezone.utc) - datetime(2025, 1, 1, tzinfo=timezone.utc)).days + 1
                            ws.send(make_dtc_message(
                                DTC_HISTORICAL_ORDER_FILLS_REQ,
                                TradeAccount=acct,
                                RequestID=hash((name, acct, 'fills', 'refresh')) & 0x7FFFFFFF,
                                NumberOfDays=max(days_back, 1),
                                StartDateTime=FILLS_START_TIMESTAMP,
                            ))
                        except Exception:
                            pass
            store['perf_dirty'] = True

    # Side filter (All / Longs / Shorts)
    tk.Label(filter_frame, text='Side:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))
    side_var = tk.StringVar(value='All')
    side_combo = ttk.Combobox(filter_frame, textvariable=side_var,
                              state='readonly', style='Dark.TCombobox', width=8)
    side_combo['values'] = ['All', 'Longs', 'Shorts']
    side_combo.pack(side=tk.LEFT)

    def on_side_filter_change(event=None):
        store['perf_side_filter'] = side_var.get()
        store['perf_dirty'] = True
    side_combo.bind('<<ComboboxSelected>>', on_side_filter_change)

    # Symbol filter (product groups + individual symbols)
    tk.Label(filter_frame, text='Symbol:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))
    sym_filter_var = tk.StringVar(value='All Symbols')
    sym_filter_combo = ttk.Combobox(filter_frame, textvariable=sym_filter_var,
                                     state='readonly', style='Dark.TCombobox', width=14)
    sym_filter_combo['values'] = ['All Symbols']
    sym_filter_combo.pack(side=tk.LEFT)

    def on_sym_filter_change(event=None):
        store['perf_symbol_filter'] = sym_filter_var.get()
        store['perf_dirty'] = True
    sym_filter_combo.bind('<<ComboboxSelected>>', on_sym_filter_change)

    # Size filter (All / Full Size / Micro)
    tk.Label(filter_frame, text='Size:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))
    size_var = tk.StringVar(value='All')
    size_combo = ttk.Combobox(filter_frame, textvariable=size_var,
                               state='readonly', style='Dark.TCombobox', width=9)
    size_combo['values'] = ['All', 'Full Size', 'Micro']
    size_combo.pack(side=tk.LEFT)

    def on_size_filter_change(event=None):
        store['perf_size_filter'] = size_var.get()
        store['perf_dirty'] = True
    size_combo.bind('<<ComboboxSelected>>', on_size_filter_change)

    refresh_btn = tk.Button(filter_frame, text='\u21BB Refresh', bg=BG_HEADER, fg=FG_TEXT,
                            font=(FONT_FAMILY, 9), relief='flat', padx=8, pady=2,
                            activebackground='#30363d', activeforeground=FG_ACCENT,
                            command=on_refresh)
    refresh_btn.pack(side=tk.LEFT, padx=(16, 0))

    def on_share_viewer():
        from tkinter import filedialog as fd
        import tkinter.messagebox as mb

        # 1. Gather filtered trades (same logic as PDF export)
        acct_filter = store.get('perf_acct_filter', 'All Accounts')
        all_trades = aggregate_all_round_trips(store, acct_filter)
        date_range = store.get('perf_date_range', 'All Time')
        if date_range == 'Custom':
            s, e = store.get('perf_custom_start', ''), store.get('perf_custom_end', '')
            if s:
                all_trades = [t for t in all_trades if t.get('trade_date', '') >= s]
            if e:
                all_trades = [t for t in all_trades if t.get('trade_date', '') <= e]
        else:
            cutoff = _date_range_cutoff(date_range)
            if cutoff:
                all_trades = [t for t in all_trades if t.get('trade_date', '') >= cutoff]
        sf = store.get('perf_side_filter', 'All')
        if sf == 'Longs':
            all_trades = [t for t in all_trades if t.get('side') == 'Long']
        elif sf == 'Shorts':
            all_trades = [t for t in all_trades if t.get('side') == 'Short']

        sz = store.get('perf_size_filter', 'All')
        if sz == 'Full Size':
            all_trades = [t for t in all_trades if get_base_symbol(t.get('symbol', '')) in FULL_SYMBOLS]
        elif sz == 'Micro':
            all_trades = [t for t in all_trades if get_base_symbol(t.get('symbol', '')) in MICRO_SYMBOLS]

        sym_f = store.get('perf_symbol_filter', 'All Symbols')
        if sym_f != 'All Symbols':
            if sym_f == 'Other':
                all_trades = [t for t in all_trades if get_base_symbol(t.get('symbol', '')) not in _ALL_GROUPED_SYMBOLS]
            elif sym_f in SYMBOL_GROUPS:
                allowed = set(SYMBOL_GROUPS[sym_f])
                all_trades = [t for t in all_trades if get_base_symbol(t.get('symbol', '')) in allowed]

        if not all_trades:
            mb.showinfo('Share', 'No trades to share with current filters.')
            return

        # 2. Build JSON payload
        acct_label = ', '.join(sorted(acct_filter)) if isinstance(acct_filter, set) else acct_filter
        payload = {
            'exported': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'account_filter': acct_label,
            'trades': all_trades,
        }
        json_bytes = json.dumps(payload, indent=2).encode('utf-8')

        # 3. Locate viewer exe
        script_dir = os.path.dirname(os.path.abspath(__file__))
        exe_candidates = [
            os.path.join(script_dir, 'dist', 'Performance Viewer.exe'),
            os.path.join(script_dir, 'Performance Viewer.exe'),
        ]
        exe_path = None
        for p in exe_candidates:
            if os.path.isfile(p):
                exe_path = p
                break

        # 4. Ask save location
        save_path = fd.asksaveasfilename(
            title='Save Shared Viewer',
            defaultextension='.zip',
            filetypes=[('ZIP files', '*.zip')],
            initialfile='performance_viewer.zip',
        )
        if not save_path:
            return

        # 5. Create zip
        with zipfile.ZipFile(save_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('trades_data.json', json_bytes)
            if exe_path:
                zf.write(exe_path, 'Performance Viewer.exe')

        if exe_path:
            mb.showinfo('Share', f'Viewer zip saved!\n{save_path}\n\n'
                        f'{len(all_trades)} trades included.')
        else:
            mb.showwarning('Share — No EXE',
                           f'JSON exported to zip, but viewer.exe not found.\n'
                           f'Run build_viewer.bat first to create the exe.\n\n'
                           f'Saved: {save_path}')

    share_btn = tk.Button(filter_frame, text='\U0001F4E4 Share', bg=BG_HEADER, fg=FG_TEXT,
                          font=(FONT_FAMILY, 9), relief='flat', padx=8, pady=2,
                          activebackground='#30363d', activeforeground=FG_ACCENT,
                          command=on_share_viewer)
    share_btn.pack(side=tk.LEFT, padx=(8, 0))
    _add_tooltip(share_btn,
                 "Export a shareable performance viewer.\n"
                 "Creates a .zip snapshot of your currently\n"
                 "filtered data (account, date range, symbol,\n"
                 "side, size) and a standalone viewer app.\n"
                 "Send it to anyone — they can view your\n"
                 "stats without Sierra Chart or this journal.")

    # --- Stats panel (top) ---
    stats_frame = tk.Frame(perf_tab, bg=BG_HEADER, padx=10, pady=8)
    stats_frame.pack(fill=tk.X, pady=(0, 4))

    stat_cards = {}
    stat_defs = [
        ('Trades', 'total_trades'), ('Win Rate', 'win_rate'), ('Profit Factor', 'profit_factor'),
        ('Net P&L', 'net_pnl'), ('Avg Win', 'avg_win'),
        ('Avg Loss', 'avg_loss'), ('Max DD', 'max_drawdown'), ('Max Win', 'max_win'),
        ('Max Loss', 'max_loss'), ('Sharpe', 'sharpe'),
        ('Sortino', 'sortino'), ('Kelly %', 'kelly'), ('L/S Split', 'ls_split'),
        ('Avg Duration', 'avg_duration'), ('Avg Win Dur', 'avg_win_duration'),
        ('Avg Loss Dur', 'avg_loss_duration'),
        ('Win Streak', 'max_win_streak'), ('Loss Streak', 'max_loss_streak'),
    ]
    for i, (label, key) in enumerate(stat_defs):
        row, col = divmod(i, 6)
        card = tk.Frame(stats_frame, bg=BG_HEADER, padx=8, pady=2)
        card.grid(row=row, column=col, sticky='ew', padx=4, pady=2)
        stats_frame.columnconfigure(col, weight=1)
        has_tip = key in STAT_TOOLTIPS
        name_text = f"{label} \u24D8" if has_tip else label
        name_lbl = tk.Label(card, text=name_text, bg=BG_HEADER,
                            fg=FG_ACCENT if has_tip else FG_DIM,
                            font=(FONT_FAMILY, 8))
        name_lbl.pack()
        if has_tip:
            _add_tooltip(name_lbl, STAT_TOOLTIPS[key])
        val_lbl = tk.Label(card, text='—', bg=BG_HEADER, fg=FG_TEXT,
                           font=(FONT_FAMILY, 12, 'bold'))
        val_lbl.pack()
        stat_cards[key] = val_lbl

    # --- Content area (bottom) ---
    content = tk.PanedWindow(perf_tab, orient=tk.HORIZONTAL, bg=BG_PRIMARY,
                             sashwidth=4, sashrelief=tk.FLAT)
    content.pack(fill=tk.BOTH, expand=True)

    # Left: Trade log
    log_frame = tk.Frame(content, bg=BG_PRIMARY)
    content.add(log_frame, minsize=400, stretch='always')

    log_cols = ('date', 'time', 'symbol', 'side', 'qty', 'entry', 'exit', 'pnl')
    log_scroll = ttk.Scrollbar(log_frame, orient=tk.VERTICAL)
    trade_tree = ttk.Treeview(log_frame, columns=log_cols, show='headings',
                              style='Dashboard.Treeview',
                              yscrollcommand=log_scroll.set)
    log_scroll.configure(command=trade_tree.yview)

    log_headings = {
        'date': ('Date', 85), 'time': ('Time', 70), 'symbol': ('Symbol', 80),
        'side': ('Side', 50), 'qty': ('Qty', 40), 'entry': ('Entry', 85),
        'exit': ('Exit', 85), 'pnl': ('P&L', 95),
    }
    for col, (label, width) in log_headings.items():
        trade_tree.heading(col, text=label, anchor=tk.W)
        anchor = tk.E if col in ('qty', 'entry', 'exit', 'pnl') else tk.W
        trade_tree.column(col, width=width, minwidth=35, anchor=anchor)

    trade_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    trade_tree.tag_configure('profit', foreground=FG_GREEN)
    trade_tree.tag_configure('loss', foreground=FG_RED)
    trade_tree.tag_configure('flat', foreground=FG_TEXT)
    trade_tree.tag_configure('alt_profit', background=BG_ALT, foreground=FG_GREEN)
    trade_tree.tag_configure('alt_loss', background=BG_ALT, foreground=FG_RED)
    trade_tree.tag_configure('alt_flat', background=BG_ALT, foreground=FG_TEXT)

    # Right: Sub-notebook with Calendar and Equity tabs
    right_frame = tk.Frame(content, bg=BG_PRIMARY)
    content.add(right_frame, minsize=350, stretch='always')

    sub_nb = ttk.Notebook(right_frame, style='Dark.TNotebook')
    sub_nb.pack(fill=tk.BOTH, expand=True)

    cal_tab = tk.Frame(sub_nb, bg=BG_PRIMARY)
    sub_nb.add(cal_tab, text=' Calendar ')

    # Calendar month navigation bar
    cal_nav = tk.Frame(cal_tab, bg=BG_HEADER, padx=6, pady=4)
    cal_nav.pack(fill=tk.X)

    cal_prev_btn = tk.Button(cal_nav, text='\u25C0', bg=BG_HEADER, fg=FG_TEXT,
                             font=(FONT_FAMILY, 10), relief='flat', padx=6,
                             activebackground='#30363d', activeforeground=FG_ACCENT)
    cal_prev_btn.pack(side=tk.LEFT)

    cal_month_var = tk.StringVar(value='')
    cal_month_combo = ttk.Combobox(cal_nav, textvariable=cal_month_var,
                                   state='readonly', style='Dark.TCombobox', width=16)
    cal_month_combo.pack(side=tk.LEFT, padx=8)

    cal_next_btn = tk.Button(cal_nav, text='\u25B6', bg=BG_HEADER, fg=FG_TEXT,
                             font=(FONT_FAMILY, 10), relief='flat', padx=6,
                             activebackground='#30363d', activeforeground=FG_ACCENT)
    cal_next_btn.pack(side=tk.LEFT)

    # Year View pop-out button
    cal_popout_btn = tk.Button(cal_nav, text='\u2197 Year View', bg=BG_HEADER, fg=FG_TEXT,
                               font=(FONT_FAMILY, 9), relief='flat', padx=8, pady=2,
                               activebackground='#30363d', activeforeground=FG_ACCENT)
    cal_popout_btn.pack(side=tk.RIGHT, padx=(0, 4))

    # Monthly P&L total label (right side of nav bar)
    cal_total_lbl = tk.Label(cal_nav, text='', bg=BG_HEADER, fg=FG_TEXT,
                             font=(FONT_FAMILY, 10, 'bold'))
    cal_total_lbl.pack(side=tk.RIGHT, padx=8)

    cal_canvas = tk.Canvas(cal_tab, bg=BG_PRIMARY, highlightthickness=0)
    cal_canvas.pack(fill=tk.BOTH, expand=True)

    eq_tab = tk.Frame(sub_nb, bg=BG_PRIMARY)
    sub_nb.add(eq_tab, text=' Equity ')
    eq_canvas = tk.Canvas(eq_tab, bg=BG_PRIMARY, highlightthickness=0)
    eq_canvas.pack(fill=tk.BOTH, expand=True)

    bar_tab = tk.Frame(sub_nb, bg=BG_PRIMARY)
    sub_nb.add(bar_tab, text=' Daily P&L ')
    bar_canvas = tk.Canvas(bar_tab, bg=BG_PRIMARY, highlightthickness=0)
    bar_canvas.pack(fill=tk.BOTH, expand=True)

    hour_tab = tk.Frame(sub_nb, bg=BG_PRIMARY)
    sub_nb.add(hour_tab, text=' By Hour ')
    hour_canvas = tk.Canvas(hour_tab, bg=BG_PRIMARY, highlightthickness=0)
    hour_canvas.pack(fill=tk.BOTH, expand=True)

    dur_tab = tk.Frame(sub_nb, bg=BG_PRIMARY)
    sub_nb.add(dur_tab, text=' Duration ')
    perf_dur_canvas = tk.Canvas(dur_tab, bg=BG_PRIMARY, highlightthickness=0)
    perf_dur_canvas.pack(fill=tk.BOTH, expand=True)

    # ===================== BREAKDOWN TAB =====================
    brk_tab = tk.Frame(notebook, bg=BG_PRIMARY)
    notebook.add(brk_tab, text='  Breakdown  ')

    # Account filter bar (same behavior as Performance tab)
    brk_filter = tk.Frame(brk_tab, bg=BG_PRIMARY, padx=10, pady=6)
    brk_filter.pack(fill=tk.X)
    tk.Label(brk_filter, text='Account:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(0, 6))
    brk_acct_selector = MultiAccountSelector(brk_filter, store)
    brk_acct_selector.pack(side=tk.LEFT)

    # Breakdown date range dropdown (synced with perf tab)
    tk.Label(brk_filter, text='Range:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))
    brk_date_var = tk.StringVar(value='All Time')
    brk_date_combo = ttk.Combobox(brk_filter, textvariable=brk_date_var,
                                  state='readonly', style='Dark.TCombobox', width=14)
    brk_date_combo['values'] = date_range_options
    brk_date_combo.pack(side=tk.LEFT)

    def on_brk_date_change(event=None):
        store['perf_date_range'] = brk_date_var.get()
        date_var.set(brk_date_var.get())
        store['perf_dirty'] = True
    brk_date_combo.bind('<<ComboboxSelected>>', on_brk_date_change)

    # Breakdown Symbol filter (synced with perf tab)
    tk.Label(brk_filter, text='Symbol:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))
    brk_sym_var = tk.StringVar(value='All Symbols')
    brk_sym_combo = ttk.Combobox(brk_filter, textvariable=brk_sym_var,
                                  state='readonly', style='Dark.TCombobox', width=14)
    brk_sym_combo['values'] = ['All Symbols']
    brk_sym_combo.pack(side=tk.LEFT)

    def on_brk_sym_change(event=None):
        store['perf_symbol_filter'] = brk_sym_var.get()
        sym_filter_var.set(brk_sym_var.get())
        store['perf_dirty'] = True
    brk_sym_combo.bind('<<ComboboxSelected>>', on_brk_sym_change)

    # Breakdown Size filter (synced with perf tab)
    tk.Label(brk_filter, text='Size:', bg=BG_PRIMARY, fg=FG_DIM,
             font=(FONT_FAMILY, 9)).pack(side=tk.LEFT, padx=(16, 6))
    brk_size_var = tk.StringVar(value='All')
    brk_size_combo = ttk.Combobox(brk_filter, textvariable=brk_size_var,
                                   state='readonly', style='Dark.TCombobox', width=9)
    brk_size_combo['values'] = ['All', 'Full Size', 'Micro']
    brk_size_combo.pack(side=tk.LEFT)

    def on_brk_size_change(event=None):
        store['perf_size_filter'] = brk_size_var.get()
        size_var.set(brk_size_var.get())
        store['perf_dirty'] = True
    brk_size_combo.bind('<<ComboboxSelected>>', on_brk_size_change)

    brk_refresh_btn = tk.Button(brk_filter, text='\u21BB Refresh', bg=BG_HEADER, fg=FG_TEXT,
                                font=(FONT_FAMILY, 9), relief='flat', padx=8, pady=2,
                                activebackground='#30363d', activeforeground=FG_ACCENT,
                                command=on_refresh)
    brk_refresh_btn.pack(side=tk.LEFT, padx=(16, 0))

    # Sync account selectors between Performance and Breakdown tabs
    def _perf_acct_changed(filt):
        brk_acct_selector.set_filter(filt)
    def _brk_acct_changed(filt):
        acct_selector.set_filter(filt)
    acct_selector._on_change = _perf_acct_changed
    brk_acct_selector._on_change = _brk_acct_changed

    # Sync date range changes from perf tab to breakdown tab
    _orig_date_cb = on_date_range_change
    def on_date_range_synced(event=None):
        _orig_date_cb(event)
        brk_date_var.set(date_var.get())
    date_combo.bind('<<ComboboxSelected>>', on_date_range_synced)

    # Sync symbol filter from perf tab to breakdown tab
    _orig_sym_cb = on_sym_filter_change
    def on_sym_filter_synced(event=None):
        _orig_sym_cb(event)
        brk_sym_var.set(sym_filter_var.get())
    sym_filter_combo.bind('<<ComboboxSelected>>', on_sym_filter_synced)

    # Sync size filter from perf tab to breakdown tab
    _orig_size_cb = on_size_filter_change
    def on_size_filter_synced(event=None):
        _orig_size_cb(event)
        brk_size_var.set(size_var.get())
    size_combo.bind('<<ComboboxSelected>>', on_size_filter_synced)

    # Scrollable container for all breakdown content
    brk_outer = tk.Frame(brk_tab, bg=BG_PRIMARY)
    brk_outer.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

    brk_scroll_canvas = tk.Canvas(brk_outer, bg=BG_PRIMARY, highlightthickness=0)
    brk_scrollbar = ttk.Scrollbar(brk_outer, orient=tk.VERTICAL, command=brk_scroll_canvas.yview)
    brk_content = tk.Frame(brk_scroll_canvas, bg=BG_PRIMARY)

    brk_content.bind('<Configure>',
                     lambda e: brk_scroll_canvas.configure(scrollregion=brk_scroll_canvas.bbox('all')))
    brk_win_id = brk_scroll_canvas.create_window((0, 0), window=brk_content, anchor='nw')
    brk_scroll_canvas.configure(yscrollcommand=brk_scrollbar.set)

    brk_scroll_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    brk_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    # Mousewheel scrolling — bind/unbind when Breakdown tab is active
    def _on_brk_mousewheel(event):
        brk_scroll_canvas.yview_scroll(int(-1 * (event.delta / 120)), 'units')

    def _on_tab_changed(event):
        sel = notebook.index(notebook.select())
        if sel == 2:  # Breakdown tab index
            brk_scroll_canvas.bind_all('<MouseWheel>', _on_brk_mousewheel)
        else:
            brk_scroll_canvas.unbind_all('<MouseWheel>')
    notebook.bind('<<NotebookTabChanged>>', _on_tab_changed)

    # Bind the content frame width to the canvas width
    def _on_brk_canvas_configure(event):
        brk_scroll_canvas.itemconfigure(brk_win_id, width=event.width)
    brk_scroll_canvas.bind('<Configure>', _on_brk_canvas_configure)

    brk_cols = ('label', 'wins', 'win_pnl', 'avg_win',
                'losses', 'loss_pnl', 'avg_loss', 'net_pnl', 'win_pct', 'ratio')
    brk_headings = {
        'label': ('', 110), 'wins': ('Wins', 55), 'win_pnl': ('Win P&L', 95),
        'avg_win': ('Avg Win', 85), 'losses': ('Losses', 55),
        'loss_pnl': ('Loss P&L', 95), 'avg_loss': ('Avg Loss', 85),
        'net_pnl': ('Net P&L', 100), 'win_pct': ('Win %', 60), 'ratio': ('Ratio', 60),
    }

    def make_breakdown_tree(parent, title):
        """Create a labeled Treeview for breakdown data."""
        wrapper = tk.Frame(parent, bg=BG_PRIMARY)
        tk.Label(wrapper, text=title, bg=BG_PRIMARY, fg=FG_ACCENT,
                 font=(FONT_FAMILY, 11, 'bold'), anchor=tk.W).pack(fill=tk.X, padx=6, pady=(8, 2))
        tf = tk.Frame(wrapper, bg=BG_PRIMARY)
        tf.pack(fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(tf, orient=tk.VERTICAL)
        tv = ttk.Treeview(tf, columns=brk_cols, show='headings',
                          style='Dashboard.Treeview', height=7,
                          yscrollcommand=sb.set)
        sb.configure(command=tv.yview)
        for col, (lbl, w) in brk_headings.items():
            tv.heading(col, text=lbl, anchor=tk.W if col == 'label' else tk.CENTER)
            anch = tk.W if col == 'label' else tk.E
            tv.column(col, width=w, minwidth=40, anchor=anch)
        tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        tv.tag_configure('profit', foreground=FG_GREEN)
        tv.tag_configure('loss', foreground=FG_RED)
        tv.tag_configure('flat', foreground=FG_TEXT)
        tv.tag_configure('alt_profit', background=BG_ALT, foreground=FG_GREEN)
        tv.tag_configure('alt_loss', background=BG_ALT, foreground=FG_RED)
        tv.tag_configure('alt_flat', background=BG_ALT, foreground=FG_TEXT)
        return wrapper, tv

    session_frame, session_tree = make_breakdown_tree(brk_content, 'Session Breakdown')
    session_frame.pack(fill=tk.X, padx=2)

    dow_frame, dow_tree = make_breakdown_tree(brk_content, 'Day of Week Breakdown')
    dow_frame.pack(fill=tk.X, padx=2)

    sym_frame, sym_tree = make_breakdown_tree(brk_content, 'Symbol Breakdown')
    sym_frame.pack(fill=tk.X, padx=2)

    # Day × Hour P&L Heatmap canvas
    hm_wrapper = tk.Frame(brk_content, bg=BG_PRIMARY)
    hm_wrapper.pack(fill=tk.X, padx=2)
    tk.Label(hm_wrapper, text='Day × Hour Heatmap (CT)', bg=BG_PRIMARY, fg=FG_ACCENT,
             font=(FONT_FAMILY, 11, 'bold'), anchor=tk.W).pack(fill=tk.X, padx=6, pady=(8, 2))
    hm_canvas = tk.Canvas(hm_wrapper, bg=BG_PRIMARY, highlightthickness=0, height=220)
    hm_canvas.pack(fill=tk.X, padx=6, pady=(0, 4))

    monthly_frame, monthly_tree = make_breakdown_tree(brk_content, 'Monthly P&L')
    monthly_frame.pack(fill=tk.X, padx=2)

    perf_widgets = {
        'stat_cards': stat_cards,
        'trade_tree': trade_tree,
        'cal_canvas': cal_canvas,
        'cal_month_var': cal_month_var,
        'cal_month_combo': cal_month_combo,
        'cal_prev_btn': cal_prev_btn,
        'cal_next_btn': cal_next_btn,
        'cal_total_lbl': cal_total_lbl,
        'cal_popout_btn': cal_popout_btn,
        'cal_daily_pnl': {},  # mutable ref to latest daily_pnl, updated on refresh
        'cal_months_list': [],  # mutable list of 'YYYY-MM' strings, updated on refresh
        'eq_canvas': eq_canvas,
        'bar_canvas': bar_canvas,
        'hour_canvas': hour_canvas,
        'dur_canvas': perf_dur_canvas,
        'acct_selector': acct_selector,
        'brk_acct_selector': brk_acct_selector,
        'session_tree': session_tree,
        'dow_tree': dow_tree,
        'sym_tree': sym_tree,
        'monthly_tree': monthly_tree,
        'hm_canvas': hm_canvas,
        'sym_filter_combo': sym_filter_combo,
        'brk_sym_combo': brk_sym_combo,
    }

    # Calendar month navigation callbacks
    def _cal_navigate(delta):
        months_list = perf_widgets['cal_months_list']
        if not months_list:
            return
        current = cal_month_var.get()
        try:
            idx = months_list.index(current)
        except ValueError:
            idx = len(months_list) - 1
        new_idx = max(0, min(len(months_list) - 1, idx + delta))
        cal_month_var.set(months_list[new_idx])
        store['perf_dirty'] = True

    cal_prev_btn.configure(command=lambda: _cal_navigate(-1))
    cal_next_btn.configure(command=lambda: _cal_navigate(1))
    cal_month_combo.bind('<<ComboboxSelected>>', lambda e: store.update(perf_dirty=True))

    def _open_year_view():
        daily_pnl = perf_widgets.get('cal_daily_pnl', {})
        if not daily_pnl:
            return
        open_year_view_window(root, daily_pnl)

    cal_popout_btn.configure(command=_open_year_view)

    # Feature 4: Canvas redraw on resize — set perf_dirty so the 2s timer redraws
    def _on_canvas_resize(event):
        store['perf_dirty'] = True
    for c in (cal_canvas, eq_canvas, bar_canvas, hour_canvas, perf_dur_canvas, hm_canvas,
              today_eq_canvas, today_dur_canvas):
        c.bind('<Configure>', _on_canvas_resize)

    return root, tree, status_labels, balance_labels, totals_label, perf_widgets, today_widgets


def format_currency(value):
    """Format a float as currency string with sign."""
    sign = '+' if value > 0 else ''
    return f"{sign}${value:,.2f}"


def refresh_gui(root, tree, _status_labels, _balance_labels, totals_label, store):
    """Periodic GUI refresh — reads DATA_STORE under lock."""
    # Read live label refs from store['gui'] (updated by rebuild_status_bar)
    gui = store.get('gui', {})
    cur_status_labels = gui.get('status_labels', _status_labels)
    cur_balance_labels = gui.get('balance_labels', _balance_labels)

    with DATA_LOCK:
        # Update status labels
        disabled = set(store.get('disabled', set()))
        for name, lbl in cur_status_labels.items():
            if name in disabled:
                lbl.configure(text=f"{name}: Disabled", fg=FG_DIM)
            else:
                status = store['status'].get(name, 'Disconnected')
                if status == 'Connected':
                    lbl.configure(text=f"{name}: Connected", fg=FG_GREEN)
                elif status == 'Connecting...':
                    lbl.configure(text=f"{name}: Connecting...", fg='#d29922')
                else:
                    lbl.configure(text=f"{name}: Disconnected", fg=FG_RED)

        # Snapshot positions and balances
        positions = dict(store['positions'])
        fills_data = dict(store['fills'])
        balances = dict(store['balances'])

    # Update per-instance balance labels
    for name, bal_lbl in cur_balance_labels.items():
        instance_bal = sum(
            b.get('cash_balance', 0.0)
            for (inst, acct), b in balances.items()
            if inst == name
        )
        if instance_bal:
            bal_lbl.configure(text=f"${instance_bal:,.2f}", fg=FG_TEXT)
        else:
            bal_lbl.configure(text="", fg=FG_DIM)

    # Clear treeview
    for item in tree.get_children():
        tree.delete(item)

    total_open_pnl = 0.0
    row_idx = 0

    # Sort by instance name, then account, then symbol
    sorted_keys = sorted(positions.keys(), key=lambda k: (k[0], k[1], k[2]))

    for pos_key in sorted_keys:
        pos = positions[pos_key]
        open_pnl = pos.get('open_pnl', 0.0)
        total_open_pnl += open_pnl

        # Compute closed PnL from fills (best-effort)
        fill_key = (pos['instance'], pos['account'], pos['symbol'])
        closed_pnl = compute_closed_pnl_for_key(fill_key, {'fills': fills_data})

        # Determine row tag
        is_alt = row_idx % 2 == 1
        if open_pnl > 0:
            tag = 'alt_profit' if is_alt else 'profit'
        elif open_pnl < 0:
            tag = 'alt_loss' if is_alt else 'loss'
        else:
            tag = 'alt_flat' if is_alt else 'flat'

        tree.insert('', tk.END, values=(
            pos['instance'],
            pos['account'],
            pos['symbol'],
            int(pos['quantity']),
            f"{pos['avg_price']:,.2f}",
            format_currency(open_pnl),
            format_currency(closed_pnl) if closed_pnl != 0 else '—',
        ), tags=(tag,))
        row_idx += 1

    # Update totals
    count = len(positions)
    total_balance = sum(b.get('cash_balance', 0.0) for b in balances.values())
    pnl_color = FG_GREEN if total_open_pnl > 0 else FG_RED if total_open_pnl < 0 else FG_TEXT
    bal_text = f"  |  Total Balance: ${total_balance:,.2f}" if total_balance else ""
    unsupported = _warned_symbols.copy()
    warn_text = f"  |  Unsupported symbol{'s' if len(unsupported) > 1 else ''}: {', '.join(sorted(unsupported))} — visit amtbalance.com for updates" if unsupported else ""
    totals_label.configure(
        text=f"Open PnL: {format_currency(total_open_pnl)}  |  Positions: {count}{bal_text}{warn_text}",
        fg=pnl_color,
    )

    # Schedule next refresh — faster when positions are open
    delay = GUI_REFRESH_FAST_MS if count > 0 else GUI_REFRESH_MS
    root.after(delay, refresh_gui, root, tree, _status_labels, _balance_labels, totals_label, store)


# ─────────────────────────────────────────────────────────────────────────────
# PERFORMANCE TAB REFRESH
# ─────────────────────────────────────────────────────────────────────────────


def update_stats_panel(stats, stat_cards):
    """Update the stat card labels with computed stats."""
    def fmt_currency(v):
        sign = '+' if v > 0 else ''
        return f"{sign}${v:,.0f}"

    def color_for(v):
        if v > 0:
            return FG_GREEN
        elif v < 0:
            return FG_RED
        return FG_TEXT

    def fmt_duration(secs):
        if secs <= 0:
            return '—'
        if secs < 60:
            return f"{secs:.0f}s"
        if secs < 3600:
            return f"{secs / 60:.1f}m"
        return f"{secs / 3600:.1f}h"

    mappings = {
        'total_trades': (str(stats['total_trades']), FG_TEXT),
        'win_rate': (f"{stats['win_rate']:.1f}%",
                     FG_GREEN if stats['win_rate'] >= 50 else FG_RED),
        'profit_factor': (f"{stats['profit_factor']:.2f}" if stats['profit_factor'] != float('inf') else 'INF',
                          FG_GREEN if stats['profit_factor'] >= 1 else FG_RED),
        'net_pnl': (fmt_currency(stats['net_pnl']), color_for(stats['net_pnl'])),
        'avg_win': (fmt_currency(stats['avg_win']), FG_GREEN),
        'avg_loss': (fmt_currency(stats['avg_loss']), FG_RED),
        'max_drawdown': (fmt_currency(-stats['max_drawdown']), FG_RED if stats['max_drawdown'] > 0 else FG_TEXT),
        'max_win': (fmt_currency(stats['max_win']), FG_GREEN),
        'max_loss': (fmt_currency(stats['max_loss']), FG_RED),
        'sharpe': (f"{stats['sharpe']:.2f}", color_for(stats['sharpe'])),
        'sortino': (f"{stats['sortino']:.2f}", color_for(stats['sortino'])),
        'kelly': (f"{stats.get('kelly', 0):.1f}%", color_for(stats.get('kelly', 0))),
        'ls_split': (f"L:{stats['long_count']} S:{stats['short_count']}", FG_TEXT),
        'avg_duration': (fmt_duration(stats.get('avg_duration', 0)), FG_TEXT),
        'avg_win_duration': (fmt_duration(stats.get('avg_win_duration', 0)), FG_GREEN),
        'avg_loss_duration': (fmt_duration(stats.get('avg_loss_duration', 0)), FG_RED),
        'max_win_streak': (str(stats.get('max_win_streak', 0)), FG_GREEN),
        'max_loss_streak': (str(stats.get('max_loss_streak', 0)), FG_RED),
    }

    for key, (text, fg) in mappings.items():
        if key in stat_cards:
            stat_cards[key].configure(text=text, fg=fg)


def update_trade_log(trades, trade_tree):
    """Populate trade log treeview with round-trip trades."""
    for item in trade_tree.get_children():
        trade_tree.delete(item)

    for i, t in enumerate(trades):
        date_part, time_part = _format_local_time(t.get('exit_time', ''))
        pnl = t['pnl_dollars']

        is_alt = i % 2 == 1
        if pnl > 0:
            tag = 'alt_profit' if is_alt else 'profit'
        elif pnl < 0:
            tag = 'alt_loss' if is_alt else 'loss'
        else:
            tag = 'alt_flat' if is_alt else 'flat'

        sign = '+' if pnl > 0 else ''
        trade_tree.insert('', tk.END, values=(
            date_part, time_part, t['symbol'], t['side'],
            t['qty'], f"{t['entry_price']:,.2f}", f"{t['exit_price']:,.2f}",
            f"{sign}${pnl:,.2f}",
        ), tags=(tag,))


def draw_calendar(daily_pnl, canvas, perf_widgets):
    """Draw a single-month calendar heatmap with navigation via perf_widgets."""
    canvas.delete('all')
    canvas.update_idletasks()
    cw = canvas.winfo_width()
    ch = canvas.winfo_height()
    if cw < 50 or ch < 50:
        return

    if not daily_pnl:
        canvas.create_text(cw // 2, ch // 2, text='No trades yet',
                           fill=FG_DIM, font=(FONT_FAMILY, 12))
        perf_widgets['cal_month_combo']['values'] = []
        perf_widgets['cal_total_lbl'].configure(text='')
        return

    # Determine date range and build months list
    dates = sorted(daily_pnl.keys())
    try:
        first = datetime.strptime(dates[0], '%Y-%m-%d')
        last = datetime.strptime(dates[-1], '%Y-%m-%d')
    except (ValueError, IndexError):
        return

    months = []
    y, m = first.year, first.month
    while (y, m) <= (last.year, last.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    # Build dropdown labels (e.g. 'Mar 2026')
    month_labels = [f"{cal_mod.month_abbr[mo]} {yr}" for yr, mo in months]
    month_keys = [f"{yr}-{mo:02d}" for yr, mo in months]

    # Update dropdown options
    perf_widgets['cal_months_list'][:] = month_labels
    combo = perf_widgets['cal_month_combo']
    combo['values'] = month_labels

    # Determine which month to show — default to latest
    current_sel = perf_widgets['cal_month_var'].get()
    if current_sel not in month_labels:
        perf_widgets['cal_month_var'].set(month_labels[-1])
        current_sel = month_labels[-1]

    sel_idx = month_labels.index(current_sel)
    year, month = months[sel_idx]
    month_key = month_keys[sel_idx]

    # Update monthly total label
    month_total = sum(v for d, v in daily_pnl.items() if d.startswith(month_key))
    sign = '+' if month_total > 0 else ''
    total_color = FG_GREEN if month_total > 0 else FG_RED if month_total < 0 else FG_TEXT
    perf_widgets['cal_total_lbl'].configure(text=f"{sign}${month_total:,.0f}", fg=total_color)

    # Color scale based on THIS month's max for better contrast
    month_vals = [v for d, v in daily_pnl.items() if d.startswith(month_key)]
    max_abs = max(abs(v) for v in month_vals) if month_vals else 1
    if max_abs == 0:
        max_abs = 1

    # Layout — single month, fill available space
    pad = 12
    dow_header_h = 24
    avail_w = cw - 2 * pad
    avail_h = ch - 2 * pad - dow_header_h
    cell_size = max(20, min(avail_w // 7, avail_h // 6))
    grid_w = 7 * cell_size
    x_offset = max(pad, (cw - grid_w) // 2)
    y_offset = pad

    # Day-of-week headers
    dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for d, name in enumerate(dow_names):
        cx = x_offset + d * cell_size + cell_size // 2
        canvas.create_text(cx, y_offset + dow_header_h // 2, text=name, fill=FG_DIM,
                           font=(FONT_FAMILY, 9, 'bold'))

    y_start = y_offset + dow_header_h

    # Draw day cells
    cal_days = cal_mod.monthcalendar(year, month)
    for week_idx, week in enumerate(cal_days):
        for dow, day in enumerate(week):
            if day == 0:
                continue
            cx = x_offset + dow * cell_size
            cy = y_start + week_idx * cell_size

            date_str = f"{year}-{month:02d}-{day:02d}"
            pnl_val = daily_pnl.get(date_str, None)

            if pnl_val is not None:
                intensity = min(abs(pnl_val) / max_abs, 1.0)
                if pnl_val > 0:
                    r = int(13 + intensity * (63 - 13))
                    g = int(17 + intensity * (185 - 17))
                    b = int(23 + intensity * (80 - 23))
                elif pnl_val < 0:
                    r = int(17 + intensity * (249 - 17))
                    g = int(22 + intensity * (117 - 22))
                    b = int(34 + intensity * (131 - 34))
                else:
                    r, g, b = 33, 38, 45
                fill_color = f'#{r:02x}{g:02x}{b:02x}'
            else:
                fill_color = '#161b22'

            canvas.create_rectangle(cx, cy, cx + cell_size - 1, cy + cell_size - 1,
                                    fill=fill_color, outline=BG_PRIMARY, width=1)

            text_color = FG_TEXT if pnl_val is not None else '#30363d'
            day_font_size = max(8, cell_size // 3)
            if pnl_val is not None and cell_size >= 28:
                canvas.create_text(cx + cell_size // 2, cy + cell_size // 3,
                                   text=str(day), fill=text_color,
                                   font=(FONT_FAMILY, day_font_size))
                pnl_text = f"${abs(pnl_val):,.0f}" if abs(pnl_val) >= 1 else '$0'
                canvas.create_text(cx + cell_size // 2, cy + cell_size * 2 // 3,
                                   text=pnl_text, fill=text_color,
                                   font=(FONT_FAMILY, max(7, cell_size // 4)))
            else:
                canvas.create_text(cx + cell_size // 2, cy + cell_size // 2,
                                   text=str(day), fill=text_color,
                                   font=(FONT_FAMILY, day_font_size))


def open_year_view_window(parent, daily_pnl):
    """Open a pop-out window showing all months in a scrollable year grid."""
    if not daily_pnl:
        return

    dates = sorted(daily_pnl.keys())
    try:
        first = datetime.strptime(dates[0], '%Y-%m-%d')
        last = datetime.strptime(dates[-1], '%Y-%m-%d')
    except (ValueError, IndexError):
        return

    all_months = []
    y, m = first.year, first.month
    while (y, m) <= (last.year, last.month):
        all_months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    # Build year list from data
    all_years = sorted(set(yr for yr, mo in all_months))
    year_options = ['All Years'] + [str(yr) for yr in all_years]

    # Window setup
    win = tk.Toplevel(parent)
    win.title("Calendar — Year View")
    win.configure(bg=BG_PRIMARY)
    win.geometry("1100x750")
    win.minsize(700, 400)

    # Year picker bar
    picker_frame = tk.Frame(win, bg=BG_HEADER, padx=10, pady=6)
    picker_frame.pack(fill=tk.X)

    tk.Label(picker_frame, text='Year:', bg=BG_HEADER, fg=FG_DIM,
             font=(FONT_FAMILY, 10)).pack(side=tk.LEFT, padx=(0, 6))

    year_var = tk.StringVar(value='All Years')
    year_combo = ttk.Combobox(picker_frame, textvariable=year_var,
                              state='readonly', style='Dark.TCombobox', width=12)
    year_combo['values'] = year_options
    year_combo.pack(side=tk.LEFT)

    # Year total label (right side of picker bar)
    year_total_lbl = tk.Label(picker_frame, text='', bg=BG_HEADER, fg=FG_TEXT,
                              font=(FONT_FAMILY, 11, 'bold'))
    year_total_lbl.pack(side=tk.RIGHT, padx=8)

    # Scrollable canvas
    outer = tk.Frame(win, bg=BG_PRIMARY)
    outer.pack(fill=tk.BOTH, expand=True)

    xscroll = ttk.Scrollbar(outer, orient=tk.HORIZONTAL)
    yscroll = ttk.Scrollbar(outer, orient=tk.VERTICAL)
    canvas = tk.Canvas(outer, bg=BG_PRIMARY, highlightthickness=0,
                       xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)
    xscroll.configure(command=canvas.xview)
    yscroll.configure(command=canvas.yview)

    canvas.grid(row=0, column=0, sticky='nsew')
    yscroll.grid(row=0, column=1, sticky='ns')
    xscroll.grid(row=1, column=0, sticky='ew')
    outer.rowconfigure(0, weight=1)
    outer.columnconfigure(0, weight=1)

    def _on_mousewheel(event):
        canvas.yview_scroll(int(-1 * (event.delta / 120)), 'units')
    canvas.bind_all('<MouseWheel>', _on_mousewheel)
    win.bind('<Destroy>', lambda e: canvas.unbind_all('<MouseWheel>') if e.widget == win else None)

    def _draw_year_grid(event=None):
        canvas.delete('all')
        canvas.update_idletasks()
        cw = canvas.winfo_width()
        if cw < 100:
            cw = 1100

        # Filter months by selected year
        sel = year_var.get()
        if sel == 'All Years':
            months = all_months
        else:
            filter_year = int(sel)
            months = [(yr, mo) for yr, mo in all_months if yr == filter_year]

        if not months:
            canvas.create_text(cw // 2, 50, text='No data for selected year',
                               fill=FG_DIM, font=(FONT_FAMILY, 12))
            year_total_lbl.configure(text='')
            return

        # Color scale based on filtered data for better contrast
        filtered_pnl = {}
        for d, v in daily_pnl.items():
            if sel == 'All Years' or d.startswith(sel):
                filtered_pnl[d] = v

        max_abs = max(abs(v) for v in filtered_pnl.values()) if filtered_pnl else 1
        if max_abs == 0:
            max_abs = 1

        # Update year total label
        grand_total = sum(filtered_pnl.values())
        sign = '+' if grand_total > 0 else ''
        gt_color = FG_GREEN if grand_total > 0 else FG_RED if grand_total < 0 else FG_TEXT
        year_total_lbl.configure(text=f"Total: {sign}${grand_total:,.0f}", fg=gt_color)

        cell_size = 38
        month_spacing = 16
        header_h = 28
        dow_header_h = 20
        month_block_h = header_h + dow_header_h + 6 * cell_size + month_spacing
        month_block_w = 7 * cell_size + month_spacing

        cols_per_row = max(1, (cw - 20) // month_block_w)
        x_start = max(10, (cw - cols_per_row * month_block_w) // 2)
        y_start = 10

        dow_names = ['M', 'T', 'W', 'T', 'F', 'S', 'S']

        for idx, (year, month) in enumerate(months):
            col = idx % cols_per_row
            row = idx // cols_per_row
            mx = x_start + col * month_block_w
            my = y_start + row * month_block_h

            # Month header with total
            month_name = cal_mod.month_abbr[month]
            month_key = f"{year}-{month:02d}"
            month_total = sum(v for d, v in daily_pnl.items() if d.startswith(month_key))
            sign = '+' if month_total > 0 else ''
            total_color = FG_GREEN if month_total > 0 else FG_RED if month_total < 0 else FG_DIM
            canvas.create_text(mx + 7 * cell_size // 2, my + header_h // 2,
                               text=f"{month_name} {year}", fill=FG_ACCENT,
                               font=(FONT_FAMILY, 11, 'bold'))
            canvas.create_text(mx + 7 * cell_size - 4, my + header_h // 2,
                               text=f"{sign}${month_total:,.0f}", fill=total_color,
                               font=(FONT_FAMILY, 8), anchor=tk.E)

            # Day-of-week headers
            for d, name in enumerate(dow_names):
                cx = mx + d * cell_size + cell_size // 2
                canvas.create_text(cx, my + header_h + dow_header_h // 2, text=name,
                                   fill=FG_DIM, font=(FONT_FAMILY, 8, 'bold'))

            # Day cells
            cal_days = cal_mod.monthcalendar(year, month)
            cell_y_start = my + header_h + dow_header_h
            for week_idx, week in enumerate(cal_days):
                for dow, day in enumerate(week):
                    if day == 0:
                        continue
                    cx = mx + dow * cell_size
                    cy = cell_y_start + week_idx * cell_size
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    pnl_val = daily_pnl.get(date_str, None)

                    if pnl_val is not None:
                        intensity = min(abs(pnl_val) / max_abs, 1.0)
                        if pnl_val > 0:
                            r = int(13 + intensity * (63 - 13))
                            g = int(17 + intensity * (185 - 17))
                            b = int(23 + intensity * (80 - 23))
                        elif pnl_val < 0:
                            r = int(17 + intensity * (249 - 17))
                            g = int(22 + intensity * (117 - 22))
                            b = int(34 + intensity * (131 - 34))
                        else:
                            r, g, b = 33, 38, 45
                        fill_color = f'#{r:02x}{g:02x}{b:02x}'
                    else:
                        fill_color = '#161b22'

                    canvas.create_rectangle(cx, cy, cx + cell_size - 1, cy + cell_size - 1,
                                            fill=fill_color, outline=BG_PRIMARY, width=1)
                    text_color = FG_TEXT if pnl_val is not None else '#30363d'
                    canvas.create_text(cx + cell_size // 2, cy + cell_size // 3,
                                       text=str(day), fill=text_color,
                                       font=(FONT_FAMILY, 9))
                    if pnl_val is not None:
                        pnl_text = f"${abs(pnl_val):,.0f}" if abs(pnl_val) >= 1 else '$0'
                        canvas.create_text(cx + cell_size // 2, cy + cell_size * 2 // 3,
                                           text=pnl_text, fill=text_color,
                                           font=(FONT_FAMILY, 7))

        # Grand total at bottom of canvas
        total_rows = (len(months) + cols_per_row - 1) // cols_per_row
        bottom_y = y_start + total_rows * month_block_h + 10
        canvas.create_text(cw // 2, bottom_y,
                           text=f"Total: {sign}${grand_total:,.0f}",
                           fill=gt_color, font=(FONT_FAMILY, 13, 'bold'))

        canvas.configure(scrollregion=canvas.bbox('all') or (0, 0, cw, bottom_y + 30))

    # Redraw when year selection changes
    year_combo.bind('<<ComboboxSelected>>', _draw_year_grid)

    # Draw on load and on resize
    canvas.bind('<Configure>', _draw_year_grid)
    win.after(50, _draw_year_grid)


def draw_equity_curve(cumulative_pnl, canvas):
    """Draw an equity curve on the canvas."""
    canvas.delete('all')
    canvas.update_idletasks()
    cw = canvas.winfo_width()
    ch = canvas.winfo_height()
    if cw < 50 or ch < 50:
        return

    if not cumulative_pnl or len(cumulative_pnl) < 2:
        canvas.create_text(cw // 2, ch // 2, text='Not enough trades',
                           fill=FG_DIM, font=(FONT_FAMILY, 12))
        return

    pad_x = 50
    pad_y = 30
    plot_w = cw - 2 * pad_x
    plot_h = ch - 2 * pad_y

    min_pnl = min(cumulative_pnl)
    max_pnl = max(cumulative_pnl)
    pnl_range = max_pnl - min_pnl
    if pnl_range == 0:
        pnl_range = 1

    n = len(cumulative_pnl)

    def x_for(i):
        return pad_x + (i / (n - 1)) * plot_w

    def y_for(v):
        return pad_y + plot_h - ((v - min_pnl) / pnl_range) * plot_h

    # Zero line
    if min_pnl <= 0 <= max_pnl:
        zy = y_for(0)
        canvas.create_line(pad_x, zy, cw - pad_x, zy, fill='#30363d', dash=(4, 4))
        canvas.create_text(pad_x - 5, zy, text='$0', fill=FG_DIM,
                           font=(FONT_FAMILY, 8), anchor=tk.E)

    # Y-axis labels
    for frac in (0, 0.25, 0.5, 0.75, 1.0):
        val = min_pnl + frac * pnl_range
        yy = y_for(val)
        canvas.create_text(pad_x - 5, yy, text=f"${val:,.0f}", fill=FG_DIM,
                           font=(FONT_FAMILY, 8), anchor=tk.E)
        canvas.create_line(pad_x, yy, cw - pad_x, yy, fill='#21262d', width=1)

    # X-axis labels
    step = max(1, n // 6)
    for i in range(0, n, step):
        xx = x_for(i)
        canvas.create_text(xx, ch - pad_y + 12, text=str(i + 1), fill=FG_DIM,
                           font=(FONT_FAMILY, 8))

    canvas.create_text(cw // 2, ch - 8, text='Trade #', fill=FG_DIM,
                       font=(FONT_FAMILY, 8))

    # Full fill under curve split at zero: green above zero, red below zero
    bottom_y = pad_y + plot_h
    has_zero = min_pnl <= 0 <= max_pnl
    zy = y_for(0) if has_zero else None

    # Build curve points as (x, y) pairs
    curve_pts = [(x_for(i), y_for(val)) for i, val in enumerate(cumulative_pnl)]

    if has_zero:
        # Green fill: curve clamped above zero → down to zero line
        green_pts = [x_for(0), zy]
        for cx, cy in curve_pts:
            green_pts.extend([cx, min(cy, zy)])
        green_pts.extend([x_for(n - 1), zy])
        canvas.create_polygon(*green_pts, fill='#0d2818', outline='')

        # Red fill: zero line → curve clamped below zero → down to bottom
        red_pts = [x_for(0), zy]
        for cx, cy in curve_pts:
            red_pts.extend([cx, max(cy, zy)])
        red_pts.extend([x_for(n - 1), zy])
        canvas.create_polygon(*red_pts, fill='#2d1018', outline='')
    else:
        # All positive or all negative — single fill to bottom
        fill_color = '#0d2818' if max_pnl >= 0 else '#2d1018'
        fill_pts = [x_for(0), bottom_y]
        for cx, cy in curve_pts:
            fill_pts.extend([cx, cy])
        fill_pts.extend([x_for(n - 1), bottom_y])
        canvas.create_polygon(*fill_pts, fill=fill_color, outline='')

    # Draw the line — green when above zero, red when below zero
    # Split into segments at zero crossings for proper coloring
    if has_zero:
        # Find zero-crossing interpolated points and draw colored segments
        seg_pts = []  # current segment points
        seg_color = FG_GREEN if cumulative_pnl[0] >= 0 else FG_RED
        for i in range(n):
            val = cumulative_pnl[i]
            cx, cy = curve_pts[i]
            # Check for zero crossing between previous and current point
            if i > 0:
                prev_val = cumulative_pnl[i - 1]
                if (prev_val >= 0) != (val >= 0) and prev_val != val:
                    # Interpolate x position at zero crossing
                    frac = abs(prev_val) / abs(val - prev_val)
                    cross_x = x_for(i - 1) + frac * (x_for(i) - x_for(i - 1))
                    cross_y = zy
                    seg_pts.extend([cross_x, cross_y])
                    # Draw completed segment
                    if len(seg_pts) >= 4:
                        canvas.create_line(*seg_pts, fill=seg_color, width=2)
                    # Start new segment from crossing point
                    seg_color = FG_GREEN if val >= 0 else FG_RED
                    seg_pts = [cross_x, cross_y]
            seg_pts.extend([cx, cy])
        # Draw final segment
        if len(seg_pts) >= 4:
            canvas.create_line(*seg_pts, fill=seg_color, width=2)
    else:
        # All one color
        line_color = FG_GREEN if max_pnl >= 0 else FG_RED
        points = []
        for cx, cy in curve_pts:
            points.extend([cx, cy])
        if len(points) >= 4:
            canvas.create_line(*points, fill=line_color, width=2)

    # End value label
    last_val = cumulative_pnl[-1]
    end_color = FG_GREEN if last_val >= 0 else FG_RED
    canvas.create_oval(x_for(n - 1) - 4, y_for(last_val) - 4,
                       x_for(n - 1) + 4, y_for(last_val) + 4,
                       fill=end_color, outline=end_color)
    canvas.create_text(x_for(n - 1), y_for(last_val) - 12,
                       text=f"${last_val:,.0f}", fill=end_color,
                       font=(FONT_FAMILY, 9, 'bold'))


def draw_daily_bars(daily_pnl, canvas):
    """Draw a daily P&L bar chart — green/red bars, one per trading day."""
    canvas.delete('all')
    canvas.update_idletasks()
    cw = canvas.winfo_width()
    ch = canvas.winfo_height()
    if cw < 50 or ch < 50:
        return

    if not daily_pnl:
        canvas.create_text(cw // 2, ch // 2, text='No trades yet',
                           fill=FG_DIM, font=(FONT_FAMILY, 12))
        return

    sorted_days = sorted(daily_pnl.items())
    n = len(sorted_days)
    pad_x = 50
    pad_y = 30
    plot_w = cw - 2 * pad_x
    plot_h = ch - 2 * pad_y

    max_abs = max(abs(v) for _, v in sorted_days)
    if max_abs == 0:
        max_abs = 1

    bar_w = max(2, plot_w / n - 1)
    gap = max(0, min(2, (plot_w / n - bar_w)))

    zero_y = pad_y + plot_h / 2

    def y_for(v):
        return zero_y - (v / max_abs) * (plot_h / 2)

    # Zero line
    canvas.create_line(pad_x, zero_y, cw - pad_x, zero_y, fill='#30363d', dash=(4, 4))
    canvas.create_text(pad_x - 5, zero_y, text='$0', fill=FG_DIM,
                       font=(FONT_FAMILY, 8), anchor=tk.E)

    # Y-axis labels
    for frac in (-1, -0.5, 0.5, 1):
        val = frac * max_abs
        yy = y_for(val)
        canvas.create_text(pad_x - 5, yy, text=f"${val:,.0f}", fill=FG_DIM,
                           font=(FONT_FAMILY, 8), anchor=tk.E)
        canvas.create_line(pad_x, yy, cw - pad_x, yy, fill='#21262d', width=1)

    # Bars
    for i, (date_str, pnl) in enumerate(sorted_days):
        x = pad_x + i * (bar_w + gap)
        top = y_for(pnl)
        fill = FG_GREEN if pnl >= 0 else FG_RED
        canvas.create_rectangle(x, top, x + bar_w, zero_y, fill=fill, outline='')

    # X-axis date labels (every Nth)
    label_step = max(1, n // 8)
    for i in range(0, n, label_step):
        date_str = sorted_days[i][0]
        x = pad_x + i * (bar_w + gap) + bar_w / 2
        label = date_str[5:]  # MM-DD
        canvas.create_text(x, ch - pad_y + 12, text=label, fill=FG_DIM,
                           font=(FONT_FAMILY, 7), angle=45 if n > 15 else 0)


def draw_pnl_by_hour(hourly_pnl, canvas):
    """Draw a P&L by hour bar chart — bars for hours 0-23 CT."""
    canvas.delete('all')
    canvas.update_idletasks()
    cw = canvas.winfo_width()
    ch = canvas.winfo_height()
    if cw < 50 or ch < 50:
        return

    if not hourly_pnl:
        canvas.create_text(cw // 2, ch // 2, text='No trades yet',
                           fill=FG_DIM, font=(FONT_FAMILY, 12))
        return

    pad_x = 50
    pad_y = 30
    plot_w = cw - 2 * pad_x
    plot_h = ch - 2 * pad_y

    # Build values for all 24 hours
    hours = list(range(24))
    values = [hourly_pnl.get(h, 0) for h in hours]
    max_abs = max(abs(v) for v in values) if any(v != 0 for v in values) else 1
    if max_abs == 0:
        max_abs = 1

    bar_w = max(2, plot_w / 24 - 2)
    gap = max(1, (plot_w / 24 - bar_w))
    zero_y = pad_y + plot_h / 2

    def y_for(v):
        return zero_y - (v / max_abs) * (plot_h / 2)

    # Zero line
    canvas.create_line(pad_x, zero_y, cw - pad_x, zero_y, fill='#30363d', dash=(4, 4))
    canvas.create_text(pad_x - 5, zero_y, text='$0', fill=FG_DIM,
                       font=(FONT_FAMILY, 8), anchor=tk.E)

    # Y-axis labels
    for frac in (-1, -0.5, 0.5, 1):
        val = frac * max_abs
        yy = y_for(val)
        canvas.create_text(pad_x - 5, yy, text=f"${val:,.0f}", fill=FG_DIM,
                           font=(FONT_FAMILY, 8), anchor=tk.E)
        canvas.create_line(pad_x, yy, cw - pad_x, yy, fill='#21262d', width=1)

    # Bars + hour labels
    for i, h in enumerate(hours):
        x = pad_x + i * (bar_w + gap)
        pnl = values[i]
        top = y_for(pnl)
        fill = FG_GREEN if pnl >= 0 else FG_RED
        if pnl != 0:
            canvas.create_rectangle(x, top, x + bar_w, zero_y, fill=fill, outline='')
        # Hour label
        canvas.create_text(x + bar_w / 2, ch - pad_y + 12, text=str(h), fill=FG_DIM,
                           font=(FONT_FAMILY, 7))

    # Title
    canvas.create_text(cw // 2, pad_y // 2, text='Aggregate P&L by Hour (CT)',
                       fill=FG_DIM, font=(FONT_FAMILY, 9))


DURATION_BUCKETS = [
    (15, 'Under 15 sec'), (45, '15-45 sec'), (60, '45 sec - 1 min'),
    (120, '1 min - 2 min'), (300, '2 min - 5 min'), (600, '5 min - 10 min'),
    (1800, '10 min - 30 min'), (3600, '30 min - 1 hour'), (7200, '1 hour - 2 hours'),
    (14400, '2 hours - 4 hours'), (float('inf'), '4 hours and up'),
]


def _bucket_trades_by_duration(trades):
    """Sort trades into duration buckets. Returns {bucket_label: {'count': n, 'pnl': x, 'wins': w}}."""
    buckets = {label: {'count': 0, 'pnl': 0.0, 'wins': 0} for _, label in DURATION_BUCKETS}
    for t in trades:
        entry_str = t.get('entry_time', '')
        exit_str = t.get('exit_time', '')
        if not entry_str or not exit_str:
            continue
        try:
            efmt = '%Y-%m-%d %H:%M:%S'
            dur = (datetime.strptime(exit_str[:19], efmt) -
                   datetime.strptime(entry_str[:19], efmt)).total_seconds()
        except (ValueError, TypeError):
            continue
        if dur < 0:
            continue
        for threshold, label in DURATION_BUCKETS:
            if dur < threshold:
                buckets[label]['count'] += 1
                buckets[label]['pnl'] += t['pnl_dollars']
                if t['pnl_dollars'] > 0:
                    buckets[label]['wins'] += 1
                break
    return buckets


def draw_duration_chart(trades, canvas):
    """Combined horizontal bar chart — trade count per duration bucket, colored by win rate."""
    canvas.delete('all')
    canvas.update_idletasks()
    cw = canvas.winfo_width()
    ch = canvas.winfo_height()
    if cw < 80 or ch < 50:
        return

    # Border
    canvas.create_rectangle(2, 2, cw - 2, ch - 2, outline='#30363d', width=1)

    buckets = _bucket_trades_by_duration(trades)
    labels = [label for _, label in DURATION_BUCKETS]
    counts = [buckets[l]['count'] for l in labels]

    if not any(counts):
        canvas.create_text(cw // 2, ch // 2, text='No trades yet',
                           fill=FG_DIM, font=(FONT_FAMILY, 12))
        return

    n = len(labels)
    title_h = 28
    label_w = 115
    pad_r = 60  # room for "14  75%" at end of bar
    pad_bot = 22
    plot_w = cw - label_w - pad_r
    plot_h = ch - title_h - pad_bot
    bar_h = max(4, plot_h / n - 3)
    gap = max(1, (plot_h / n - bar_h))
    max_count = max(counts) if max(counts) > 0 else 1

    # Round up to a nice axis max
    nice_max = max_count
    for step in (1, 2, 4, 5, 8, 10, 15, 20, 25, 50, 100, 200, 500):
        if step * 4 >= max_count:
            nice_max = step * 4
            break

    # Title
    canvas.create_text(10, title_h // 2, text='Trade Duration Analysis',
                       fill=FG_ACCENT, font=(FONT_FAMILY, 10), anchor=tk.W)

    # X-axis gridlines + labels
    x_steps = 4
    for tick in range(x_steps + 1):
        val = nice_max * tick / x_steps
        xx = label_w + (val / nice_max) * plot_w
        canvas.create_line(xx, title_h, xx, ch - pad_bot, fill='#21262d', width=1)
        canvas.create_text(xx, ch - pad_bot + 10, text=str(int(val)),
                           fill=FG_DIM, font=(FONT_FAMILY, 7))

    # Bars
    for i, label in enumerate(labels):
        y = title_h + i * (bar_h + gap)
        count = counts[i]
        wins = buckets[label]['wins']
        wr = (wins / count * 100) if count > 0 else 0

        # Bucket label (left)
        canvas.create_text(label_w - 8, y + bar_h / 2, text=label, fill=FG_DIM,
                           font=(FONT_FAMILY, 8), anchor=tk.E)

        if count > 0:
            bar_end = label_w + (count / nice_max) * plot_w
            fill = FG_GREEN if wr >= 50 else FG_RED
            canvas.create_rectangle(label_w, y, bar_end, y + bar_h,
                                    fill=fill, outline='')
            # Count + win rate at end of bar
            canvas.create_text(bar_end + 5, y + bar_h / 2,
                               text=f"{count}  ({wr:.0f}%)",
                               fill=FG_TEXT, font=(FONT_FAMILY, 8), anchor=tk.W)
        else:
            canvas.create_text(label_w + 5, y + bar_h / 2, text='0',
                               fill=FG_DIM, font=(FONT_FAMILY, 8), anchor=tk.W)


def draw_dayhour_heatmap(dayhour_pnl, canvas):
    """Draw a Day × Hour P&L heatmap on the canvas. Rows=Mon-Sun, Cols=hours 0-23 CT."""
    canvas.delete('all')
    canvas.update_idletasks()
    cw = canvas.winfo_width()
    ch = canvas.winfo_height()
    if cw < 50 or ch < 50:
        return

    if not dayhour_pnl:
        canvas.create_text(cw // 2, ch // 2, text='No trades yet',
                           fill=FG_DIM, font=(FONT_FAMILY, 12))
        return

    max_abs = max(abs(v) for v in dayhour_pnl.values()) if dayhour_pnl else 1
    if max_abs == 0:
        max_abs = 1

    day_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    n_rows = 7
    n_cols = 24
    label_w = 40
    header_h = 18
    pad = 6

    avail_w = cw - label_w - 2 * pad
    avail_h = ch - header_h - 2 * pad
    cell_w = max(12, avail_w // n_cols)
    cell_h = max(12, avail_h // n_rows)

    x0 = label_w + pad
    y0 = header_h + pad

    # Hour labels across top
    for h in range(n_cols):
        cx = x0 + h * cell_w + cell_w // 2
        canvas.create_text(cx, y0 - 6, text=str(h), fill=FG_DIM,
                           font=(FONT_FAMILY, 7))

    # Row labels and cells
    for r, day_name in enumerate(day_labels):
        cy = y0 + r * cell_h
        canvas.create_text(x0 - 6, cy + cell_h // 2, text=day_name, fill=FG_DIM,
                           font=(FONT_FAMILY, 8), anchor=tk.E)
        for h in range(n_cols):
            cx = x0 + h * cell_w
            pnl_val = dayhour_pnl.get((r, h), 0)
            if pnl_val != 0:
                intensity = min(abs(pnl_val) / max_abs, 1.0)
                if pnl_val > 0:
                    red = int(13 + intensity * (63 - 13))
                    grn = int(17 + intensity * (185 - 17))
                    blu = int(23 + intensity * (80 - 23))
                else:
                    red = int(17 + intensity * (249 - 17))
                    grn = int(22 + intensity * (117 - 22))
                    blu = int(34 + intensity * (131 - 34))
                fill_color = f'#{red:02x}{grn:02x}{blu:02x}'
            else:
                fill_color = '#161b22'
            canvas.create_rectangle(cx, cy, cx + cell_w - 1, cy + cell_h - 1,
                                    fill=fill_color, outline=BG_PRIMARY, width=1)
            # Show dollar amount if cell is big enough
            if pnl_val != 0 and cell_w >= 22 and cell_h >= 16:
                txt = f"${abs(pnl_val):,.0f}" if abs(pnl_val) >= 1 else ''
                canvas.create_text(cx + cell_w // 2, cy + cell_h // 2, text=txt,
                                   fill=FG_TEXT, font=(FONT_FAMILY, max(6, min(8, cell_w // 4))))


def update_breakdown_tables(breakdown, perf_widgets):
    """Populate session and day-of-week breakdown treeviews."""
    def fmt(v):
        sign = '+' if v > 0 else ''
        return f"{sign}${v:,.0f}"

    for key, tree_key in [('session', 'session_tree'), ('dow', 'dow_tree'),
                          ('symbol', 'sym_tree'), ('monthly', 'monthly_tree')]:
        tv = perf_widgets[tree_key]
        for item in tv.get_children():
            tv.delete(item)
        for i, row in enumerate(breakdown[key]):
            net = row['net_pnl']
            is_alt = i % 2 == 1
            if net > 0:
                tag = 'alt_profit' if is_alt else 'profit'
            elif net < 0:
                tag = 'alt_loss' if is_alt else 'loss'
            else:
                tag = 'alt_flat' if is_alt else 'flat'
            tv.insert('', tk.END, values=(
                row['label'], row['win_count'], fmt(row['win_pnl']),
                fmt(row['avg_win']), row['loss_count'], fmt(row['loss_pnl']),
                fmt(row['avg_loss']), fmt(net),
                f"{row['win_pct']:.0f}%", f"{row['ratio']:.2f}",
            ), tags=(tag,))


def _date_range_cutoff(range_label):
    """Return a YYYY-MM-DD cutoff string for the given date range label, or None for All Time."""
    from datetime import timedelta
    today = datetime.now()
    if range_label == 'Today':
        return today.strftime('%Y-%m-%d')
    elif range_label == 'This Week':
        monday = today - timedelta(days=today.weekday())
        return monday.strftime('%Y-%m-%d')
    elif range_label == 'Last 7 Days':
        return (today - timedelta(days=7)).strftime('%Y-%m-%d')
    elif range_label == 'This Month':
        return today.strftime('%Y-%m-01')
    elif range_label == 'Last 30 Days':
        return (today - timedelta(days=30)).strftime('%Y-%m-%d')
    return None  # All Time


def _update_symbol_dropdown_options(store, perf_widgets):
    """Rebuild the Symbol dropdown options from all known fills."""
    with DATA_LOCK:
        traded_bases = set()
        for (inst, acct, sym) in store['fills'].keys():
            base = get_base_symbol(sym)
            if base:
                traded_bases.add(base)

    sym_options = ['All Symbols']
    # Add product groups that have traded symbols
    for grp, syms in SYMBOL_GROUPS.items():
        if any(s in traded_bases for s in syms):
            sym_options.append(grp)
    # Add "Other" if any traded symbol is not in a known group
    if any(s not in _ALL_GROUPED_SYMBOLS for s in traded_bases):
        sym_options.append('Other')

    for combo_key in ('sym_filter_combo', 'brk_sym_combo'):
        combo = perf_widgets.get(combo_key)
        if combo:
            current_vals = list(combo['values'])
            if sym_options != current_vals:
                combo['values'] = sym_options


def refresh_performance(root, perf_widgets, store):
    """Periodic performance tab refresh — only recomputes when dirty."""
    if store.get('shutdown'):
        return
    if store.get('perf_dirty'):
        store['perf_dirty'] = False
        store['today_dirty'] = True  # propagate to Today tab

        # Update account selector options from known fills
        with DATA_LOCK:
            known_accounts = sorted(set(
                f"{inst}: {acct}" for (inst, acct, sym) in store['fills'].keys()
            ))
        for sel_key in ('acct_selector', 'brk_acct_selector'):
            selector = perf_widgets.get(sel_key)
            if selector:
                selector.update_options(known_accounts)

        acct_filter = store.get('perf_acct_filter', 'All Accounts')
        trades = aggregate_all_round_trips(store, acct_filter)

        # Apply date range filter
        date_range = store.get('perf_date_range', 'All Time')
        if date_range == 'Custom':
            start = store.get('perf_custom_start', '')
            end = store.get('perf_custom_end', '')
            if start:
                trades = [t for t in trades if t.get('trade_date', '') >= start]
            if end:
                trades = [t for t in trades if t.get('trade_date', '') <= end]
        else:
            cutoff = _date_range_cutoff(date_range)
            if cutoff:
                trades = [t for t in trades if t.get('trade_date', '') >= cutoff]

        # Apply side filter
        side_filter = store.get('perf_side_filter', 'All')
        if side_filter == 'Longs':
            trades = [t for t in trades if t.get('side') == 'Long']
        elif side_filter == 'Shorts':
            trades = [t for t in trades if t.get('side') == 'Short']

        # Apply size filter
        size_filter = store.get('perf_size_filter', 'All')
        if size_filter == 'Full Size':
            trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) in FULL_SYMBOLS]
        elif size_filter == 'Micro':
            trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) in MICRO_SYMBOLS]

        # Apply symbol filter
        sym_filter = store.get('perf_symbol_filter', 'All Symbols')
        if sym_filter != 'All Symbols':
            if sym_filter == 'Other':
                trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) not in _ALL_GROUPED_SYMBOLS]
            elif sym_filter in SYMBOL_GROUPS:
                allowed = set(SYMBOL_GROUPS[sym_filter])
                trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) in allowed]

        # Update Symbol dropdown options dynamically from current data (pre-symbol-filter)
        _update_symbol_dropdown_options(store, perf_widgets)

        stats = compute_performance_stats(trades)
        perf_widgets['cal_daily_pnl'] = stats['daily_pnl']
        update_stats_panel(stats, perf_widgets['stat_cards'])
        update_trade_log(trades, perf_widgets['trade_tree'])
        draw_calendar(stats['daily_pnl'], perf_widgets['cal_canvas'], perf_widgets)
        draw_equity_curve(stats['cumulative_pnl'], perf_widgets['eq_canvas'])
        draw_daily_bars(stats['daily_pnl'], perf_widgets['bar_canvas'])
        draw_pnl_by_hour(stats.get('hourly_pnl', {}), perf_widgets['hour_canvas'])
        draw_duration_chart(trades, perf_widgets['dur_canvas'])

        # Breakdown tab
        breakdown = compute_breakdown_stats(trades, sym_filter)
        update_breakdown_tables(breakdown, perf_widgets)
        draw_dayhour_heatmap(breakdown.get('dayhour_pnl', {}), perf_widgets['hm_canvas'])

    root.after(PERF_REFRESH_MS, refresh_performance, root, perf_widgets, store)


def _update_today_stats(stats, stat_cards):
    """Update the Today tab stat cards (compact layout with streaks combo)."""
    def fmt_currency(v):
        sign = '+' if v > 0 else ''
        return f"{sign}${v:,.0f}"

    def color_for(v):
        if v > 0:
            return FG_GREEN
        elif v < 0:
            return FG_RED
        return FG_TEXT

    mappings = {
        'total_trades': (str(stats['total_trades']), FG_TEXT),
        'win_rate': (f"{stats['win_rate']:.1f}%",
                     FG_GREEN if stats['win_rate'] >= 50 else FG_RED),
        'profit_factor': (f"{stats['profit_factor']:.2f}" if stats['profit_factor'] != float('inf') else 'INF',
                          FG_GREEN if stats['profit_factor'] >= 1 else FG_RED),
        'net_pnl': (fmt_currency(stats['net_pnl']), color_for(stats['net_pnl'])),
        'avg_win': (fmt_currency(stats['avg_win']), FG_GREEN),
        'avg_loss': (fmt_currency(stats['avg_loss']), FG_RED),
        'max_win': (fmt_currency(stats['max_win']), FG_GREEN),
        'max_loss': (fmt_currency(stats['max_loss']), FG_RED),
        'ls_split': (f"L:{stats['long_count']} S:{stats['short_count']}", FG_TEXT),
        'streaks': (f"W:{stats.get('max_win_streak', 0)} L:{stats.get('max_loss_streak', 0)}", FG_TEXT),
    }
    for key, (text, fg) in mappings.items():
        if key in stat_cards:
            stat_cards[key].configure(text=text, fg=fg)


def _update_today_trade_log(trades, trade_tree):
    """Populate Today trade log — same as update_trade_log but with duration column."""
    for item in trade_tree.get_children():
        trade_tree.delete(item)

    for i, t in enumerate(trades):
        date_part, time_part = _format_local_time(t.get('exit_time', ''))
        pnl = t['pnl_dollars']

        # Compute duration
        dur_str = ''
        exit_dt = t.get('exit_time', '')
        entry_str = t.get('entry_time', '')
        if entry_str and exit_dt:
            try:
                efmt = '%Y-%m-%d %H:%M:%S'
                dur_secs = (datetime.strptime(exit_dt[:19], efmt) -
                            datetime.strptime(entry_str[:19], efmt)).total_seconds()
                if dur_secs < 0:
                    dur_str = ''
                elif dur_secs < 60:
                    dur_str = f"{dur_secs:.0f}s"
                elif dur_secs < 3600:
                    dur_str = f"{dur_secs / 60:.1f}m"
                else:
                    dur_str = f"{dur_secs / 3600:.1f}h"
            except (ValueError, TypeError):
                pass

        is_alt = i % 2 == 1
        if pnl > 0:
            tag = 'alt_profit' if is_alt else 'profit'
        elif pnl < 0:
            tag = 'alt_loss' if is_alt else 'loss'
        else:
            tag = 'alt_flat' if is_alt else 'flat'

        sign = '+' if pnl > 0 else ''
        trade_tree.insert('', tk.END, values=(
            date_part, time_part, t['symbol'], t['side'],
            t['qty'], f"{t['entry_price']:,.2f}", f"{t['exit_price']:,.2f}",
            f"{sign}${pnl:,.2f}", dur_str,
        ), tags=(tag,))


def refresh_today_tab(root, today_widgets, store):
    """Periodic Today tab refresh — shows today's session data."""
    if store.get('shutdown'):
        return
    if store.get('today_dirty') or store.get('today_needs_init', True):
        store['today_dirty'] = False
        store['today_needs_init'] = False

        acct_filter = store.get('perf_acct_filter', 'All Accounts')
        trades = aggregate_all_round_trips(store, acct_filter)

        # Filter to today only
        today_date = datetime.now().strftime('%Y-%m-%d')
        trades = [t for t in trades if t.get('trade_date', '') == today_date]

        # Apply side filter (shared with Performance tab)
        side_filter = store.get('perf_side_filter', 'All')
        if side_filter == 'Longs':
            trades = [t for t in trades if t.get('side') == 'Long']
        elif side_filter == 'Shorts':
            trades = [t for t in trades if t.get('side') == 'Short']

        # Apply size filter (shared with Performance tab)
        size_filter = store.get('perf_size_filter', 'All')
        if size_filter == 'Full Size':
            trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) in FULL_SYMBOLS]
        elif size_filter == 'Micro':
            trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) in MICRO_SYMBOLS]

        # Apply symbol filter (shared with Performance tab)
        sym_filter = store.get('perf_symbol_filter', 'All Symbols')
        if sym_filter != 'All Symbols':
            if sym_filter == 'Other':
                trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) not in _ALL_GROUPED_SYMBOLS]
            elif sym_filter in SYMBOL_GROUPS:
                allowed = set(SYMBOL_GROUPS[sym_filter])
                trades = [t for t in trades if get_base_symbol(t.get('symbol', '')) in allowed]

        stats = compute_performance_stats(trades)
        _update_today_stats(stats, today_widgets['stat_cards'])
        _update_today_trade_log(trades, today_widgets['trade_tree'])
        draw_equity_curve(stats['cumulative_pnl'], today_widgets['eq_canvas'])
        draw_duration_chart(trades, today_widgets['dur_canvas'])

    root.after(PERF_REFRESH_MS, refresh_today_tab, root, today_widgets, store)


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION MANAGER
# ─────────────────────────────────────────────────────────────────────────────


def rebuild_status_bar(store):
    """Destroy and rebuild per-instance frames in the status bar from current SC_INSTANCES."""
    gui = store['gui']
    status_frame = gui['status_frame']

    # Destroy old instance frames
    for frame in gui['instance_frames'].values():
        frame.destroy()

    status_labels = {}
    balance_labels = {}
    instance_frames = {}

    for name in SC_INSTANCES:
        frame = tk.Frame(status_frame, bg=BG_ALT)
        frame.pack(side=tk.LEFT, padx=(0, 16))
        lbl = tk.Label(frame,
                       text=f"{name}: Disconnected",
                       bg=BG_ALT,
                       fg=FG_RED,
                       font=(FONT_FAMILY, 9))
        lbl.pack()
        bal_lbl = tk.Label(frame,
                           text="",
                           bg=BG_ALT,
                           fg=FG_DIM,
                           font=(FONT_FAMILY, 8))
        bal_lbl.pack()
        status_labels[name] = lbl
        balance_labels[name] = bal_lbl
        instance_frames[name] = frame

    gui['status_labels'] = status_labels
    gui['balance_labels'] = balance_labels
    gui['instance_frames'] = instance_frames


def open_instance_editor(parent, store, existing_name=None, existing_cfg=None):
    """Open a small dialog to add or edit an instance. Returns (name, cfg) or None."""
    dlg = tk.Toplevel(parent)
    dlg.title("Edit Instance" if existing_name else "Add Instance")
    dlg.configure(bg=BG_PRIMARY)
    dlg.geometry("340x280")
    dlg.resizable(False, False)
    dlg.transient(parent)
    dlg.grab_set()

    result = [None]  # mutable holder for return value

    # Fields
    fields_frame = tk.Frame(dlg, bg=BG_PRIMARY, padx=16, pady=12)
    fields_frame.pack(fill=tk.BOTH, expand=True)

    tk.Label(fields_frame, text="Name:", bg=BG_PRIMARY, fg=FG_TEXT,
             font=(FONT_FAMILY, 10)).grid(row=0, column=0, sticky='w', pady=4)
    name_var = tk.StringVar(value=existing_name or "")
    name_entry = tk.Entry(fields_frame, textvariable=name_var, bg=BG_HEADER, fg=FG_TEXT,
                          insertbackground=FG_TEXT, font=(FONT_FAMILY, 10), width=24)
    name_entry.grid(row=0, column=1, sticky='ew', pady=4, padx=(8, 0))
    if existing_name:
        name_entry.configure(state='disabled')

    tk.Label(fields_frame, text="Host:", bg=BG_PRIMARY, fg=FG_TEXT,
             font=(FONT_FAMILY, 10)).grid(row=1, column=0, sticky='w', pady=4)
    host_var = tk.StringVar(value=existing_cfg.get('host', '127.0.0.1') if existing_cfg else '127.0.0.1')
    tk.Entry(fields_frame, textvariable=host_var, bg=BG_HEADER, fg=FG_TEXT,
             insertbackground=FG_TEXT, font=(FONT_FAMILY, 10), width=24
             ).grid(row=1, column=1, sticky='ew', pady=4, padx=(8, 0))

    tk.Label(fields_frame, text="Port:", bg=BG_PRIMARY, fg=FG_TEXT,
             font=(FONT_FAMILY, 10)).grid(row=2, column=0, sticky='w', pady=4)
    port_var = tk.StringVar(value=str(existing_cfg.get('port', 11050) if existing_cfg else 11050))
    tk.Entry(fields_frame, textvariable=port_var, bg=BG_HEADER, fg=FG_TEXT,
             insertbackground=FG_TEXT, font=(FONT_FAMILY, 10), width=24
             ).grid(row=2, column=1, sticky='ew', pady=4, padx=(8, 0))

    tk.Label(fields_frame, text="Accounts:", bg=BG_PRIMARY, fg=FG_TEXT,
             font=(FONT_FAMILY, 10)).grid(row=3, column=0, sticky='nw', pady=4)
    accounts_text = tk.Text(fields_frame, bg=BG_HEADER, fg=FG_TEXT,
                            insertbackground=FG_TEXT, font=(FONT_FAMILY, 10),
                            width=24, height=2)
    accounts_text.grid(row=3, column=1, sticky='ew', pady=4, padx=(8, 0))
    if existing_cfg and existing_cfg.get('accounts'):
        accounts_text.insert('1.0', ', '.join(existing_cfg['accounts']))

    enabled_var = tk.BooleanVar(value=existing_cfg.get('enabled', True) if existing_cfg else True)
    tk.Checkbutton(fields_frame, text="Enabled", variable=enabled_var,
                   bg=BG_PRIMARY, fg=FG_TEXT, selectcolor=BG_HEADER,
                   activebackground=BG_PRIMARY, activeforeground=FG_TEXT,
                   font=(FONT_FAMILY, 10)).grid(row=4, column=1, sticky='w', pady=4, padx=(8, 0))

    # Save and Cancel buttons in row 5 of the same grid
    ttk.Button(fields_frame, text="Cancel", command=dlg.destroy,
               style='CM.TButton').grid(row=5, column=0, sticky='w', pady=(12, 0))

    fields_frame.columnconfigure(1, weight=1)

    # Error label
    error_lbl = tk.Label(fields_frame, text="", bg=BG_PRIMARY, fg=FG_RED, font=(FONT_FAMILY, 9))
    error_lbl.grid(row=6, column=0, columnspan=2, sticky='w', pady=(4, 0))

    def on_save():
        name = name_var.get().strip()
        if not name:
            error_lbl.configure(text="Name cannot be empty.")
            return
        try:
            port = int(port_var.get().strip())
        except ValueError:
            error_lbl.configure(text="Port must be a number.")
            return
        if not existing_name and name in SC_INSTANCES:
            error_lbl.configure(text=f"'{name}' already exists.")
            return

        raw_accounts = accounts_text.get('1.0', tk.END).strip()
        accounts = [a.strip() for a in raw_accounts.split(',') if a.strip()] if raw_accounts else []

        result[0] = (name, {
            'host': host_var.get().strip() or '127.0.0.1',
            'port': port,
            'accounts': accounts,
            'enabled': enabled_var.get(),
        })
        dlg.destroy()

    ttk.Button(fields_frame, text="Save", command=on_save,
               style='CMAccent.TButton').grid(row=5, column=1, sticky='e', pady=(12, 0))

    dlg.wait_window()
    return result[0]


def open_connection_manager(parent, store):
    """Open the Connection Manager dialog."""
    dlg = tk.Toplevel(parent)
    dlg.title("Connection Manager")
    dlg.configure(bg=BG_PRIMARY)
    dlg.geometry("640x380")
    dlg.minsize(500, 300)
    dlg.transient(parent)

    # --- Buttons (pack BEFORE tree so they claim space at bottom) ---
    btn_frame = tk.Frame(dlg, bg=BG_ALT, padx=10, pady=8)
    btn_frame.pack(side=tk.BOTTOM, fill=tk.X)

    # --- Timezone selector ---
    tz_frame = tk.Frame(dlg, bg=BG_PRIMARY, padx=10, pady=10)
    tz_frame.pack(side=tk.TOP, fill=tk.X)

    tk.Label(tz_frame, text="Timezone", bg=BG_PRIMARY, fg=FG_TEXT,
             font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(0, 8))

    tz_options = ['US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific', 'UTC']
    tz_var = tk.StringVar(value=USER_TIMEZONE)
    tz_combo = ttk.Combobox(tz_frame, textvariable=tz_var, values=tz_options,
                            state='readonly', width=16, style='Dark.TCombobox')
    tz_combo.pack(side=tk.LEFT)

    def on_tz_change(event=None):
        global USER_TIMEZONE
        USER_TIMEZONE = tz_var.get()
        save_config()
        store['perf_dirty'] = True

    tz_combo.bind('<<ComboboxSelected>>', on_tz_change)

    # --- Treeview ---
    tree_frame = tk.Frame(dlg, bg=BG_PRIMARY, padx=10, pady=10)
    tree_frame.pack(fill=tk.BOTH, expand=True)

    columns = ('name', 'host', 'port', 'accounts', 'status')
    tree = ttk.Treeview(tree_frame, columns=columns, show='headings',
                        style='Dashboard.Treeview', height=8)
    for col, heading, width in [
        ('name', 'Name', 100), ('host', 'Host', 120), ('port', 'Port', 70),
        ('accounts', 'Accounts', 180), ('status', 'Status', 120),
    ]:
        tree.heading(col, text=heading, anchor=tk.W)
        tree.column(col, width=width, minwidth=40, anchor=tk.W)

    scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
    tree.configure(yscrollcommand=scrollbar.set)
    tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def refresh_tree():
        """Refresh the treeview rows from current SC_INSTANCES and store status."""
        sel = tree.selection()
        sel_name = None
        if sel:
            vals = tree.item(sel[0], 'values')
            if vals:
                sel_name = vals[0]
        tree.delete(*tree.get_children())
        with DATA_LOCK:
            statuses = dict(store['status'])
            disabled = set(store.get('disabled', set()))
        for name, cfg in SC_INSTANCES.items():
            if name in disabled:
                status_text = 'Disabled'
            else:
                status_text = statuses.get(name, 'Disconnected')
            accts = ', '.join(cfg.get('accounts', [])) or '(all)'
            iid = tree.insert('', tk.END, values=(
                name, cfg['host'], cfg['port'], accts, status_text
            ))
            if name == sel_name:
                tree.selection_set(iid)

    def schedule_refresh():
        """Auto-refresh every 1s while dialog is open."""
        if dlg.winfo_exists():
            refresh_tree()
            dlg.after(1000, schedule_refresh)

    def get_selected():
        sel = tree.selection()
        if not sel:
            return None, None
        vals = tree.item(sel[0], 'values')
        name = vals[0]
        return name, SC_INSTANCES.get(name)

    def on_add():
        result = open_instance_editor(dlg, store)
        if result:
            name, cfg = result
            SC_INSTANCES[name] = cfg
            save_config()
            with DATA_LOCK:
                store['status'][name] = 'Disconnected'
                if not cfg.get('enabled', True):
                    store['disabled'].add(name)
            rebuild_status_bar(store)
            if cfg.get('enabled', True):
                start_instance_connection(name, cfg, store)
            refresh_tree()

    def on_edit():
        name, cfg = get_selected()
        if not name or not cfg:
            return
        old_host, old_port = cfg['host'], cfg['port']
        result = open_instance_editor(dlg, store, existing_name=name, existing_cfg=cfg)
        if result:
            _, new_cfg = result
            SC_INSTANCES[name] = new_cfg
            save_config()
            # If host/port changed, reconnect
            if new_cfg['host'] != old_host or new_cfg['port'] != old_port:
                disconnect_instance(name, store)
                with DATA_LOCK:
                    store['disabled'].discard(name)
                if new_cfg.get('enabled', True):
                    start_instance_connection(name, new_cfg, store)
            # If enabled state changed
            elif new_cfg.get('enabled', True) and name in store.get('disabled', set()):
                with DATA_LOCK:
                    store['disabled'].discard(name)
                start_instance_connection(name, new_cfg, store)
            elif not new_cfg.get('enabled', True) and name not in store.get('disabled', set()):
                disconnect_instance(name, store)
            refresh_tree()

    def on_remove():
        name, cfg = get_selected()
        if not name:
            return
        # Confirm
        confirm = messagebox.askyesno(
            "Remove Instance",
            f"Remove '{name}' and disconnect?",
            parent=dlg
        )
        if not confirm:
            return
        disconnect_instance(name, store)
        cleanup_instance_store(name, store)
        SC_INSTANCES.pop(name, None)
        save_config()
        rebuild_status_bar(store)
        refresh_tree()

    def on_connect_disconnect():
        name, cfg = get_selected()
        if not name or not cfg:
            return
        with DATA_LOCK:
            is_disabled = name in store.get('disabled', set())
        if is_disabled:
            # Connect
            with DATA_LOCK:
                store['disabled'].discard(name)
            SC_INSTANCES[name]['enabled'] = True
            save_config()
            start_instance_connection(name, cfg, store)
        else:
            # Disconnect
            disconnect_instance(name, store)
            SC_INSTANCES[name]['enabled'] = False
            save_config()
        refresh_tree()

    for text, cmd in [("Add", on_add), ("Edit", on_edit), ("Remove", on_remove)]:
        ttk.Button(btn_frame, text=text, command=cmd,
                   style='CM.TButton').pack(side=tk.LEFT, padx=(0, 6))

    ttk.Button(btn_frame, text="Connect / Disconnect", command=on_connect_disconnect,
               style='CMAccent.TButton').pack(side=tk.LEFT, padx=(0, 6))

    ttk.Button(btn_frame, text="Close", command=dlg.destroy,
               style='CM.TButton').pack(side=tk.RIGHT)

    refresh_tree()
    schedule_refresh()
    dlg.update_idletasks()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def main():
    # Load instance config (seeds defaults on first run)
    load_config()

    # Initialize fills database
    init_fills_db()

    store = init_data_store()

    # Populate disabled set from instances with enabled=false
    with DATA_LOCK:
        for name, cfg in SC_INSTANCES.items():
            if not cfg.get('enabled', True):
                store['disabled'].add(name)

    # Load historical fills from DB
    with DATA_LOCK:
        store['fills'] = load_fills_from_db()
        store['perf_dirty'] = True

    # Build GUI
    root, tree, status_labels, balance_labels, totals_label, perf_widgets, today_widgets = build_gui(store)

    # Start WebSocket connections for enabled instances only
    for name, cfg in SC_INSTANCES.items():
        if cfg.get('enabled', True):
            start_instance_connection(name, cfg, store)

    # Start heartbeat for connected instances
    def heartbeat_monitor():
        """Check all connected instances and send heartbeats."""
        with DATA_LOCK:
            if store['shutdown']:
                return
            for name in SC_INSTANCES:
                ws = store['ws_refs'].get(name)
                if ws and store['status'].get(name) == 'Connected':
                    try:
                        ws.send(make_dtc_message(DTC_HEARTBEAT))
                    except Exception:
                        pass
        t = threading.Timer(HEARTBEAT_INTERVAL, heartbeat_monitor)
        t.daemon = True
        t.start()

    heartbeat_monitor()

    # Periodic position/fill re-request for live updates (Rithmic workaround)
    def periodic_refresh_monitor():
        """Re-request positions/balances every POSITION_POLL_SECONDS,
        and fills every FILLS_POLL_SECONDS (via tick counter)."""
        with DATA_LOCK:
            if store['shutdown']:
                return
            store['_poll_tick'] += 1
            tick = store['_poll_tick']
            fills_interval = max(1, FILLS_POLL_SECONDS // POSITION_POLL_SECONDS)
            request_fills = (tick % fills_interval == 0)

            for name in SC_INSTANCES:
                ws = store['ws_refs'].get(name)
                if ws and store['status'].get(name) == 'Connected':
                    for acct in store['accounts'].get(name, []):
                        try:
                            ws.send(make_dtc_message(
                                DTC_CURRENT_POSITIONS_REQUEST,
                                TradeAccount=acct,
                                RequestID=hash((name, acct, 'pos', 'poll')) & 0x7FFFFFFF,
                            ))
                            ws.send(make_dtc_message(
                                DTC_ACCOUNT_BALANCE_REQUEST,
                                TradeAccount=acct,
                                RequestID=hash((name, acct, 'bal', 'poll')) & 0x7FFFFFFF,
                            ))
                            if request_fills:
                                days_back = (datetime.now(timezone.utc) - datetime(2025, 1, 1, tzinfo=timezone.utc)).days + 1
                                ws.send(make_dtc_message(
                                    DTC_HISTORICAL_ORDER_FILLS_REQ,
                                    TradeAccount=acct,
                                    RequestID=hash((name, acct, 'fills', 'poll')) & 0x7FFFFFFF,
                                    NumberOfDays=max(days_back, 1),
                                    StartDateTime=FILLS_START_TIMESTAMP,
                                ))
                        except Exception:
                            pass
            if request_fills:
                store['perf_dirty'] = True

        t = threading.Timer(POSITION_POLL_SECONDS, periodic_refresh_monitor)
        t.daemon = True
        t.start()

    periodic_refresh_monitor()

    # Start GUI refresh loops
    root.after(GUI_REFRESH_MS, refresh_gui, root, tree, status_labels, balance_labels, totals_label, store)
    root.after(PERF_REFRESH_MS, refresh_performance, root, perf_widgets, store)
    root.after(PERF_REFRESH_MS, refresh_today_tab, root, today_widgets, store)

    # Handle window close
    def on_closing():
        with DATA_LOCK:
            store['shutdown'] = True
            # Close all WebSocket connections
            for name, ws in list(store['ws_refs'].items()):
                try:
                    ws.close()
                except Exception:
                    pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()


if __name__ == '__main__':
    main()
