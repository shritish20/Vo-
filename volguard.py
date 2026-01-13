"""
VOLGUARD 2.0 - Production Trading System
Version: 42.5 (CRITICAL FIXES - COMPLETE)
- Fixed WebSocket mode (full instead of option_chain)
- Fixed DataFrame boolean ambiguity
- Fixed dictionary iteration crash
- Fixed Risk Manager LTP updates
"""

import os
import sys
import time
import json
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import threading
import traceback
import concurrent.futures
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from urllib.parse import quote
import io

import requests
import pandas as pd
import numpy as np
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pytz
from arch import arch_model
import psutil

# Upstox SDK
import upstox_client
from upstox_client.rest import ApiException

# ==========================================
# PRODUCTION CONFIGURATION
# ==========================================
class ProductionConfig:
    ENVIRONMENT = os.getenv("VG_ENV", "PRODUCTION")
    
    # API Credentials
    UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    # Session
    UPSTOX_CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
    UPSTOX_CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")
    UPSTOX_REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI")
    UPSTOX_REFRESH_TOKEN = os.getenv("UPSTOX_REFRESH_TOKEN")
    
    # Endpoints
    API_V2 = "https://api.upstox.com/v2"
    API_V3 = "https://api.upstox.com/v3"
    NIFTY_KEY = "NSE_INDEX|Nifty 50"
    VIX_KEY = "NSE_INDEX|India VIX"
    
    # Capital & Risk
    BASE_CAPITAL = int(os.getenv("VG_BASE_CAPITAL", "1000000"))
    MARGIN_SELL_BASE = 125000
    MARGIN_BUY_BASE = 30000
    MAX_CAPITAL_USAGE = 0.80
    DAILY_LOSS_LIMIT = 0.03
    MAX_POSITION_SIZE = 0.25
    
    # Advanced Risk
    GAMMA_DANGER_DTE = 1
    GEX_STICKY_RATIO = 0.03
    HIGH_VOL_IVP = 75.0
    LOW_VOL_IVP = 25.0
    VOV_CRASH_ZSCORE = 2.5
    VOV_WARNING_ZSCORE = 2.0
    
    # Scoring
    WEIGHT_VOL = 0.40
    WEIGHT_STRUCT = 0.30
    WEIGHT_EDGE = 0.20
    WEIGHT_RISK = 0.10
    
    # Thresholds
    FII_STRONG_LONG = 50000
    FII_STRONG_SHORT = -50000
    FII_MODERATE = 20000
    
    # Execution
    TARGET_PROFIT_PCT = 0.50
    STOP_LOSS_PCT = 1.0
    MAX_SHORT_DELTA = 0.35
    EXIT_DTE = 1
    SLIPPAGE_TOLERANCE = 0.02
    PARTIAL_FILL_TOLERANCE = 0.90
    ORDER_TIMEOUT = 10
    MAX_BID_ASK_SPREAD = 0.05
    
    # System
    POLL_INTERVAL = 1.0
    ANALYSIS_INTERVAL = 1800
    MAX_API_RETRIES = 3
    DASHBOARD_REFRESH_RATE = 1.0
    
    # Infrastructure
    DB_PATH = os.getenv("VG_DB_PATH", "/app/data/volguard.db")
    LOG_DIR = os.getenv("VG_LOG_DIR", "/app/logs")
    LOG_FILE = os.path.join(LOG_DIR, f"volguard_{ENVIRONMENT.lower()}.log")
    LOG_LEVEL = logging.INFO
    
    # Market Hours
    MARKET_OPEN = (00, 00)
    MARKET_CLOSE = (23, 59)
    SAFE_ENTRY_START = (00, 00)
    SAFE_EXIT_END = (23, 59)
    
    # Breakers
    MAX_CONSECUTIVE_LOSSES = 3
    COOL_DOWN_PERIOD = 86400
    MAX_SLIPPAGE_EVENTS_PER_DAY = 5
    
    @classmethod
    def validate(cls):
        missing = []
        if not cls.UPSTOX_ACCESS_TOKEN: missing.append("UPSTOX_ACCESS_TOKEN")
        if not cls.TELEGRAM_BOT_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.TELEGRAM_CHAT_ID: missing.append("TELEGRAM_CHAT_ID")
        if missing: raise EnvironmentError(f"Missing: {', '.join(missing)}")

# ==========================================
# LOGGING
# ==========================================
os.makedirs(ProductionConfig.LOG_DIR, exist_ok=True)
file_handler = RotatingFileHandler(
    ProductionConfig.LOG_FILE, maxBytes=10*1024*1024, backupCount=5
)
stream_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=ProductionConfig.LOG_LEVEL,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger("VOLGUARD")

# ==========================================
# TELEGRAM ALERTS
# ==========================================
class TelegramAlerter:
    def __init__(self):
        self.bot_token = ProductionConfig.TELEGRAM_BOT_TOKEN
        self.chat_id = ProductionConfig.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
    
    def send(self, message: str, level: str = "INFO"):
        emoji_map = {
            "CRITICAL": "🚨", "ERROR": "❌", "WARNING": "⚠️",
            "INFO": "ℹ️", "SUCCESS": "✅", "TRADE": "💰", "SYSTEM": "⚙️"
        }
        prefix = emoji_map.get(level, "📢")
        full_msg = f"{prefix} *VOLGUARD v42.5*\n{message}"
        try:
            requests.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": full_msg, "parse_mode": "Markdown"},
                timeout=5
            )
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")

telegram = TelegramAlerter()

# ==========================================
# DATABASE MANAGER
# ==========================================
class DatabaseManager:
    def __init__(self, db_path: str = ProductionConfig.DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.commit()
        self._init_schema()
    
    def _init_schema(self):
        schema = """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            strategy_type TEXT,
            expiry_date DATE,
            entry_premium REAL,
            max_risk REAL,
            status TEXT,
            legs_json TEXT
        );
        CREATE TABLE IF NOT EXISTS positions (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            instrument_key TEXT,
            strike REAL,
            option_type TEXT,
            side TEXT,
            qty INTEGER,
            entry_price REAL,
            current_price REAL,
            delta REAL,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS risk_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,
            severity TEXT,
            description TEXT,
            action_taken TEXT
        );
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.conn.executescript(schema)
        self.conn.commit()
    
    def save_trade(self, trade_id, strategy, expiry, legs, entry_premium, max_risk):
        self.conn.execute("""
            INSERT INTO trades (trade_id, strategy_type, expiry_date, 
                              entry_premium, max_risk, status, legs_json)
            VALUES (?, ?, ?, ?, ?, 'OPEN', ?)
        """, (trade_id, strategy, expiry, entry_premium, max_risk, json.dumps(legs)))
        self.conn.commit()
    
    def log_risk_event(self, event_type, severity, desc, action):
        self.conn.execute("""
            INSERT INTO risk_events (event_type, severity, description, action_taken)
            VALUES (?, ?, ?, ?)
        """, (event_type, severity, desc, action))
        self.conn.commit()
    
    def set_state(self, key, value):
        self.conn.execute("""
            INSERT OR REPLACE INTO system_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (key, value))
        self.conn.commit()
    
    def update_system_vitals(self, latency_ms, cpu_usage, ram_usage):
        vitals = {
            "latency": round(latency_ms, 2),
            "cpu": round(cpu_usage, 1),
            "ram": round(ram_usage, 1),
            "updated_at": datetime.now().strftime("%H:%M:%S")
        }
        self.set_state("system_vitals", json.dumps(vitals))

db = DatabaseManager()

# ==========================================
# CIRCUIT BREAKER
# ==========================================
class CircuitBreaker:
    def __init__(self):
        self.consecutive_losses = 0
        self.breaker_triggered = False
        self.breaker_until = None
    
    def check_daily_loss_limit(self, current_pnl) -> bool:
        loss_pct = abs(current_pnl) / ProductionConfig.BASE_CAPITAL
        if current_pnl < 0 and loss_pct >= ProductionConfig.DAILY_LOSS_LIMIT:
            self.trigger_breaker("DAILY_LOSS_LIMIT", f"Loss: ₹{current_pnl:,.2f}")
            return False
        return True
    
    def record_trade_result(self, pnl) -> bool:
        if pnl < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= ProductionConfig.MAX_CONSECUTIVE_LOSSES:
                self.trigger_breaker("CONSECUTIVE_LOSSES", f"{self.consecutive_losses} losses")
                return False
        else:
            self.consecutive_losses = 0
        return True
    
    def trigger_breaker(self, reason, details):
        self.breaker_triggered = True
        self.breaker_until = datetime.now() + timedelta(seconds=ProductionConfig.COOL_DOWN_PERIOD)
        telegram.send(f"🔴 *CIRCUIT BREAKER*\n{reason}: {details}", "CRITICAL")
        db.log_risk_event("CIRCUIT_BREAKER", "CRITICAL", reason, details)
        logger.critical(f"CIRCUIT BREAKER: {reason}")
    
    def is_active(self) -> bool:
        return self.breaker_triggered

circuit_breaker = CircuitBreaker()

# ==========================================
# DATA MODELS
# ==========================================
@dataclass
class TimeMetrics:
    current_date: date; weekly_exp: date; monthly_exp: date
    next_weekly_exp: date; dte_weekly: int; dte_monthly: int
    is_gamma_week: bool; is_gamma_month: bool; days_to_next_weekly: int

@dataclass
class VolMetrics:
    spot: float; vix: float; rv7: float; rv28: float; rv90: float
    garch7: float; garch28: float; park7: float; park28: float
    vov: float; vov_zscore: float; ivp_30d: float; ivp_90d: float; ivp_1yr: float
    ma20: float; atr14: float; trend_strength: float
    vol_regime: str; is_fallback: bool

@dataclass
class StructMetrics:
    net_gex: float; gex_ratio: float; total_oi_value: float
    gex_regime: str; pcr: float; max_pain: float
    skew_25d: float; oi_regime: str; lot_size: int

@dataclass
class EdgeMetrics:
    iv_weekly: float; vrp_rv_weekly: float; vrp_garch_weekly: float; vrp_park_weekly: float
    iv_monthly: float; vrp_rv_monthly: float; vrp_garch_monthly: float; vrp_park_monthly: float
    term_spread: float; term_regime: str; primary_edge: str

@dataclass
class ParticipantData:
    fut_long: float; fut_short: float; fut_net: float
    call_long: float; call_short: float; call_net: float
    put_long: float; put_short: float; put_net: float
    stock_net: float

@dataclass
class ExternalMetrics:
    fii: Optional[ParticipantData]; dii: Optional[ParticipantData]
    pro: Optional[ParticipantData]; client: Optional[ParticipantData]
    fii_net_change: float; flow_regime: str; fast_vol: bool; data_date: str

@dataclass
class RegimeScore:
    vol_score: float; struct_score: float; edge_score: float; risk_score: float
    composite: float; confidence: str

@dataclass
class TradingMandate:
    expiry_type: str; expiry_date: date; dte: int
    regime_name: str; strategy_type: str
    allocation_pct: float; max_lots: int; risk_per_lot: float
    score: RegimeScore
    rationale: List[str]; warnings: List[str]
    suggested_structure: str

# ==========================================
# SESSION MANAGER
# ==========================================
class SessionManager:
    def __init__(self):
        self.client_id = ProductionConfig.UPSTOX_CLIENT_ID
        self.client_secret = ProductionConfig.UPSTOX_CLIENT_SECRET
        self.redirect_uri = ProductionConfig.UPSTOX_REDIRECT_URI
        self.access_token = ProductionConfig.UPSTOX_ACCESS_TOKEN
        
    def validate_session(self):
        try:
            headers = {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}
            r = requests.get(f"{ProductionConfig.API_V2}/user/profile", headers=headers)
            if r.status_code == 401:
                logger.warning("Token Expired. Initiating Refresh...")
                return self._refresh_token()
            return True
        except Exception as e:
            logger.error(f"Session validation error: {e}")
            return False
            
    def _refresh_token(self):
        url = "https://api.upstox.com/v2/login/authorization/token"
        headers = {'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': self.client_id, 'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri, 'grant_type': 'refresh_token',
            'refresh_token': ProductionConfig.UPSTOX_REFRESH_TOKEN
        }
        try:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                new_token = response.json().get('access_token')
                ProductionConfig.UPSTOX_ACCESS_TOKEN = new_token
                self.access_token = new_token
                api_client.configuration.access_token = new_token
                api_client.api_client.default_headers['Authorization'] = f'Bearer {new_token}'
                api_client.session.headers.update({"Authorization": f"Bearer {new_token}"})
                logger.info("Token Refreshed Successfully")
                return True
            else:
                logger.critical(f"Token Refresh Failed: {response.text}")
                return False
        except Exception as e:
            logger.critical(f"Token Refresh Exception: {e}")
            return False

# ==========================================
# UPSTOX API CLIENT (FIXED)
# ==========================================
class UpstoxAPIClient:
    def __init__(self):
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = ProductionConfig.UPSTOX_ACCESS_TOKEN
        self.api_client = upstox_client.ApiClient(self.configuration)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {ProductionConfig.UPSTOX_ACCESS_TOKEN}",
            "Accept": "application/json",
            "Api-Version": "2.0"
        })
        self.market_streamer = None
        self.latest_prices = {}
        self.streamer_lock = threading.Lock()
    
    def is_market_open_today(self) -> bool:
        try:
            today_str = date.today().strftime("%Y-%m-%d")
            url = f"{ProductionConfig.API_V2}/market/holidays/{today_str}"
            response = self.session.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json().get('data', [])
                if data and len(data) > 0:
                    if any(h.get('holiday_type') == 'TRADING_HOLIDAY' for h in data):
                        logger.info(f"Market Closed: {data[0].get('description')}")
                        return False
            return True
        except:
            return True
    
    def check_margin_requirement(self, legs: List[Dict]) -> float:
        try:
            instruments = []
            for leg in legs:
                instruments.append({
                    "instrument_key": leg['key'], "quantity": int(leg['qty']),
                    "transaction_type": leg['side'], "product": "M" 
                })
            response = self.session.post(
                f"{ProductionConfig.API_V2}/charges/margin",
                json={"instruments": instruments}, timeout=5
            )
            if response.status_code == 200:
                return float(response.json().get('data', {}).get('required_margin', 0.0))
            return float('inf')
        except:
            return float('inf')

    def get_gtt_order_details(self, gtt_id: str) -> Optional[str]:
        try:
            api_instance = upstox_client.OrderApiV3(self.api_client)
            response = api_instance.get_gtt_order_details(gtt_order_id=gtt_id)
            if response.data:
                data = response.data[0] if isinstance(response.data, list) else response.data
                return data.status
            return None
        except Exception as e:
            logger.error(f"GTT Check Failed: {e}")
            return None

    def place_gtt_order(self, instrument_key: str, qty: int, side: str, 
                        stop_loss_price: float, target_price: float) -> Optional[str]:
        try:
            api_instance = upstox_client.OrderApiV3(self.api_client)
            rules = [
                upstox_client.GttRule(strategy="STOPLOSS", trigger_type="IMMEDIATE", trigger_price=float(stop_loss_price)),
                upstox_client.GttRule(strategy="TARGET", trigger_type="IMMEDIATE", trigger_price=float(target_price))
            ]
            body = upstox_client.GttPlaceOrderRequest(
                type="MULTIPLE", quantity=int(qty), product="D", 
                rules=rules, instrument_token=instrument_key, transaction_type=side
            )
            response = api_instance.place_gtt_order(body)
            logger.info(f"GTT PLACED: {side} {qty}x {instrument_key}")
            
            if response.data and hasattr(response.data, 'gtt_order_ids') and response.data.gtt_order_ids:
                return response.data.gtt_order_ids[0]
            return getattr(response.data, 'gtt_order_id', None)
        except Exception as e:
            logger.error(f"GTT Placement failed: {e}")
            return None
    
    def get_history(self, key: str, days: int = 400) -> pd.DataFrame:
        try:
            encoded_key = quote(key, safe='')
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            url = f"{ProductionConfig.API_V2}/historical-candle/{encoded_key}/day/{to_date}/{from_date}"
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json().get("data", {}).get("candles", [])
                if data:
                    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    return df.astype(float).sort_index()
        except:
            pass
        return pd.DataFrame()
    
    def check_socket_health(self):
        """v42.5: Heartbeat Check"""
        with self.streamer_lock:
            last_time = self.latest_prices.get('_last_update_ts', 0)
            if time.time() - last_time > 10:
                return False
            return True

    def start_market_stream(self, instruments: List[str]):
        """CRITICAL FIX: Changed to 'full' mode - CORRECT Upstox V3 mode"""
        if self.market_streamer is not None:
            return

        try:
            def on_message(message):
                with self.streamer_lock:
                    self.latest_prices['_last_update_ts'] = time.time()
                    
                    # FIX: Handle 'feeds' as Dictionary (key -> feed_data)
                    feeds = message.get('feeds', {})
                    if not isinstance(feeds, dict):
                        return
                    
                    for key, feed_data in feeds.items():
                        if not isinstance(feed_data, dict):
                            continue
                        
                        # Extract LTP - Critical for Risk Manager
                        ltp = 0
                        if 'ltpc' in feed_data:
                            ltp = feed_data['ltpc'].get('ltp', 0)
                        elif 'fullFeed' in feed_data and isinstance(feed_data['fullFeed'], dict):
                            if 'ltpc' in feed_data['fullFeed']:
                                ltp = feed_data['fullFeed']['ltpc'].get('ltp', 0)
                        
                        if ltp > 0:
                            self.latest_prices[key] = ltp
                        
                        # Extract Greeks
                        greeks = None
                        if 'optionGreeks' in feed_data:
                            greeks = feed_data['optionGreeks']
                        elif 'fullFeed' in feed_data and isinstance(feed_data['fullFeed'], dict):
                            if 'optionGreeks' in feed_data['fullFeed']:
                                greeks = feed_data['fullFeed']['optionGreeks']
                        
                        if greeks and isinstance(greeks, dict):
                            self.latest_prices[f"{key}_greeks"] = greeks
            
            def on_open(): 
                logger.info("✅ WebSocket Connected (full mode - LTP + Greeks)")
            
            def on_error(error):
                logger.error(f"WebSocket Error: {error}")
            
            # FIX: Use 'full' mode - this is the correct Upstox V3 mode
            self.market_streamer = upstox_client.MarketDataStreamerV3(
                self.api_client, instruments, mode="full"
            )
            self.market_streamer.on("message", on_message)
            self.market_streamer.on("open", on_open)
            self.market_streamer.on("error", on_error)
            
            threading.Thread(target=self.market_streamer.connect, daemon=True).start()
            time.sleep(2)
            logger.info("WebSocket initialization complete")
        except Exception as e:
            logger.error(f"WebSocket start failed: {e}")
            traceback.print_exc()
    
    def get_live_prices(self, keys: List[str]) -> Dict[str, float]:
        with self.streamer_lock:
            return {k: self.latest_prices.get(k, 0) for k in keys}
    
    def get_expiries(self) -> Tuple[Optional[date], Optional[date], Optional[date], int]:
        try:
            response = self.session.get(
                f"{ProductionConfig.API_V2}/option/contract",
                params={"instrument_key": ProductionConfig.NIFTY_KEY}, timeout=10
            )
            if response.status_code == 200:
                data = response.json().get('data', [])
                if not data:
                    return None, None, None, 0
                lot_size = next((int(c['lot_size']) for c in data if 'lot_size' in c), 0)
                expiry_dates = sorted(list(set([datetime.strptime(c['expiry'], "%Y-%m-%d").date() for c in data if c.get('expiry')])))
                valid_dates = [d for d in expiry_dates if d >= date.today()]
                if not valid_dates:
                    return None, None, None, lot_size
                weekly = valid_dates[0]
                next_weekly = valid_dates[1] if len(valid_dates) > 1 else valid_dates[0]
                current_month = date.today().month
                monthly_candidates = [d for d in valid_dates if d.month == (current_month + 1 if current_month < 12 else 1)]
                if not monthly_candidates:
                    monthly_candidates = [d for d in valid_dates if d.month == current_month]
                monthly = monthly_candidates[-1] if monthly_candidates else valid_dates[-1]
                return weekly, monthly, next_weekly, lot_size
        except:
            pass
        return None, None, None, 0
    
    def get_option_chain(self, expiry_date: date) -> pd.DataFrame:
        try:
            expiry_str = expiry_date.strftime("%Y-%m-%d")
            response = self.session.get(
                f"{ProductionConfig.API_V2}/option/chain",
                params={"instrument_key": ProductionConfig.NIFTY_KEY, "expiry_date": expiry_str},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json().get('data', [])
                return pd.DataFrame([{
                    'strike': x['strike_price'],
                    'ce_iv': x['call_options']['option_greeks']['iv'],
                    'pe_iv': x['put_options']['option_greeks']['iv'],
                    'ce_delta': x['call_options']['option_greeks']['delta'],
                    'pe_delta': x['put_options']['option_greeks']['delta'],
                    'ce_gamma': x['call_options']['option_greeks']['gamma'],
                    'pe_gamma': x['put_options']['option_greeks']['gamma'],
                    'ce_oi': x['call_options']['market_data']['oi'],
                    'pe_oi': x['put_options']['market_data']['oi'],
                    'ce_ltp': x['call_options']['market_data']['ltp'],
                    'pe_ltp': x['put_options']['market_data']['ltp'],
                    'ce_bid': x['call_options']['market_data'].get('bid_price', 0),
                    'ce_ask': x['call_options']['market_data'].get('ask_price', 0),
                    'pe_bid': x['put_options']['market_data'].get('bid_price', 0),
                    'pe_ask': x['put_options']['market_data'].get('ask_price', 0),
                    'ce_key': x['call_options']['instrument_key'],
                    'pe_key': x['put_options']['instrument_key']
                } for x in data])
        except:
            pass
        return pd.DataFrame()
    
    def place_order(self, instrument_key: str, qty: int, side: str, 
                   order_type: str = "LIMIT", price: float = 0.0) -> Optional[str]:
        try:
            api_instance = upstox_client.OrderApiV3(self.api_client)
            body = upstox_client.PlaceOrderV3Request(
                quantity=int(qty), product="M", validity="DAY", price=float(price),
                tag="VG42", instrument_token=instrument_key, order_type=order_type,
                transaction_type=side, disclosed_quantity=0, trigger_price=0.0, is_amo=False
            )
            api_response = api_instance.place_order(body)
            order_id = api_response.order_id
            logger.info(f"ORDER PLACED: {side} {qty}x {instrument_key} @ {price} | ID={order_id}")
            return order_id
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None
    
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        try:
            api_instance = upstox_client.OrderApi(self.api_client)
            response = api_instance.get_order_details(order_id=order_id)
            return {
                'status': response.status,
                'avg_price': float(response.average_price) if response.average_price else 0,
                'filled_qty': int(response.filled_quantity) if response.filled_quantity else 0,
            }
        except:
            return None
    
    def cancel_order(self, order_id: str) -> bool:
        try:
            api_instance = upstox_client.OrderApiV3(self.api_client)
            api_instance.cancel_order(order_id=order_id)
            logger.info(f"ORDER CANCELLED: {order_id}")
            return True
        except:
            return False
    
    def get_positions(self) -> List[Dict]:
        try:
            api_instance = upstox_client.PortfolioApi(self.api_client)
            positions = api_instance.get_positions().data or []
            return positions
        except:
            return []
    
    def get_funds(self) -> float:
        try:
            api_instance = upstox_client.UserApi(self.api_client)
            response = api_instance.get_user_fund_margin(segment="SEC")
            return float(response.data.equity.available_margin)
        except:
            return 0.0

api_client = UpstoxAPIClient()

# ==========================================
# NSE PARTICIPANT DATA
# ==========================================
class ParticipantDataFetcher:
    @staticmethod
    def get_trading_dates():
        tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(tz)
        dates = []
        candidate = now
        if candidate.hour < 18:
            candidate -= timedelta(days=1)
        while len(dates) < 2:
            if candidate.weekday() < 5:
                dates.append(candidate)
            candidate -= timedelta(days=1)
        return dates
    
    @staticmethod
    def fetch_oi_csv(date_obj):
        date_str = date_obj.strftime('%d%m%Y')
        url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                content = r.content.decode('utf-8')
                lines = content.splitlines()
                for idx, line in enumerate(lines[:20]):
                    if "Future Index Long" in line:
                        df = pd.read_csv(io.StringIO(content), skiprows=idx)
                        df.columns = df.columns.str.strip()
                        return df
        except:
            pass
        return None
    
    @staticmethod
    def process_participant_data(df) -> Dict[str, ParticipantData]:
        data = {}
        for p in ["FII", "DII", "Client", "Pro"]:
            try:
                row = df[df['Client Type'].astype(str).str.contains(p, case=False, na=False)].iloc[0]
                data[p] = ParticipantData(
                    fut_long=float(row['Future Index Long']),
                    fut_short=float(row['Future Index Short']),
                    fut_net=float(row['Future Index Long']) - float(row['Future Index Short']),
                    call_long=float(row['Option Index Call Long']),
                    call_short=float(row['Option Index Call Short']),
                    call_net=float(row['Option Index Call Long']) - float(row['Option Index Call Short']),
                    put_long=float(row['Option Index Put Long']),
                    put_short=float(row['Option Index Put Short']),
                    put_net=float(row['Option Index Put Long']) - float(row['Option Index Put Short']),
                    stock_net=float(row['Future Stock Long']) - float(row['Future Stock Short'])
                )
            except:
                data[p] = None
        return data
    
    @classmethod
    def fetch_participant_metrics(cls):
        dates = cls.get_trading_dates()
        today, yest = dates[0], dates[1]
        logger.info(f"Fetching participant data for {today.strftime('%d-%b-%Y')}")
        df_today = cls.fetch_oi_csv(today)
        df_yest = cls.fetch_oi_csv(yest)
        if df_today is None:
            return None, None, 0.0, today.strftime('%d-%b-%Y')
        today_data = cls.process_participant_data(df_today)
        yest_data = cls.process_participant_data(df_yest) if df_yest is not None else {}
        fii_net_change = 0.0
        if today_data.get('FII') and yest_data.get('FII'):
            fii_net_change = today_data['FII'].fut_net - yest_data['FII'].fut_net
        return today_data, yest_data, fii_net_change, today.strftime('%d-%b-%Y')

# ==========================================
# ANALYTICS ENGINE
# ==========================================
class AnalyticsEngine:
    def get_time_metrics(self, weekly, monthly, next_weekly) -> TimeMetrics:
        today = date.today()
        dte_w = (weekly - today).days
        dte_m = (monthly - today).days
        dte_nw = (next_weekly - today).days
        return TimeMetrics(
            today, weekly, monthly, next_weekly, dte_w, dte_m,
            dte_w <= ProductionConfig.GAMMA_DANGER_DTE,
            dte_m <= ProductionConfig.GAMMA_DANGER_DTE,
            dte_nw
        )
    
    def get_vol_metrics(self, nifty_hist, vix_hist, spot_live, vix_live) -> VolMetrics:
        is_fallback = False
        spot = spot_live if spot_live > 0 else (nifty_hist.iloc[-1]['close'] if not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if not vix_hist.empty else 0)
        if spot_live <= 0 or vix_live <= 0:
            is_fallback = True
        
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        rv7 = returns.rolling(7).std().iloc[-1] * np.sqrt(252) * 100
        rv28 = returns.rolling(28).std().iloc[-1] * np.sqrt(252) * 100
        rv90 = returns.rolling(90).std().iloc[-1] * np.sqrt(252) * 100
        
        def fit_garch(horizon):
            try:
                if len(returns) < 100:
                    return 0
                model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='normal')
                result = model.fit(disp='off', show_warning=False)
                forecast = result.forecast(horizon=horizon, reindex=False)
                return np.sqrt(forecast.variance.values[-1, -1]) * np.sqrt(252)
            except:
                return 0
        
        garch7 = fit_garch(7) or rv7
        garch28 = fit_garch(28) or rv28
        
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100
        park28 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100
        
        vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
        vov = vix_returns.rolling(30).std().iloc[-1] * np.sqrt(252) * 100
        vov_rolling = vix_returns.rolling(30).std() * np.sqrt(252) * 100
        vov_mean = vov_rolling.rolling(60).mean().iloc[-1]
        vov_std = vov_rolling.rolling(60).std().iloc[-1]
        vov_zscore = (vov - vov_mean) / vov_std if vov_std > 0 else 0
        
        def calc_ivp(window):
            if len(vix_hist) < window:
                return 0.0
            history = vix_hist['close'].tail(window)
            return (history < vix).mean() * 100
        
        ivp_30d, ivp_90d, ivp_1yr = calc_ivp(30), calc_ivp(90), calc_ivp(252)
        
        ma20 = nifty_hist['close'].rolling(20).mean().iloc[-1]
        true_range = pd.concat([
            nifty_hist['high']-nifty_hist['low'],
            (nifty_hist['high']-nifty_hist['close'].shift(1)).abs(),
            (nifty_hist['low']-nifty_hist['close'].shift(1)).abs()
        ], axis=1).max(axis=1)
        atr14 = true_range.rolling(14).mean().iloc[-1]
        trend_strength = abs(spot - ma20) / atr14 if atr14 > 0 else 0
        
        vol_regime = "EXPLODING" if vov_zscore > ProductionConfig.VOV_CRASH_ZSCORE else \
                     "RICH" if ivp_1yr > ProductionConfig.HIGH_VOL_IVP else \
                     "CHEAP" if ivp_1yr < ProductionConfig.LOW_VOL_IVP else "FAIR"
        
        return VolMetrics(
            spot, vix, rv7, rv28, rv90, garch7, garch28, park7, park28,
            vov, vov_zscore, ivp_30d, ivp_90d, ivp_1yr, ma20, atr14,
            trend_strength, vol_regime, is_fallback
        )
    
    def get_struct_metrics(self, chain, spot, lot_size) -> StructMetrics:
        if chain.empty or spot == 0:
            return StructMetrics(0, 0, 0, "NEUTRAL", 0, 0, 0, "NEUTRAL", lot_size)
        subset = chain[(chain['strike'] > spot * 0.90) & (chain['strike'] < spot * 1.10)]
        net_gex = ((subset['ce_gamma'] * subset['ce_oi']).sum() - (subset['pe_gamma'] * subset['pe_oi']).sum()) * spot * lot_size
        total_oi_value = (chain['ce_oi'].sum() + chain['pe_oi'].sum()) * spot * lot_size
        gex_ratio = abs(net_gex) / total_oi_value if total_oi_value > 0 else 0
        gex_regime = "STICKY" if gex_ratio > ProductionConfig.GEX_STICKY_RATIO else "SLIPPERY" if gex_ratio < ProductionConfig.GEX_STICKY_RATIO * 0.5 else "NEUTRAL"
        pcr = chain['pe_oi'].sum() / chain['ce_oi'].sum() if chain['ce_oi'].sum() > 0 else 1.0
        
        strikes = chain['strike'].values
        losses = [np.sum(np.maximum(0, s - strikes) * chain['ce_oi'].values) + np.sum(np.maximum(0, strikes - s) * chain['pe_oi'].values) for s in strikes]
        max_pain = strikes[np.argmin(losses)] if losses else 0
        
        try:
            ce_25d_idx = (chain['ce_delta'].abs() - 0.25).abs().argsort()[:1]
            pe_25d_idx = (chain['pe_delta'].abs() - 0.25).abs().argsort()[:1]
            skew_25d = chain.iloc[pe_25d_idx]['pe_iv'].values[0] - chain.iloc[ce_25d_idx]['ce_iv'].values[0]
        except:
            skew_25d = 0
        
        oi_regime = "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL"
        return StructMetrics(net_gex, gex_ratio, total_oi_value, gex_regime, pcr, max_pain, skew_25d, oi_regime, lot_size)
    
    def get_edge_metrics(self, weekly_chain, monthly_chain, spot, vol: VolMetrics) -> EdgeMetrics:
        def get_atm_iv(chain):
            if chain.empty or spot == 0:
                return 0
            atm_idx = (chain['strike'] - spot).abs().argsort()[:1]
            row = chain.iloc[atm_idx].iloc[0]
            return (row['ce_iv'] + row['pe_iv']) / 2
        
        iv_weekly = get_atm_iv(weekly_chain)
        iv_monthly = get_atm_iv(monthly_chain)
        
        vrp_rv_weekly = iv_weekly - vol.rv7
        vrp_garch_weekly = iv_weekly - vol.garch7
        vrp_park_weekly = iv_weekly - vol.park7
        vrp_rv_monthly = iv_monthly - vol.rv28
        vrp_garch_monthly = iv_monthly - vol.garch28
        vrp_park_monthly = iv_monthly - vol.park28
        term_spread = iv_monthly - iv_weekly
        term_regime = "BACKWARDATION" if term_spread < -1.0 else "CONTANGO" if term_spread > 1.0 else "FLAT"
        primary_edge = "LONG_VOL" if vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP else \
                      "SHORT_GAMMA" if vrp_park_weekly > 4.0 and vol.ivp_1yr > 50 else \
                      "SHORT_VEGA" if vrp_park_monthly > 3.0 and vol.ivp_1yr > 50 else \
                      "CALENDAR_SPREAD" if term_regime == "BACKWARDATION" and term_spread < -2.0 else \
                      "MEAN_REVERSION" if vol.ivp_1yr > ProductionConfig.HIGH_VOL_IVP else "NONE"
        
        return EdgeMetrics(
            iv_weekly, vrp_rv_weekly, vrp_garch_weekly, vrp_park_weekly,
            iv_monthly, vrp_rv_monthly, vrp_garch_monthly, vrp_park_monthly,
            term_spread, term_regime, primary_edge
        )
    
    def get_external_metrics(self, nifty_hist, participant_data, participant_yest, fii_net_change, data_date) -> ExternalMetrics:
        fast_vol = False
        if not nifty_hist.empty:
            last_bar = nifty_hist.iloc[-1]
            daily_range_pct = ((last_bar['high'] - last_bar['low']) / last_bar['open']) * 100
            fast_vol = daily_range_pct > 1.8
        
        flow_regime = "NEUTRAL"
        if participant_data and participant_data.get('FII'):
            fii_net = participant_data['FII'].fut_net
            if fii_net > ProductionConfig.FII_STRONG_LONG:
                flow_regime = "STRONG_LONG"
            elif fii_net < ProductionConfig.FII_STRONG_SHORT:
                flow_regime = "STRONG_SHORT"
            elif abs(fii_net) > ProductionConfig.FII_MODERATE:
                flow_regime = "MODERATE_LONG" if fii_net > 0 else "MODERATE_SHORT"
        
        return ExternalMetrics(
            fii=participant_data.get('FII') if participant_data else None,
            dii=participant_data.get('DII') if participant_data else None,
            pro=participant_data.get('Pro') if participant_data else None,
            client=participant_data.get('Client') if participant_data else None,
            fii_net_change=fii_net_change, flow_regime=flow_regime, fast_vol=fast_vol, data_date=data_date
        )

# ==========================================
# REGIME ENGINE
# ==========================================
class RegimeEngine:
    def calculate_scores(self, vol: VolMetrics, struct: StructMetrics, edge: EdgeMetrics,
                        external: ExternalMetrics, time: TimeMetrics, expiry_type: str) -> RegimeScore:
        
        if expiry_type == "WEEKLY":
            garch_val = edge.vrp_garch_weekly
            park_val = edge.vrp_park_weekly
            rv_val = edge.vrp_rv_weekly
        else:
            garch_val = edge.vrp_garch_monthly
            park_val = edge.vrp_park_monthly
            rv_val = edge.vrp_rv_monthly
        
        weighted_vrp = (garch_val * 0.70) + (park_val * 0.15) + (rv_val * 0.15)
        edge_score = 5.0
        
        if weighted_vrp > 4.0:
            edge_score += 3.0
        elif weighted_vrp > 2.0:
            edge_score += 2.0
        elif weighted_vrp > 1.0:
            edge_score += 1.0
        elif weighted_vrp < 0:
            edge_score -= 3.0
        
        if edge.term_regime == "BACKWARDATION" and edge.term_spread < -2.0:
            edge_score += 1.0
        elif edge.term_regime == "CONTANGO":
            edge_score += 0.5
        edge_score = max(0, min(10, edge_score))
        
        vol_score = 5.0
        if vol.vov_zscore > ProductionConfig.VOV_CRASH_ZSCORE:
            vol_score = 0.0
        elif vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE:
            vol_score -= 3.0
        elif vol.vov_zscore < 1.5:
            vol_score += 1.5
        if vol.ivp_1yr > ProductionConfig.HIGH_VOL_IVP:
            vol_score += 0.5
        elif vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP:
            vol_score -= 2.5
        else:
            vol_score += 1.0
        vol_score = max(0, min(10, vol_score))
        
        struct_score = 5.0
        if struct.gex_regime == "STICKY":
            struct_score += 2.5 if expiry_type == "WEEKLY" and time.dte_weekly <= 1 else 1.0
        elif struct.gex_regime == "SLIPPERY":
            struct_score -= 1.0
        if 0.9 < struct.pcr < 1.1:
            struct_score += 1.0
        elif struct.pcr > 1.3 or struct.pcr < 0.7:
            struct_score -= 0.5
        if abs(struct.skew_25d) > 3.0:
            struct_score -= 0.5
        struct_score = max(0, min(10, struct_score))
        
        risk_score = 10.0
        if external.fast_vol:
            risk_score -= 2.0
        if external.flow_regime == "STRONG_SHORT":
            risk_score -= 3.0
        elif external.flow_regime == "STRONG_LONG":
            risk_score += 1.0
        if expiry_type == "WEEKLY" and time.is_gamma_week:
            risk_score -= 2.0
        elif expiry_type == "MONTHLY" and time.is_gamma_month:
            risk_score -= 2.5
        risk_score = max(0, min(10, risk_score))
        
        composite = (vol_score * ProductionConfig.WEIGHT_VOL +
                     struct_score * ProductionConfig.WEIGHT_STRUCT +
                     edge_score * ProductionConfig.WEIGHT_EDGE +
                     risk_score * ProductionConfig.WEIGHT_RISK)
        confidence = "VERY_HIGH" if composite >= 8.0 else "HIGH" if composite >= 6.5 else "MODERATE" if composite >= 4.0 else "LOW"
        return RegimeScore(vol_score, struct_score, edge_score, risk_score, composite, confidence)
    
    def generate_mandate(self, score: RegimeScore, vol: VolMetrics, struct: StructMetrics,
                        edge: EdgeMetrics, external: ExternalMetrics, time: TimeMetrics,
                        expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        rationale = []
        warnings = []
        
        if expiry_type == "WEEKLY":
            w_vrp = (edge.vrp_garch_weekly * 0.7) + (edge.vrp_park_weekly * 0.15) + (edge.vrp_rv_weekly * 0.15)
        else:
            w_vrp = (edge.vrp_garch_monthly * 0.7) + (edge.vrp_park_monthly * 0.15) + (edge.vrp_rv_monthly * 0.15)
        
        if score.composite >= 7.5:
            regime_name = "AGGRESSIVE_SHORT"; allocation = 60.0; strategy = "AGGRESSIVE_SHORT"; suggested = "STRANGLE"
            rationale.append(f"High Confidence ({score.confidence}): Weighted VRP {w_vrp:.2f} is strong")
        elif score.composite >= 6.0:
            regime_name = "MODERATE_SHORT"; allocation = 40.0; strategy = "MODERATE_SHORT"; suggested = "IRON_CONDOR" if dte > 1 else "IRON_FLY"
            rationale.append(f"Moderate Confidence: VRP {w_vrp:.2f} is positive")
        elif score.composite >= 4.0:
            regime_name = "DEFENSIVE"; allocation = 20.0; strategy = "DEFENSIVE"; suggested = "CREDIT_SPREAD"
            rationale.append("Defensive Posture: Focus on defined risk only")
        else:
            regime_name = "CASH"; allocation = 0.0; strategy = "CASH"; suggested = "NONE"
            rationale.append("Regime Unfavorable: Cash is a position")
        
        if vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE:
            warnings.append(f"⚠️ HIGH VOL-OF-VOL ({vol.vov_zscore:.2f}σ)")
        if external.flow_regime == "STRONG_SHORT" and external.fii:
            warnings.append(f"⚠️ FII DUMPING")
            allocation = min(allocation, 30.0)
        if dte <= ProductionConfig.GAMMA_DANGER_DTE and expiry_type == "WEEKLY":
            warnings.append(f"⚠️ GAMMA RISK")
            allocation *= 0.5
        
        deployable = ProductionConfig.BASE_CAPITAL * (allocation / 100.0)
        risk_per_lot = ProductionConfig.MARGIN_SELL_BASE if strategy != "DEFENSIVE" else ProductionConfig.MARGIN_SELL_BASE * 0.6
        max_lots = int(deployable / risk_per_lot) if risk_per_lot > 0 else 0
        
        return TradingMandate(
            expiry_type, expiry_date, dte, regime_name, strategy,
            allocation, max_lots, risk_per_lot, score, rationale, warnings, suggested
        )

# ==========================================
# STRATEGY FACTORY
# ==========================================
class StrategyFactory:
    def __init__(self, api: UpstoxAPIClient):
        self.api = api
    
    def find_leg(self, df, type_, target_delta):
        target = abs(target_delta)
        col_delta = f"{type_.lower()}_delta"
        df['delta_diff'] = (df[col_delta].abs() - target).abs()
        candidates = df.sort_values('delta_diff')
        
        for _, row in candidates.iterrows():
            bid = row[f'{type_.lower()}_bid']
            ask = row[f'{type_.lower()}_ask']
            ltp = row[f'{type_.lower()}_ltp']
            if ltp <= 0 or ask <= 0:
                continue
            
            spread = (ask - bid) / ltp
            if spread > ProductionConfig.MAX_BID_ASK_SPREAD:
                logger.warning(f"Skipping {type_} {row['strike']}: Spread {spread*100:.1f}%")
                continue
            
            return {
                'key': row[f'{type_.lower()}_key'],
                'strike': row['strike'],
                'ltp': ltp,
                'delta': row[col_delta],
                'type': type_
            }
        
        logger.critical(f"NO LIQUID STRIKES FOUND for {type_}. Aborting.")
        telegram.send(f"⚠️ LIQUIDITY DRY: No {type_} strikes within limit.", "CRITICAL")
        return None
    
    def generate(self, mandate: TradingMandate, chain: pd.DataFrame, lot_size: int) -> List[Dict]:
        if mandate.max_lots == 0 or chain.empty:
            return []
        
        qty = mandate.max_lots * lot_size
        legs = []
        
        def get_safe_leg(type_, delta, side, role):
            l = self.find_leg(chain, type_, delta)
            if l:
                return {**l, 'side': side, 'role': role, 'qty': qty}
            return None

        if mandate.suggested_structure == "IRON_CONDOR":
            legs = [
                get_safe_leg('CE', 0.05, 'BUY', 'HEDGE'),
                get_safe_leg('PE', 0.05, 'BUY', 'HEDGE'),
                get_safe_leg('CE', 0.20, 'SELL', 'CORE'),
                get_safe_leg('PE', 0.20, 'SELL', 'CORE')
            ]
        elif mandate.suggested_structure == "STRANGLE":
            legs = [
                get_safe_leg('CE', 0.25, 'SELL', 'CORE'),
                get_safe_leg('PE', 0.25, 'SELL', 'CORE')
            ]
        elif mandate.suggested_structure == "CREDIT_SPREAD":
            legs = [
                get_safe_leg('PE', 0.10, 'BUY', 'HEDGE'),
                get_safe_leg('PE', 0.25, 'SELL', 'CORE')
            ]
        
        if any(l is None for l in legs):
            logger.error("Strategy Generation Failed: One or more legs illiquid.")
            return []
            
        return legs

# ==========================================
# EXECUTION ENGINE
# ==========================================
class ExecutionEngine:
    def __init__(self, api: UpstoxAPIClient):
        self.api = api
    
    def _execute_leg_atomic(self, leg):
        bias = 1.002 if leg['side'] == 'BUY' else 0.998
        limit_price = round(leg['ltp'] * bias, 1)
        expected_price = leg['ltp']
        logger.info(f"PLACING {leg['side']} {leg['strike']} @ {limit_price}")
        order_id = self.api.place_order(leg['key'], leg['qty'], leg['side'], "LIMIT", limit_price)
        
        if not order_id:
            return None
        
        start = time.time()
        while (time.time() - start) < ProductionConfig.ORDER_TIMEOUT:
            status = self.api.get_order_status(order_id)
            if not status:
                time.sleep(0.5)
                continue
            
            if status['status'] == 'complete':
                if status['filled_qty'] < leg['qty'] * ProductionConfig.PARTIAL_FILL_TOLERANCE:
                    logger.critical(f"PARTIAL FILL: {status['filled_qty']}/{leg['qty']}")
                    return None
                actual_price = status['avg_price']
                slippage = abs(actual_price - expected_price) / expected_price
                if slippage > ProductionConfig.SLIPPAGE_TOLERANCE:
                    logger.critical(f"SLIPPAGE BREACH: {slippage*100:.2f}%")
                leg['entry_price'] = actual_price
                leg['filled_qty'] = status['filled_qty']
                return leg
            elif status['status'] in ['rejected', 'cancelled']:
                logger.error(f"ORDER DEAD: {status['status']}")
                return None
            time.sleep(0.5)
        
        logger.warning(f"TIMEOUT on {order_id}. Cancelling...")
        self.api.cancel_order(order_id)
        time.sleep(1)
        final = self.api.get_order_status(order_id)
        if final and final['status'] == 'complete':
            leg['entry_price'] = final['avg_price']
            leg['filled_qty'] = final['filled_qty']
            return leg
        return None
    
    def execute_strategy(self, legs: List[Dict]) -> List[Dict]:
        executed = []
        required_margin = self.api.check_margin_requirement(legs)
        available_funds = self.api.get_funds()
        if required_margin > available_funds:
            logger.critical(f"MARGIN ERROR: Need {required_margin}, Have {available_funds}")
            telegram.send(f"Margin Shortfall: Need {required_margin/100000:.2f}L", "ERROR")
            return []

        hedges = [l for l in legs if l['role'] == 'HEDGE']
        logger.info(f"Executing {len(hedges)} Hedges in Parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(hedges) or 1) as executor:
            future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in hedges}
            hedge_failed = False
            for future in concurrent.futures.as_completed(future_to_leg):
                if not future.result():
                    hedge_failed = True
                else:
                    executed.append(future.result())
        
        if hedge_failed or len(executed) != len(hedges):
            logger.critical("Aborting before Cores. Flattening Hedges.")
            self.flatten(executed)
            return []

        cores = [l for l in legs if l['role'] == 'CORE']
        logger.info(f"Hedges Filled. Executing {len(cores)} Cores in Parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(cores) or 1) as executor:
            future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in cores}
            core_failed = False
            for future in concurrent.futures.as_completed(future_to_leg):
                if not future.result():
                    core_failed = True
                else:
                    executed.append(future.result())

        if core_failed:
            self.flatten(executed)
            return []

        logger.info(f"STRATEGY DEPLOYED: {len(executed)} legs")
        telegram.send(f"Position opened: {len(executed)} legs (Parallel)", "TRADE")
        return executed
    
    def verify_gtt(self, gtt_ids: List[str]):
        if not gtt_ids:
            return
        logger.info(f"Verifying {len(gtt_ids)} GTT Orders...")
        verified = 0
        for gid in gtt_ids:
            status = api_client.get_gtt_order_details(gid)
            if status == 'active':
                verified += 1
            else:
                logger.critical(f"GTT {gid} FAILURE: {status}")
                telegram.send(f"GTT FAILURE: {gid} is {status}", "CRITICAL")
        if verified == len(gtt_ids):
            telegram.send(f"Shield Up: {verified} GTTs Active", "INFO")

    def flatten(self, legs):
        logger.critical("🚨 EMERGENCY FLATTEN")
        telegram.send("Emergency position flatten", "CRITICAL")
        for leg in legs:
            side = 'SELL' if leg['side'] == 'BUY' else 'BUY'
            self.api.place_order(leg['key'], leg['filled_qty'], side, "MARKET", 0)

# ==========================================
# RISK MANAGER (FIXED)
# ==========================================
class RiskManager:
    def __init__(self, api: UpstoxAPIClient, legs, expiry_date):
        self.api = api
        self.legs = legs
        self.expiry = expiry_date
        self.running = True
        credit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'SELL')
        debit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'BUY')
        self.net_premium = credit - debit
        self.max_spread_loss = self.net_premium * 2
    
    def monitor(self):
        while self.running:
            try:
                self._update_dashboard_state()
                if (self.expiry - date.today()).days <= ProductionConfig.EXIT_DTE:
                    self.flatten_all("DTE_EXIT")
                    return
                
                keys = [l['key'] for l in self.legs]
                prices = self.api.get_live_prices(keys)
                current_pnl = 0
                for leg in self.legs:
                    ltp = prices.get(leg['key'], leg['entry_price'])
                    leg['current_ltp'] = ltp
                    pnl = (leg['entry_price'] - ltp) * leg['filled_qty'] if leg['side'] == 'SELL' else (ltp - leg['entry_price']) * leg['filled_qty']
                    current_pnl += pnl
                
                if current_pnl < -(self.net_premium * ProductionConfig.STOP_LOSS_PCT):
                    self.flatten_all("STOP_LOSS_PREMIUM")
                    return
                if self.max_spread_loss > 0 and current_pnl < -(self.max_spread_loss * 0.8):
                    self.flatten_all("STOP_LOSS_MAX_RISK")
                    return
                if self.net_premium > 0 and current_pnl >= (self.net_premium * ProductionConfig.TARGET_PROFIT_PCT):
                    self.flatten_all("TARGET_PROFIT")
                    return
                
                time.sleep(ProductionConfig.POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Risk manager error: {e}")
                time.sleep(5)
    
    def _update_dashboard_state(self):
        p_delta = p_theta = p_gamma = current_pnl = 0.0
        keys = [l['key'] for l in self.legs]
        prices = self.api.get_live_prices(keys + [f"{k}_greeks" for k in keys])
        for leg in self.legs:
            ltp = prices.get(leg['key'], leg['entry_price'])
            direction = -1 if leg['side'] == 'SELL' else 1
            current_pnl += ((leg['entry_price'] - ltp) if leg['side'] == 'SELL' else (ltp - leg['entry_price'])) * leg['filled_qty']
            greeks = prices.get(f"{leg['key']}_greeks")
            if greeks and isinstance(greeks, dict):
                p_delta += (greeks.get('delta', 0) * leg['filled_qty'] * direction)
                p_theta += (greeks.get('theta', 0) * leg['filled_qty'] * direction)
                p_gamma += (greeks.get('gamma', 0) * leg['filled_qty'] * direction)
        
        db.set_state("live_portfolio", json.dumps({
            "pnl": round(current_pnl, 2), "net_delta": round(p_delta, 2),
            "net_theta": round(p_theta, 2), "net_gamma": round(p_gamma, 5),
            "updated_at": datetime.now().strftime("%H:%M:%S")
        }))
    
    def flatten_all(self, reason="SIGNAL"):
        logger.critical(f"GLOBAL FLATTEN: {reason}")
        telegram.send(f"🚨 EMERGENCY FLATTEN: {reason}", "CRITICAL")
        pending_exits = [l for l in self.legs if l['filled_qty'] > 0]
        
        for leg in pending_exits:
            retry_count = 0
            max_retries = 3
            success = False
            while retry_count < max_retries and not success:
                try:
                    side = 'SELL' if leg['side'] == 'BUY' else 'BUY'
                    oid = self.api.place_order(leg['key'], leg['filled_qty'], side, "MARKET", 0)
                    if oid:
                        for _ in range(5):
                            status = self.api.get_order_status(oid)
                            if status and status['status'] == 'complete':
                                logger.info(f"✅ Exited {leg['key']}")
                                success = True
                                break
                            elif status and status['status'] in ['rejected', 'cancelled']:
                                logger.error(f"Exit Rejected: {status.get('message')}")
                                break
                            time.sleep(0.5)
                    if not success:
                        raise Exception("Order not confirmed")
                except Exception as e:
                    logger.error(f"Exit failed {leg['key']}: {e}")
                    retry_count += 1
                    time.sleep(0.5)
            if not success:
                msg = f"CRITICAL: FAILED TO CLOSE {leg['key']}. MANUALLY EXIT NOW!"
                logger.critical(msg)
                telegram.send(msg, "CRITICAL")
        self.running = False

# ==========================================
# TRADING ORCHESTRATOR
# ==========================================
class TradingOrchestrator:
    def __init__(self):
        self.api = api_client
        self.analytics = AnalyticsEngine()
        self.regime = RegimeEngine()
        self.factory = StrategyFactory(self.api)
        self.executor = ExecutionEngine(self.api)
        self.last_analysis = None
    
    def run_analysis(self) -> Optional[Dict]:
        logger.info("Starting market analysis...")
        self.api.start_market_stream([ProductionConfig.NIFTY_KEY, ProductionConfig.VIX_KEY])
        
        participant_data, participant_yest, fii_net_change, data_date = ParticipantDataFetcher.fetch_participant_metrics()
        nifty_hist = self.api.get_history(ProductionConfig.NIFTY_KEY)
        vix_hist = self.api.get_history(ProductionConfig.VIX_KEY)
        
        if nifty_hist.empty:
            logger.error("Failed to fetch historical data")
            return None
        
        live_prices = self.api.get_live_prices([ProductionConfig.NIFTY_KEY, ProductionConfig.VIX_KEY])
        weekly, monthly, next_weekly, lot_size = self.api.get_expiries()
        if not weekly or not monthly:
            logger.error("Failed to fetch expiries")
            return None
        
        weekly_chain = self.api.get_option_chain(weekly)
        monthly_chain = self.api.get_option_chain(monthly)
        if weekly_chain.empty and monthly_chain.empty:
            logger.error("Failed to fetch option chains")
            return None
        
        time_metrics = self.analytics.get_time_metrics(weekly, monthly, next_weekly)
        vol_metrics = self.analytics.get_vol_metrics(
            nifty_hist, vix_hist, live_prices.get(ProductionConfig.NIFTY_KEY, 0), live_prices.get(ProductionConfig.VIX_KEY, 0)
        )
        
        struct_metrics_weekly = self.analytics.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
        struct_metrics_monthly = self.analytics.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
        edge_metrics = self.analytics.get_edge_metrics(weekly_chain, monthly_chain, vol_metrics.spot, vol_metrics)
        external_metrics = self.analytics.get_external_metrics(nifty_hist, participant_data, participant_yest, fii_net_change, data_date)
        
        weekly_score = self.regime.calculate_scores(vol_metrics, struct_metrics_weekly, edge_metrics, external_metrics, time_metrics, "WEEKLY")
        monthly_score = self.regime.calculate_scores(vol_metrics, struct_metrics_monthly, edge_metrics, external_metrics, time_metrics, "MONTHLY")
        
        weekly_mandate = self.regime.generate_mandate(weekly_score, vol_metrics, struct_metrics_weekly, edge_metrics, external_metrics, time_metrics, "WEEKLY", weekly, time_metrics.dte_weekly)
        monthly_mandate = self.regime.generate_mandate(monthly_score, vol_metrics, struct_metrics_monthly, edge_metrics, external_metrics, time_metrics, "MONTHLY", monthly, time_metrics.dte_monthly)
        
        self.last_analysis = {
            'timestamp': datetime.now(),
            'time_metrics': time_metrics, 'vol_metrics': vol_metrics,
            'weekly_mandate': weekly_mandate, 'monthly_mandate': monthly_mandate,
            'weekly_chain': weekly_chain, 'monthly_chain': monthly_chain, 'lot_size': lot_size
        }
        logger.info(f"Analysis complete - Weekly: {weekly_score.composite:.2f}, Monthly: {monthly_score.composite:.2f}")
        return self.last_analysis
    
    def execute_best_mandate(self, analysis: Dict) -> Optional[str]:
        weekly_mandate = analysis['weekly_mandate']
        monthly_mandate = analysis['monthly_mandate']
        mandate = weekly_mandate if weekly_mandate.score.composite > monthly_mandate.score.composite else monthly_mandate
        chain = analysis['weekly_chain'] if mandate == weekly_mandate else analysis['monthly_chain']
        
        if mandate.max_lots == 0:
            logger.info(f"Mandate is CASH - no trade")
            return None
        if circuit_breaker.is_active():
            logger.warning("Circuit breaker active - skipping trade")
            return None
        
        legs = self.factory.generate(mandate, chain, analysis['lot_size'])
        if not legs:
            logger.error("Failed to generate legs")
            return None
        
        filled_legs = self.executor.execute_strategy(legs)
        if not filled_legs:
            logger.error("Execution failed")
            return None
        
        trade_id = f"VG42_{int(datetime.now().timestamp())}"
        entry_premium = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'SELL')
        db.save_trade(trade_id, mandate.strategy_type, mandate.expiry_date, filled_legs, entry_premium, mandate.risk_per_lot * mandate.max_lots)
        
        short_legs = [l for l in filled_legs if l['side'] == 'SELL']
        gtt_ids = []
        for leg in short_legs:
            sl_price = leg['entry_price'] * (1 + mandate.risk_per_lot/10000)
            target_price = leg['entry_price'] * 0.20
            gid = self.api.place_gtt_order(leg['key'], leg['filled_qty'], 'BUY', round(sl_price, 1), round(target_price, 1))
            if gid:
                gtt_ids.append(gid)
        self.executor.verify_gtt(gtt_ids)
        
        risk_manager = RiskManager(self.api, filled_legs, mandate.expiry_date)
        threading.Thread(target=risk_manager.monitor, daemon=True).start()
        logger.info(f"Trade opened: {trade_id}")
        return trade_id
    
    def run_auto_mode(self):
        logger.info("Starting AUTO MODE")
        telegram.send("Auto-trading activated", "SYSTEM")
        session = SessionManager()
        
        if not session.validate_session():
            logger.critical("Session Invalid. Stopping.")
            return
        if not self.api.is_market_open_today():
            logger.info("Today is a Trading Holiday.")
            telegram.send("Market Closed (Holiday).", "SYSTEM")
            return
        
        while True:
            try:
                loop_start_time = time.time()
                if not session.validate_session():
                    logger.critical("Session Invalid.")
                    break
                now = datetime.now()
                if now.weekday() >= 5:
                    logger.info("Weekend")
                    time.sleep(3600)
                    continue
                if not (ProductionConfig.SAFE_ENTRY_START <= (now.hour, now.minute) <= ProductionConfig.SAFE_EXIT_END):
                    logger.debug("Outside trading hours")
                    time.sleep(600)
                    continue
                
                if not self.api.check_socket_health():
                    logger.warning("WebSocket Stale. Restarting...")
                    self.api.market_streamer = None
                    self.api.start_market_stream([ProductionConfig.NIFTY_KEY, ProductionConfig.VIX_KEY])

                positions = self.api.get_positions()
                # FIX: Check if positions list has any open positions properly
                has_open_positions = False
                if positions and len(positions) > 0:
                    for p in positions:
                        qty = int(p.get('quantity', 0)) if hasattr(p, 'get') else int(getattr(p, 'quantity', 0))
                        if qty != 0:
                            has_open_positions = True
                            break
                
                if has_open_positions:
                    logger.debug("Positions already open")
                    time.sleep(60)
                    continue
                
                if self.last_analysis:
                    age = (datetime.now() - self.last_analysis['timestamp']).seconds
                    if age < ProductionConfig.ANALYSIS_INTERVAL:
                        logger.debug(f"Analysis fresh ({age}s)")
                        time.sleep(60)
                        continue
                
                analysis = self.run_analysis()
                if not analysis:
                    logger.warning("Analysis failed")
                    time.sleep(600)
                    continue
                
                trade_id = self.execute_best_mandate(analysis)
                if trade_id:
                    logger.info(f"Trade {trade_id} opened")
                    while True:
                        positions = self.api.get_positions()
                        all_closed = True
                        if positions and len(positions) > 0:
                            for p in positions:
                                qty = int(p.get('quantity', 0)) if hasattr(p, 'get') else int(getattr(p, 'quantity', 0))
                                if qty != 0:
                                    all_closed = False
                                    break
                        if all_closed:
                            break
                        time.sleep(60)
                else:
                    logger.info("No trade executed")
                    time.sleep(ProductionConfig.ANALYSIS_INTERVAL)
                
                db.update_system_vitals((time.time()-loop_start_time)*1000, psutil.cpu_percent(interval=0.1), psutil.virtual_memory().percent)
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Auto-trading error: {e}")
                traceback.print_exc()
                telegram.send(f"Error: {str(e)}", "ERROR")
                time.sleep(300)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="VOLGUARD v42.5 - FIXED")
    parser.add_argument('--mode', choices=['analysis', 'auto'], default='analysis')
    args = parser.parse_args()
    
    try:
        ProductionConfig.validate()
        logger.info("Configuration validated")
    except Exception as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)
    
    db.set_state("system_version", "42.5-FIXED")
    logger.info("Database initialized (WAL Mode enabled)")
    telegram.send("System startup successful (v42.5 - WebSocket + DataFrame FIXED)", "SUCCESS")
    orchestrator = TradingOrchestrator()
    
    try:
        if args.mode == 'analysis':
            logger.info("Running ANALYSIS mode")
            result = orchestrator.run_analysis()
            if result:
                logger.info("=" * 80)
                logger.info(f"Weekly Mandate: {result['weekly_mandate'].regime_name}")
                logger.info(f"Monthly Mandate: {result['monthly_mandate'].regime_name}")
                logger.info("=" * 80)
        elif args.mode == 'auto':
            bypass_confirm = os.getenv("VG_AUTO_CONFIRM", "FALSE")
            if bypass_confirm == "TRUE":
                logger.warning("⚠️ AUTO CONFIRMATION VIA ENV VAR - LIVE TRADING ENABLED")
                telegram.send("Auto-trading starting (Docker mode)", "SYSTEM")
                orchestrator.run_auto_mode()
            else:
                print("⚠️  AUTO MODE REQUESTED")
                try:
                    if input("Type 'I ACCEPT THE RISK' to continue: ") == "I ACCEPT THE RISK":
                        orchestrator.run_auto_mode()
                    else:
                        logger.info("Auto mode cancelled")
                except EOFError:
                    logger.critical("No input available. Set VG_AUTO_CONFIRM=TRUE")
                    sys.exit(1)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        traceback.print_exc()
        telegram.send(f"System crashed: {str(e)}", "CRITICAL")
        sys.exit(1)
    finally:
        logger.info("System shutdown")
        telegram.send("System shutdown", "SYSTEM")

if __name__ == "__main__":
    main()
