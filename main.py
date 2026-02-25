"""
SPX Polymarket Monitor Bot
Monitora os futuros do E-mini S&P 500 (ES=F) e odds do Polymarket para mercados SPX Up/Down
Grava histórico no PostgreSQL para análise de divergências
"""

import asyncio
import aiohttp
import json
import os
import time
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from dataclasses import dataclass, field
from typing import Optional, List, Deque, Union
from collections import deque
from enum import Enum
import logging
import math
import yfinance as yf

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============== CONFIGURAÇÕES ==============
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))  # 1 minuto
ZSCORE_CHECK_INTERVAL = int(os.getenv("ZSCORE_CHECK_INTERVAL", "10"))  # 10 segundos para z-score
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "1200"))  # 20 minutos (salvamento no banco)
TELEGRAM_HEARTBEAT_INTERVAL = int(os.getenv("TELEGRAM_HEARTBEAT_INTERVAL", "43200"))  # 12 horas (notificação Telegram)

# Thresholds ES=F
ESF_THRESHOLD_1M = float(os.getenv("ESF_THRESHOLD_1M", "0.002"))  # 0.2% desde última medição
ESF_THRESHOLD_ACCUMULATED = float(os.getenv("ESF_THRESHOLD_ACCUMULATED", "0.005"))  # 0.5% desde último salvamento

# Thresholds Polymarket
PM_THRESHOLD_PERCENT = float(os.getenv("PM_THRESHOLD_PERCENT", "0.05"))  # 5%
PM_THRESHOLD_CENTS = float(os.getenv("PM_THRESHOLD_CENTS", "0.01"))  # 1 centavo

# Z-Score Detection Config
ZSCORE_WINDOW_SIZE = int(os.getenv("ZSCORE_WINDOW_SIZE", "16"))  # Janela de 16 ticks
ZSCORE_MIN_TICKS = int(os.getenv("ZSCORE_MIN_TICKS", "5"))  # Mínimo de 5 ticks para calcular
ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "2.0"))  # Trigger em |z| >= 2.0
ZSCORE_COOLDOWN_SECONDS = int(os.getenv("ZSCORE_COOLDOWN_SECONDS", "300"))  # 5 minutos de cooldown
ZSCORE_MIN_DELTA_POINTS = float(os.getenv("ZSCORE_MIN_DELTA_POINTS", "4.0"))
ZSCORE_MIN_WINDOW_TICKS = int(os.getenv("ZSCORE_MIN_WINDOW_TICKS", "8"))
ZSCORE_T1_DELAY_SECONDS = int(os.getenv("ZSCORE_T1_DELAY_SECONDS", "60"))  # 60 segundos para t+1

# Timezones
ET_TIMEZONE = ZoneInfo("America/New_York")
BR_TIMEZONE = ZoneInfo("America/Sao_Paulo")


# ============== ENUMS ==============
class SaveReason(Enum):
    TIME_INTERVAL = "time_interval"
    PRICE_CHANGE_1M = "price_change_1m"
    PRICE_CHANGE_ACCUMULATED = "price_change_accumulated"
    CROSSED_CLOSE = "crossed_close"
    INITIAL = "initial"


# ============== ESTRUTURAS DE DADOS ==============
@dataclass
class ESFuturesData:
    timestamp: datetime
    price: float
    previous_close: float
    above_close: bool
    change_from_close_pct: float
    save_reason: SaveReason


@dataclass
class SPXClose:
    date: datetime
    close_price: float
    source: str = "^GSPC"


@dataclass
class PolymarketOdds:
    timestamp: datetime
    market_date: datetime
    up_price: float
    down_price: float
    save_reason: str = ""


@dataclass
class ZScoreResult:
    """Resultado do cálculo de z-score com metadados estatísticos"""
    z_score: float
    price_delta: float      # na mesma unidade do z-score (delta_points)
    avg_delta: float        # média dos deltas na janela, mesma unidade
    stddev_delta: float     # desvio padrão dos deltas na janela, mesma unidade
    window_ticks: int       # quantos ticks válidos na janela (entre 5 e 16)
    metric_type: str        # 'delta_points' - usamos delta absoluto em pontos


@dataclass
class ZScoreTriggerData:
    """Dados completos de um z-score trigger para salvar no banco"""
    # Timestamps
    trigger_ts: datetime
    cycle_start_ts: datetime
    t_es_observed_ts: datetime
    es_data_ts: Optional[datetime]  # Timestamp do dado do mercado (se disponível)
    t_pm_t0_read_ts: Optional[datetime]
    
    # Preços
    spx_price: float
    z_score: float
    price_delta: float
    
    # Metadados estatísticos
    avg_delta: float
    stddev_delta: float
    window_ticks: int
    metric_type: str
    
    # Métricas de timing (ms)
    cycle_to_trigger_ms: int
    es_fetch_ms: int
    pm_t0_fetch_ms: Optional[int]
    
    # Colunas antigas (mantidas para compatibilidade)
    detect_lag_ms: int
    poly_read_lag_ms: Optional[int]
    
    # Polymarket t0
    pm_up_t0: float
    pm_dw_t0: float
    pm_source: str
    pm_snapshot_ts_t0: Optional[datetime]
    
    # Polymarket t1 (preenchido depois)
    t_pm_t1_read_ts: Optional[datetime] = None
    pm_t1_fetch_ms: Optional[int] = None
    pm_up_t1: Optional[float] = None
    pm_dw_t1: Optional[float] = None
    pm_snapshot_ts_t1: Optional[datetime] = None


# ============== DATABASE ==============
class Database:
    """Gerencia conexão e operações com PostgreSQL"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None
    
    async def connect(self):
        """Conecta ao banco de dados"""
        try:
            import asyncpg
            self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=5)
            await self._create_tables()
            logger.info("Conectado ao PostgreSQL")
        except Exception as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            self.pool = None
    
    async def close(self):
        """Fecha conexão"""
        if self.pool:
            await self.pool.close()
    
    async def _create_tables(self):
        """Cria tabelas se não existirem"""
        if not self.pool:
            return
        
        async with self.pool.acquire() as conn:
            # Tabela de futuros ES=F
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS spx_futures (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    price DECIMAL(10,2),
                    previous_close DECIMAL(10,2),
                    above_close BOOLEAN,
                    change_from_close_pct DECIMAL(8,4),
                    save_reason VARCHAR(50)
                )
            ''')
            
            # Tabela de fechamentos do SPX
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS spx_closes (
                    id SERIAL PRIMARY KEY,
                    date DATE UNIQUE,
                    close_price DECIMAL(10,2),
                    source VARCHAR(20) DEFAULT '^GSPC'
                )
            ''')
            
            # Tabela de odds Polymarket - Opens Up or Down
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS polymarket_opens (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    market_date DATE,
                    up_price DECIMAL(5,4),
                    down_price DECIMAL(5,4),
                    save_reason VARCHAR(50)
                )
            ''')
            
            # Tabela de odds Polymarket - Up or Down (fechamento)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS polymarket_closes (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    market_date DATE,
                    up_price DECIMAL(5,4),
                    down_price DECIMAL(5,4),
                    save_reason VARCHAR(50)
                )
            ''')
            
            # Índices
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_spx_futures_timestamp ON spx_futures(timestamp)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_spx_closes_date ON spx_closes(date)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_pm_opens_timestamp ON polymarket_opens(timestamp)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_pm_closes_timestamp ON polymarket_closes(timestamp)')
            
            # Tabela de z-score triggers
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS zscore_triggers (
                    id SERIAL PRIMARY KEY,
                    trigger_ts TIMESTAMPTZ NOT NULL,
                    spx_price NUMERIC(10,2),
                    z_score NUMERIC(8,4),
                    price_delta NUMERIC(10,4),
                    detect_lag_ms INTEGER,
                    poly_read_lag_ms INTEGER,
                    pm_up_t0 NUMERIC(6,4),
                    pm_dw_t0 NUMERIC(6,4),
                    pm_up_t1 NUMERIC(6,4),
                    pm_dw_t1 NUMERIC(6,4)
                )
            ''')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_zscore_trigger_ts ON zscore_triggers(trigger_ts)')
            
            # Migrar tabela zscore_triggers com novas colunas
            await self._migrate_zscore_triggers(conn)
            
            logger.info("Tabelas criadas/verificadas")
    
    async def _migrate_zscore_triggers(self, conn):
        """Adiciona colunas novas sem perder dados existentes. Idempotente."""
        migrations = [
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS cycle_start_ts TIMESTAMPTZ",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS t_es_observed_ts TIMESTAMPTZ",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS es_data_ts TIMESTAMPTZ",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS t_pm_t0_read_ts TIMESTAMPTZ",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS t_pm_t1_read_ts TIMESTAMPTZ",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS cycle_to_trigger_ms INTEGER",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS es_fetch_ms INTEGER",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS pm_t0_fetch_ms INTEGER",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS pm_t1_fetch_ms INTEGER",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS avg_delta NUMERIC(12,6)",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS stddev_delta NUMERIC(12,6)",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS window_ticks INTEGER",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS metric_type VARCHAR(20)",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS pm_source VARCHAR(50)",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS pm_snapshot_ts_t0 TIMESTAMPTZ",
            "ALTER TABLE zscore_triggers ADD COLUMN IF NOT EXISTS pm_snapshot_ts_t1 TIMESTAMPTZ",
        ]
        for sql in migrations:
            await conn.execute(sql)
    
    async def save_futures(self, data: ESFuturesData):
        """Salva registro do ES=F"""
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO spx_futures (timestamp, price, previous_close, above_close, change_from_close_pct, save_reason)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', data.timestamp, data.price, data.previous_close, data.above_close, 
                data.change_from_close_pct, data.save_reason.value)
    
    async def get_last_futures(self) -> Optional[dict]:
        """Retorna último registro salvo do ES=F"""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT price, previous_close, above_close, timestamp
                FROM spx_futures ORDER BY timestamp DESC LIMIT 1
            ''')
            if row:
                return {'price': float(row['price']), 'previous_close': float(row['previous_close']),
                        'above_close': row['above_close'], 'timestamp': row['timestamp']}
            return None
    
    async def save_spx_close(self, data: SPXClose):
        """Salva fechamento do SPX"""
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO spx_closes (date, close_price, source)
                VALUES ($1, $2, $3)
                ON CONFLICT (date) DO UPDATE SET close_price = $2, source = $3
            ''', data.date.date() if isinstance(data.date, datetime) else data.date, 
                data.close_price, data.source)
    
    async def get_latest_spx_close(self) -> Optional[float]:
        """Retorna o fechamento mais recente do SPX"""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT close_price FROM spx_closes ORDER BY date DESC LIMIT 1')
            if row:
                return float(row['close_price'])
            return None
    
    async def save_polymarket_opens(self, data: PolymarketOdds):
        """Salva odds do mercado Opens"""
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO polymarket_opens (timestamp, market_date, up_price, down_price, save_reason)
                VALUES ($1, $2, $3, $4, $5)
            ''', data.timestamp, data.market_date.date() if isinstance(data.market_date, datetime) else data.market_date,
                data.up_price, data.down_price, data.save_reason)
    
    async def save_polymarket_closes(self, data: PolymarketOdds):
        """Salva odds do mercado Up or Down (fechamento)"""
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO polymarket_closes (timestamp, market_date, up_price, down_price, save_reason)
                VALUES ($1, $2, $3, $4, $5)
            ''', data.timestamp, data.market_date.date() if isinstance(data.market_date, datetime) else data.market_date,
                data.up_price, data.down_price, data.save_reason)
    
    async def get_last_polymarket_opens(self) -> Optional[dict]:
        """Retorna último registro salvo do mercado Opens"""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT up_price, down_price, timestamp FROM polymarket_opens ORDER BY timestamp DESC LIMIT 1')
            if row:
                return {'up_price': float(row['up_price']), 'down_price': float(row['down_price']), 'timestamp': row['timestamp']}
            return None
    
    async def get_last_polymarket_closes(self) -> Optional[dict]:
        """Retorna último registro salvo do mercado Up or Down"""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT up_price, down_price, timestamp FROM polymarket_closes ORDER BY timestamp DESC LIMIT 1')
            if row:
                return {'up_price': float(row['up_price']), 'down_price': float(row['down_price']), 'timestamp': row['timestamp']}
            return None
    
    async def save_zscore_trigger(self, data: ZScoreTriggerData) -> Optional[int]:
        """Salva um z-score trigger com todos os campos e retorna o ID"""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                INSERT INTO zscore_triggers 
                (trigger_ts, spx_price, z_score, price_delta, detect_lag_ms, poly_read_lag_ms, pm_up_t0, pm_dw_t0,
                 cycle_start_ts, t_es_observed_ts, es_data_ts, t_pm_t0_read_ts, cycle_to_trigger_ms, es_fetch_ms, 
                 pm_t0_fetch_ms, avg_delta, stddev_delta, window_ticks, metric_type, pm_source, pm_snapshot_ts_t0)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                RETURNING id
            ''', data.trigger_ts, data.spx_price, data.z_score, data.price_delta,
                data.detect_lag_ms, data.poly_read_lag_ms, data.pm_up_t0, data.pm_dw_t0,
                data.cycle_start_ts, data.t_es_observed_ts, data.es_data_ts, data.t_pm_t0_read_ts,
                data.cycle_to_trigger_ms, data.es_fetch_ms, data.pm_t0_fetch_ms,
                data.avg_delta, data.stddev_delta, data.window_ticks, data.metric_type,
                data.pm_source, data.pm_snapshot_ts_t0)
            return row['id'] if row else None
    
    async def update_zscore_trigger_t1(self, trigger_id: int, pm_up_t1: float, pm_dw_t1: float,
                                        t_pm_t1_read_ts: datetime, pm_t1_fetch_ms: int,
                                        pm_snapshot_ts_t1: Optional[datetime] = None):
        """Atualiza um z-score trigger com os dados t+1"""
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE zscore_triggers 
                SET pm_up_t1 = $1, pm_dw_t1 = $2, t_pm_t1_read_ts = $3, pm_t1_fetch_ms = $4, pm_snapshot_ts_t1 = $5
                WHERE id = $6
            ''', pm_up_t1, pm_dw_t1, t_pm_t1_read_ts, pm_t1_fetch_ms, pm_snapshot_ts_t1, trigger_id)


# ============== YAHOO FINANCE API ==============
class YahooFinanceAPI:
    """Busca dados do Yahoo Finance"""
    
    def __init__(self):
        pass
    
    def get_es_futures_price(self) -> Optional[float]:
        """Retorna preço atual do ES=F"""
        try:
            ticker = yf.Ticker("ES=F")
            fast_info = ticker.fast_info
            return float(fast_info.last_price)
        except Exception as e:
            logger.error(f"Erro ao buscar ES=F: {e}")
            return None
    
    def get_latest_spx_close(self) -> Optional[float]:
        """Retorna o fechamento mais recente do SPX."""
        try:
            now_et = datetime.now(ET_TIMEZONE)
            ticker = yf.Ticker("^GSPC")
            fast_info = ticker.fast_info
            if now_et.weekday() < 5 and now_et.hour >= 16:
                return float(fast_info.last_price)
            if now_et.weekday() >= 5:
                return float(fast_info.previous_close)
            return float(fast_info.previous_close)
        except Exception as e:
            logger.error(f"Erro ao buscar fechamento ^GSPC: {e}")
            return None
    
    def get_spx_current_price(self) -> Optional[float]:
        """Retorna preço atual do SPX"""
        try:
            ticker = yf.Ticker("^GSPC")
            fast_info = ticker.fast_info
            return float(fast_info.last_price)
        except Exception as e:
            logger.error(f"Erro ao buscar preço atual ^GSPC: {e}")
            return None


# ============== POLYMARKET API ==============
class PolymarketAPI:
    """Busca dados do Polymarket via Gamma API"""
    
    BASE_URL = "https://gamma-api.polymarket.com"
    PM_SOURCE = "rest_outcome_price"
    
    MONTHS = {1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
              7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"}
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _get_next_trading_day(self, from_date: datetime) -> datetime:
        target = from_date + timedelta(days=1)
        while target.weekday() >= 5:
            target += timedelta(days=1)
        return target
    
    def _get_target_opens_day(self) -> datetime:
        now_et = datetime.now(ET_TIMEZONE)
        if now_et.weekday() >= 5:
            return self._get_next_trading_day(now_et)
        if now_et.hour < 10:
            return now_et
        return self._get_next_trading_day(now_et)
    
    def _get_target_closes_day(self) -> datetime:
        now_et = datetime.now(ET_TIMEZONE)
        if now_et.weekday() >= 5:
            return self._get_next_trading_day(now_et)
        if now_et.hour < 16 or (now_et.hour == 16 and now_et.minute < 30):
            return now_et
        return self._get_next_trading_day(now_et)
    
    def _build_opens_slug(self, target_date: datetime) -> str:
        return f"spx-opens-up-or-down-on-{self.MONTHS[target_date.month]}-{target_date.day}-{target_date.year}"
    
    def _build_closes_slug(self, target_date: datetime) -> str:
        return f"spx-up-or-down-on-{self.MONTHS[target_date.month]}-{target_date.day}-{target_date.year}"
    
    def get_opens_link(self) -> str:
        return f"https://polymarket.com/event/{self._build_opens_slug(self._get_target_opens_day())}"
    
    def get_closes_link(self) -> str:
        return f"https://polymarket.com/event/{self._build_closes_slug(self._get_target_closes_day())}"
    
    def get_opens_market_date(self) -> datetime:
        return self._get_target_opens_day()
    
    def get_closes_market_date(self) -> datetime:
        return self._get_target_closes_day()
    
    async def fetch_opens_odds(self) -> Optional[dict]:
        return await self._fetch_market_odds(self._build_opens_slug(self._get_target_opens_day()))
    
    async def fetch_closes_odds(self) -> Optional[dict]:
        return await self._fetch_market_odds(self._build_closes_slug(self._get_target_closes_day()))
    
    async def _fetch_market_odds(self, slug: str) -> Optional[dict]:
        session = await self.get_session()
        try:
            url = f"{self.BASE_URL}/events?slug={slug}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_odds(data)
                else:
                    logger.warning(f"Polymarket API retornou status {response.status} para {slug}")
        except Exception as e:
            logger.warning(f"Erro ao buscar odds do Polymarket ({slug}): {e}")
        return None
    
    def _parse_odds(self, data: list) -> Optional[dict]:
        if not data or len(data) == 0:
            return None
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            return None
        market = None
        for m in markets:
            if m.get("active", False):
                market = m
                break
        if not market:
            return None
        try:
            outcomes = market.get("outcomes", "[]")
            outcomes_prices = market.get("outcomePrices", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(outcomes_prices, str):
                outcomes_prices = json.loads(outcomes_prices)
            if len(outcomes) < 2 or len(outcomes_prices) < 2:
                return None
            up_price = down_price = None
            for i, outcome in enumerate(outcomes):
                if outcome.lower() == "up":
                    up_price = float(outcomes_prices[i])
                elif outcome.lower() == "down":
                    down_price = float(outcomes_prices[i])
            if up_price is not None and down_price is not None:
                return {'up_price': up_price, 'down_price': down_price, 'pm_source': self.PM_SOURCE, 'pm_snapshot_ts': None}
        except (json.JSONDecodeError, ValueError, IndexError) as e:
            logger.warning(f"Erro ao parsear odds: {e}")
        return None


# ============== TELEGRAM BOT ==============
class TelegramBot:
    """Envia mensagens pro Telegram"""
    
    API_URL = "https://api.telegram.org/bot{token}/sendMessage"
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        if not self.token or not self.chat_id:
            logger.warning("Telegram não configurado, pulando envio")
            return False
        session = await self.get_session()
        url = self.API_URL.format(token=self.token)
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info("Mensagem Telegram enviada")
                    return True
                else:
                    error = await response.text()
                    logger.error(f"Erro ao enviar Telegram: {error}")
                    return False
        except Exception as e:
            logger.error(f"Exceção ao enviar Telegram: {e}")
            return False
    
    async def send_startup_message(self, es_price: float, spx_close: float, opens_odds: dict, closes_odds: dict, polymarket: 'PolymarketAPI'):
        above_below = "acima" if es_price > spx_close else "abaixo"
        change_pct = ((es_price - spx_close) / spx_close) * 100
        lines = ["🤖 <b>SPX Monitor iniciado!</b>\n", f"📊 <b>ES=F:</b> {es_price:,.2f}",
                 f"📈 <b>SPX Close:</b> {spx_close:,.2f}", f"📍 Futuros {above_below} do fechamento ({change_pct:+.2f}%)\n"]
        if opens_odds:
            lines.append(f"🎯 <b>Opens:</b> Up {opens_odds['up_price']*100:.1f}¢ | Down {opens_odds['down_price']*100:.1f}¢")
        if closes_odds:
            lines.append(f"🎯 <b>Closes:</b> Up {closes_odds['up_price']*100:.1f}¢ | Down {closes_odds['down_price']*100:.1f}¢")
        lines.append(f"\n🔗 <a href='{polymarket.get_opens_link()}'>Opens Market</a>")
        lines.append(f"🔗 <a href='{polymarket.get_closes_link()}'>Closes Market</a>")
        lines.append(f"\n⏰ {datetime.now(BR_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} BRT")
        await self.send_message("\n".join(lines))
    
    async def send_heartbeat(self, es_price: float, spx_close: float, opens_odds: dict, closes_odds: dict,
                              uptime_hours: float, checks_count: int, polymarket: 'PolymarketAPI', esf_status: str = ""):
        lines = ["💓 <b>HEARTBEAT - SPX Monitor</b>\n"]
        if esf_status:
            lines.append(f"📡 <b>ES=F Status:</b> {esf_status}")
        if es_price and spx_close:
            above_below = "acima" if es_price > spx_close else "abaixo"
            change_pct = ((es_price - spx_close) / spx_close) * 100
            lines.append(f"📊 <b>ES=F:</b> {es_price:,.2f} ({above_below}, {change_pct:+.2f}%)")
            lines.append(f"📈 <b>SPX Close ref:</b> {spx_close:,.2f}\n")
        if opens_odds:
            lines.append(f"🎯 <b>Opens:</b> Up {opens_odds['up_price']*100:.1f}¢ | Down {opens_odds['down_price']*100:.1f}¢")
        if closes_odds:
            lines.append(f"🎯 <b>Closes:</b> Up {closes_odds['up_price']*100:.1f}¢ | Down {closes_odds['down_price']*100:.1f}¢")
        lines.append(f"\n📈 Verificações: {checks_count}")
        lines.append(f"⏱ Uptime: {uptime_hours:.1f}h")
        lines.append(f"\n🔗 <a href='{polymarket.get_opens_link()}'>Opens Market</a>")
        lines.append(f"🔗 <a href='{polymarket.get_closes_link()}'>Closes Market</a>")
        lines.append(f"\n⏰ {datetime.now(BR_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} BRT")
        await self.send_message("\n".join(lines))


# ============== DETECTOR DE MUDANÇAS ES=F ==============
class ESFuturesDetector:
    """Detecta mudanças significativas no ES=F com confirmação anti-ruído"""
    
    def __init__(self, threshold_1m: float, threshold_accumulated: float):
        self.threshold_1m = threshold_1m
        self.threshold_accumulated = threshold_accumulated
        self.last_price: Optional[float] = None
        self.last_saved_time: Optional[datetime] = None
        self.last_significant_price: Optional[float] = None
        self.last_above_close: Optional[bool] = None
        self.suspect_price: Optional[float] = None
        self.suspect_baseline: Optional[float] = None
        self.first_run = True
    
    def check(self, current_price: float, previous_close: float, current_time: datetime) -> Optional[SaveReason]:
        current_above_close = current_price > previous_close
        if self.first_run:
            self.last_price = current_price
            self.last_significant_price = current_price
            self.last_saved_time = current_time
            self.last_above_close = current_above_close
            self.first_run = False
            return SaveReason.INITIAL
        
        save_reason = None
        is_significant_move = False
        
        if self.last_above_close is not None and current_above_close != self.last_above_close:
            save_reason = SaveReason.CROSSED_CLOSE
            is_significant_move = True
            self._reset_suspect()
        
        if save_reason is None and self.last_significant_price:
            change_accumulated = abs(current_price - self.last_significant_price) / self.last_significant_price
            if change_accumulated >= self.threshold_accumulated:
                save_reason = SaveReason.PRICE_CHANGE_ACCUMULATED
                is_significant_move = True
                self._reset_suspect()
                logger.info(f"Variação acumulada: {self.last_significant_price:.2f} -> {current_price:.2f} ({change_accumulated*100:.2f}%)")
        
        if save_reason is None and self.last_price:
            change_1m = abs(current_price - self.last_price) / self.last_price
            if change_1m >= self.threshold_1m:
                if self.suspect_price is None:
                    self.suspect_price = current_price
                    self.suspect_baseline = self.last_price
                    logger.info(f"Movimento suspeito detectado: {self.last_price:.2f} -> {current_price:.2f} ({change_1m*100:.2f}%)")
                else:
                    if self.suspect_baseline:
                        change_from_baseline = abs(current_price - self.suspect_baseline) / self.suspect_baseline
                        if change_from_baseline >= self.threshold_1m:
                            save_reason = SaveReason.PRICE_CHANGE_1M
                            is_significant_move = True
                            logger.info(f"Movimento confirmado: {self.suspect_baseline:.2f} -> {current_price:.2f}")
                        else:
                            logger.info(f"Movimento reverteu, descartando")
                    self._reset_suspect()
            else:
                if self.suspect_price is not None:
                    self._reset_suspect()
        
        if save_reason is None and self.last_saved_time:
            if (current_time - self.last_saved_time).total_seconds() >= HEARTBEAT_INTERVAL:
                save_reason = SaveReason.TIME_INTERVAL
        
        self.last_price = current_price
        self.last_above_close = current_above_close
        if save_reason:
            self.last_saved_time = current_time
            if is_significant_move:
                self.last_significant_price = current_price
        return save_reason
    
    def _reset_suspect(self):
        self.suspect_price = None
        self.suspect_baseline = None


# ============== DETECTOR DE MUDANÇAS POLYMARKET ==============
class PolymarketDetector:
    """Detecta mudanças significativas nas odds do Polymarket"""
    
    def __init__(self, threshold_percent: float, threshold_cents: float):
        self.threshold_percent = threshold_percent
        self.threshold_cents = threshold_cents
        self.last_up_price: Optional[float] = None
        self.first_run = True
    
    def check(self, up_price: float) -> tuple[bool, str]:
        if self.first_run:
            self.last_up_price = up_price
            self.first_run = False
            return True, "initial"
        if self.last_up_price is None or self.last_up_price == 0:
            self.last_up_price = up_price
            return True, "initial"
        change_cents = abs(up_price - self.last_up_price)
        change_percent = change_cents / self.last_up_price
        if change_percent >= self.threshold_percent and change_cents >= self.threshold_cents:
            # FIX PROBLEMA 1: Guardar valor anterior ANTES de atualizar
            old_up_price = self.last_up_price
            self.last_up_price = up_price
            direction = "up" if up_price > old_up_price else "down"
            return True, f"price_change_{direction}"
        return False, ""


# ============== DETECTOR DE Z-SCORE ==============
class ZScoreDetector:
    """Detecta movimentos anômalos no ES=F usando z-score. Métrica: delta_points"""
    
    METRIC_TYPE = "delta_points"
    
    def __init__(self, window_size: int = ZSCORE_WINDOW_SIZE, min_ticks: int = ZSCORE_MIN_TICKS,
                 threshold: float = ZSCORE_THRESHOLD, cooldown_seconds: int = ZSCORE_COOLDOWN_SECONDS):
        self.window_size = window_size
        self.min_ticks = min_ticks
        self.threshold = threshold
        self.cooldown_seconds = cooldown_seconds
        self.prices: Deque[float] = deque(maxlen=window_size + 1)
        self.last_trigger_time: Optional[datetime] = None
    
    def add_price(self, price: float) -> Optional[ZScoreResult]:
        self.prices.append(price)
        if len(self.prices) < self.min_ticks + 1:
            return None
        deltas = []
        prices_list = list(self.prices)
        for i in range(1, len(prices_list)):
            deltas.append(prices_list[i] - prices_list[i-1])
        if len(deltas) < self.min_ticks:
            return None
        current_delta = deltas[-1]
        avg_delta = sum(deltas) / len(deltas)
        variance = sum((d - avg_delta) ** 2 for d in deltas) / (len(deltas) - 1)
        stddev_delta = math.sqrt(variance)
        if stddev_delta == 0:
            return None
        z_score = (current_delta - avg_delta) / stddev_delta
        
        if len(deltas) < ZSCORE_MIN_WINDOW_TICKS:
            return None
        if abs(current_delta) < ZSCORE_MIN_DELTA_POINTS:
            logger.debug(f"Trigger cortado por delta insuficiente: {current_delta:.2f} pts (min={ZSCORE_MIN_DELTA_POINTS})")
            return None

        if abs(z_score) >= self.threshold:
            if self.last_trigger_time:
                elapsed = (datetime.now(ET_TIMEZONE) - self.last_trigger_time).total_seconds()
                if elapsed < self.cooldown_seconds:
                    logger.debug(f"Z-score trigger ignorado por cooldown ({elapsed:.0f}s < {self.cooldown_seconds}s)")
                    return None
            self.last_trigger_time = datetime.now(ET_TIMEZONE)
            return ZScoreResult(z_score=z_score, price_delta=current_delta, avg_delta=avg_delta,
                               stddev_delta=stddev_delta, window_ticks=len(deltas), metric_type=self.METRIC_TYPE)
        return None
    
    def reset(self):
        self.prices.clear()


# ============== MONITOR PRINCIPAL ==============
class SPXMonitor:
    """Monitor principal"""
    
    def __init__(self):
        self.yahoo = YahooFinanceAPI()
        self.polymarket = PolymarketAPI()
        self.telegram = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        self.db = Database(DATABASE_URL)
        self.es_detector = ESFuturesDetector(ESF_THRESHOLD_1M, ESF_THRESHOLD_ACCUMULATED)
        self.opens_detector = PolymarketDetector(PM_THRESHOLD_PERCENT, PM_THRESHOLD_CENTS)
        self.closes_detector = PolymarketDetector(PM_THRESHOLD_PERCENT, PM_THRESHOLD_CENTS)
        self.zscore_detector = ZScoreDetector()
        self.running = False
        self.start_time: Optional[datetime] = None
        self.checks_count = 0
        self.zscore_checks_count = 0
        self.current_spx_close: Optional[float] = None
        self.last_es_price: Optional[float] = None
        self.last_opens_odds: Optional[dict] = None
        self.last_closes_odds: Optional[dict] = None
        self.pending_zscore_triggers: List[dict] = []
    
    def _is_esf_trading_hours(self) -> bool:
        now_et = datetime.now(ET_TIMEZONE)
        weekday = now_et.weekday()
        hour = now_et.hour
        if weekday == 5:
            return False
        if weekday == 6:
            return hour >= 18
        if weekday == 4:
            return hour < 17
        if 17 <= hour < 18:
            return False
        return True
    
    async def start(self):
        logger.info("Iniciando SPX Monitor...")
        self.running = True
        self.start_time = datetime.now()
        await self.db.connect()
        try:
            self.current_spx_close = await self.db.get_latest_spx_close()
            if not self.current_spx_close:
                self.current_spx_close = self.yahoo.get_latest_spx_close()
                if self.current_spx_close:
                    await self.db.save_spx_close(SPXClose(date=datetime.now(ET_TIMEZONE), close_price=self.current_spx_close))
            if not self.current_spx_close:
                logger.error("Não foi possível obter fechamento do SPX")
                return
            logger.info(f"SPX Close de referência: {self.current_spx_close}")
            self.last_es_price = self.yahoo.get_es_futures_price()
            if not self.last_es_price:
                logger.error("Não foi possível obter preço do ES=F")
                return
            logger.info(f"ES=F atual: {self.last_es_price}")
            self.last_opens_odds = await self.polymarket.fetch_opens_odds()
            self.last_closes_odds = await self.polymarket.fetch_closes_odds()
            logger.info(f"Opens odds: {self.last_opens_odds}")
            logger.info(f"Closes odds: {self.last_closes_odds}")
            await self.telegram.send_startup_message(self.last_es_price, self.current_spx_close,
                                                      self.last_opens_odds, self.last_closes_odds, self.polymarket)
        except Exception as e:
            logger.error(f"Erro na inicialização: {e}")
            return
        await asyncio.gather(
            self._check_loop(),
            self._zscore_loop(),
            self._heartbeat_loop(),
            self._telegram_heartbeat_loop(),
            self._close_capture_loop(),
        )
        await self.cleanup()
    
    async def _check_loop(self):
        while self.running:
            try:
                await asyncio.sleep(CHECK_INTERVAL)
                await self._check_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no check loop: {e}")
                await asyncio.sleep(30)
    
    async def _zscore_loop(self):
        """Sub-loop dedicado para z-score (10s). ÚNICO responsável por z-score e triggers pendentes."""
        while self.running:
            try:
                await self._zscore_check()
                await self._process_pending_triggers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no zscore_loop: {e}")
            await asyncio.sleep(ZSCORE_CHECK_INTERVAL)
    
    async def _zscore_check(self):
        if not self._is_esf_trading_hours():
            return
        self.zscore_checks_count += 1
        cycle_start_ts = datetime.now(ET_TIMEZONE)
        t_cycle_start = time.perf_counter()
        t_es_fetch_start = time.perf_counter()
        es_price = self.yahoo.get_es_futures_price()
        t_es_fetch_end = time.perf_counter()
        t_es_observed_ts = datetime.now(ET_TIMEZONE)
        es_fetch_ms = int((t_es_fetch_end - t_es_fetch_start) * 1000)
        if not es_price:
            return
        self.last_es_price = es_price
        zscore_result = self.zscore_detector.add_price(es_price)
        if zscore_result:
            t_trigger = time.perf_counter()
            cycle_to_trigger_ms = int((t_trigger - t_cycle_start) * 1000)
            trigger_ts = t_es_observed_ts
            await self._handle_zscore_trigger(es_price=es_price, zscore_result=zscore_result, trigger_ts=trigger_ts,
                                               cycle_start_ts=cycle_start_ts, t_es_observed_ts=t_es_observed_ts,
                                               cycle_to_trigger_ms=cycle_to_trigger_ms, es_fetch_ms=es_fetch_ms)
    
    async def _handle_zscore_trigger(self, es_price: float, zscore_result: ZScoreResult, trigger_ts: datetime,
                                      cycle_start_ts: datetime, t_es_observed_ts: datetime,
                                      cycle_to_trigger_ms: int, es_fetch_ms: int):
        t_pm_t0_start = time.perf_counter()
        closes_odds = await self.polymarket.fetch_closes_odds()
        t_pm_t0_end = time.perf_counter()
        t_pm_t0_read_ts = datetime.now(ET_TIMEZONE)
        pm_t0_fetch_ms = int((t_pm_t0_end - t_pm_t0_start) * 1000)
        pm_up_t0 = closes_odds['up_price'] if closes_odds else 0
        pm_dw_t0 = closes_odds['down_price'] if closes_odds else 0
        pm_source = closes_odds.get('pm_source', 'rest_outcome_price') if closes_odds else 'rest_outcome_price'
        pm_snapshot_ts_t0 = closes_odds.get('pm_snapshot_ts') if closes_odds else None
        detect_lag_ms = cycle_to_trigger_ms
        poly_read_lag_ms = pm_t0_fetch_ms
        logger.info(f"[LATENCY_AUDIT] trigger_ts={trigger_ts.isoformat()} t_es_observed={t_es_observed_ts.isoformat()} "
                   f"t_pm_t0_read={t_pm_t0_read_ts.isoformat()} cycle_to_trigger_ms={cycle_to_trigger_ms} "
                   f"es_fetch_ms={es_fetch_ms} pm_t0_fetch_ms={pm_t0_fetch_ms}")
        trigger_data = ZScoreTriggerData(
            trigger_ts=trigger_ts, cycle_start_ts=cycle_start_ts, t_es_observed_ts=t_es_observed_ts,
            es_data_ts=None, t_pm_t0_read_ts=t_pm_t0_read_ts, spx_price=es_price,
            z_score=zscore_result.z_score, price_delta=zscore_result.price_delta,
            avg_delta=zscore_result.avg_delta, stddev_delta=zscore_result.stddev_delta,
            window_ticks=zscore_result.window_ticks, metric_type=zscore_result.metric_type,
            cycle_to_trigger_ms=cycle_to_trigger_ms, es_fetch_ms=es_fetch_ms, pm_t0_fetch_ms=pm_t0_fetch_ms,
            detect_lag_ms=detect_lag_ms, poly_read_lag_ms=poly_read_lag_ms,
            pm_up_t0=pm_up_t0, pm_dw_t0=pm_dw_t0, pm_source=pm_source, pm_snapshot_ts_t0=pm_snapshot_ts_t0)
        trigger_id = await self.db.save_zscore_trigger(trigger_data)
        if trigger_id:
            self.pending_zscore_triggers.append({'trigger_id': trigger_id, 'trigger_time': trigger_ts,
                'z_score': zscore_result.z_score, 'es_price': es_price, 'pm_up_t0': pm_up_t0,
                'stddev_delta': zscore_result.stddev_delta})
        z_sign = "+" if zscore_result.z_score > 0 else ""
        message = (f"⚡ <b>Z-SCORE TRIGGER</b>\n\n"
                   f"📊 <b>ES=F:</b> {es_price:,.2f} | <b>Z:</b> {z_sign}{zscore_result.z_score:.2f}\n"
                   f"📈 <b>Delta:</b> {zscore_result.price_delta:+.2f} pts\n"
                   f"📉 <b>StdDev:</b> {zscore_result.stddev_delta:.2f} pts ({zscore_result.window_ticks} ticks)\n"
                   f"🎯 <b>PM Up:</b> {pm_up_t0*100:.1f}¢ → [aguardando t+1m]\n\n"
                   f"⏱ <b>ES fetch:</b> {es_fetch_ms}ms\n⏱ <b>PM t0 fetch:</b> {pm_t0_fetch_ms}ms\n"
                   f"\n⏰ {trigger_ts.strftime('%Y-%m-%d %H:%M:%S')} ET")
        await self.telegram.send_message(message)
        logger.info(f"Z-Score trigger: Z={zscore_result.z_score:.2f}, ES={es_price:.2f}, "
                   f"delta={zscore_result.price_delta:.2f}, stddev={zscore_result.stddev_delta:.2f}")
    
    async def _process_pending_triggers(self):
        if not self.pending_zscore_triggers:
            return
        now = datetime.now(ET_TIMEZONE)
        completed = []
        for pending in self.pending_zscore_triggers:
            elapsed = (now - pending['trigger_time']).total_seconds()
            if elapsed >= ZSCORE_T1_DELAY_SECONDS:
                t_pm_t1_start = time.perf_counter()
                closes_odds = await self.polymarket.fetch_closes_odds()
                t_pm_t1_end = time.perf_counter()
                t_pm_t1_read_ts = datetime.now(ET_TIMEZONE)
                pm_t1_fetch_ms = int((t_pm_t1_end - t_pm_t1_start) * 1000)
                if closes_odds:
                    pm_up_t1 = closes_odds['up_price']
                    pm_dw_t1 = closes_odds['down_price']
                    pm_snapshot_ts_t1 = closes_odds.get('pm_snapshot_ts')
                    await self.db.update_zscore_trigger_t1(trigger_id=pending['trigger_id'], pm_up_t1=pm_up_t1, pm_dw_t1=pm_dw_t1,
                                                           t_pm_t1_read_ts=t_pm_t1_read_ts, pm_t1_fetch_ms=pm_t1_fetch_ms,
                                                           pm_snapshot_ts_t1=pm_snapshot_ts_t1)
                    pm_change = pm_up_t1 - pending['pm_up_t0']
                    pm_change_sign = "+" if pm_change > 0 else ""
                    message = (f"📊 <b>Z-SCORE T+1 UPDATE</b>\n\n"
                               f"🎯 <b>PM Up:</b> {pending['pm_up_t0']*100:.1f}¢ → {pm_up_t1*100:.1f}¢ ({pm_change_sign}{pm_change*100:.1f}¢)\n"
                               f"📈 <b>ES=F trigger:</b> {pending['es_price']:,.2f}\n"
                               f"⚡ <b>Z-Score:</b> {pending['z_score']:.2f}\n"
                               f"⏱ <b>PM t1 fetch:</b> {pm_t1_fetch_ms}ms\n\n⏰ +{int(elapsed)}s from trigger")
                    await self.telegram.send_message(message)
                    logger.info(f"Z-Score t+1: PM Up {pending['pm_up_t0']:.4f} -> {pm_up_t1:.4f} (pm_t1_fetch_ms={pm_t1_fetch_ms})")
                completed.append(pending)
        for c in completed:
            self.pending_zscore_triggers.remove(c)
    
    async def _heartbeat_loop(self):
        while self.running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no heartbeat loop: {e}")
                await asyncio.sleep(60)
    
    async def _telegram_heartbeat_loop(self):
        while self.running:
            try:
                await asyncio.sleep(TELEGRAM_HEARTBEAT_INTERVAL)
                await self._send_telegram_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no telegram heartbeat loop: {e}")
                await asyncio.sleep(60)
    
    async def _close_capture_loop(self):
        while self.running:
            try:
                now_et = datetime.now(ET_TIMEZONE)
                target_time = now_et.replace(hour=16, minute=5, second=0, microsecond=0)
                if now_et >= target_time:
                    target_time += timedelta(days=1)
                while target_time.weekday() >= 5:
                    target_time += timedelta(days=1)
                wait_seconds = (target_time - now_et).total_seconds()
                logger.info(f"Próxima captura de fechamento em {wait_seconds/3600:.1f}h ({target_time})")
                await asyncio.sleep(wait_seconds)
                close_price = self.yahoo.get_spx_current_price()
                if close_price:
                    self.current_spx_close = close_price
                    await self.db.save_spx_close(SPXClose(date=datetime.now(ET_TIMEZONE), close_price=close_price))
                    logger.info(f"Fechamento SPX capturado: {close_price}")
                    self.es_detector = ESFuturesDetector(ESF_THRESHOLD_1M, ESF_THRESHOLD_ACCUMULATED)
                    self.opens_detector = PolymarketDetector(PM_THRESHOLD_PERCENT, PM_THRESHOLD_CENTS)
                    self.closes_detector = PolymarketDetector(PM_THRESHOLD_PERCENT, PM_THRESHOLD_CENTS)
                    self.zscore_detector.reset()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no close capture loop: {e}")
                await asyncio.sleep(60)
    
    async def _check_once(self):
        """Loop principal (60s). NÃO toca em z-score."""
        self.checks_count += 1
        now = datetime.now(ET_TIMEZONE)
        if self._is_esf_trading_hours() and self.last_es_price and self.current_spx_close:
            es_price = self.last_es_price
            save_reason = self.es_detector.check(es_price, self.current_spx_close, now)
            if save_reason:
                above_close = es_price > self.current_spx_close
                change_pct = ((es_price - self.current_spx_close) / self.current_spx_close) * 100
                await self.db.save_futures(ESFuturesData(timestamp=now, price=es_price, previous_close=self.current_spx_close,
                                                          above_close=above_close, change_from_close_pct=change_pct, save_reason=save_reason))
                logger.info(f"ES=F salvo: {es_price:.2f} ({save_reason.value})")
        opens_odds = await self.polymarket.fetch_opens_odds()
        if opens_odds:
            self.last_opens_odds = opens_odds
            should_save, reason = self.opens_detector.check(opens_odds['up_price'])
            if should_save:
                await self.db.save_polymarket_opens(PolymarketOdds(timestamp=now, market_date=self.polymarket.get_opens_market_date(),
                                                                    up_price=opens_odds['up_price'], down_price=opens_odds['down_price'], save_reason=reason))
                logger.info(f"Opens odds salvo: Up {opens_odds['up_price']*100:.1f}¢ ({reason})")
        closes_odds = await self.polymarket.fetch_closes_odds()
        if closes_odds:
            self.last_closes_odds = closes_odds
            should_save, reason = self.closes_detector.check(closes_odds['up_price'])
            if should_save:
                await self.db.save_polymarket_closes(PolymarketOdds(timestamp=now, market_date=self.polymarket.get_closes_market_date(),
                                                                      up_price=closes_odds['up_price'], down_price=closes_odds['down_price'], save_reason=reason))
                logger.info(f"Closes odds salvo: Up {closes_odds['up_price']*100:.1f}¢ ({reason})")
        if self.checks_count % 10 == 0:
            es_price_str = f"{self.last_es_price:.2f}" if self.last_es_price else "mercado fechado"
            logger.info(f"Check #{self.checks_count} - ES=F: {es_price_str} (zscore_checks: {self.zscore_checks_count})")
    
    async def _send_telegram_heartbeat(self):
        uptime_hours = (datetime.now() - self.start_time).total_seconds() / 3600 if self.start_time else 0
        esf_status = "🟢 Mercado aberto" if self._is_esf_trading_hours() else "🔴 Mercado fechado"
        await self.telegram.send_heartbeat(self.last_es_price, self.current_spx_close, self.last_opens_odds,
                                            self.last_closes_odds, uptime_hours, self.checks_count, self.polymarket, esf_status)
        logger.info(f"Telegram heartbeat enviado - Uptime: {uptime_hours:.1f}h")
    
    async def cleanup(self):
        await self.polymarket.close()
        await self.telegram.close()
        await self.db.close()
        logger.info("Recursos liberados")
    
    def stop(self):
        self.running = False


# ============== ENTRY POINT ==============
async def main():
    monitor = SPXMonitor()
    try:
        await monitor.start()
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")
        monitor.stop()


if __name__ == "__main__":
    asyncio.run(main())
