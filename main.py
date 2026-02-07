"""
SPX Polymarket Monitor Bot
Monitora os futuros do E-mini S&P 500 (ES=F) e odds do Polymarket para mercados SPX Up/Down
Grava histórico no PostgreSQL para análise de divergências
"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
import logging
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
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "1200"))  # 20 minutos (salvamento no banco)
TELEGRAM_HEARTBEAT_INTERVAL = int(os.getenv("TELEGRAM_HEARTBEAT_INTERVAL", "43200"))  # 12 horas (notificação Telegram)

# Thresholds ES=F
ESF_THRESHOLD_1M = float(os.getenv("ESF_THRESHOLD_1M", "0.002"))  # 0.2% desde última medição
ESF_THRESHOLD_ACCUMULATED = float(os.getenv("ESF_THRESHOLD_ACCUMULATED", "0.005"))  # 0.5% desde último salvamento

# Thresholds Polymarket
PM_THRESHOLD_PERCENT = float(os.getenv("PM_THRESHOLD_PERCENT", "0.05"))  # 5%
PM_THRESHOLD_CENTS = float(os.getenv("PM_THRESHOLD_CENTS", "0.01"))  # 1 centavo

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
            
            logger.info("Tabelas criadas/verificadas")
    
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
                FROM spx_futures
                ORDER BY timestamp DESC
                LIMIT 1
            ''')
            if row:
                return {
                    'price': float(row['price']),
                    'previous_close': float(row['previous_close']),
                    'above_close': row['above_close'],
                    'timestamp': row['timestamp']
                }
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
            row = await conn.fetchrow('''
                SELECT close_price FROM spx_closes
                ORDER BY date DESC
                LIMIT 1
            ''')
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
            row = await conn.fetchrow('''
                SELECT up_price, down_price, timestamp
                FROM polymarket_opens
                ORDER BY timestamp DESC
                LIMIT 1
            ''')
            if row:
                return {
                    'up_price': float(row['up_price']),
                    'down_price': float(row['down_price']),
                    'timestamp': row['timestamp']
                }
            return None
    
    async def get_last_polymarket_closes(self) -> Optional[dict]:
        """Retorna último registro salvo do mercado Up or Down"""
        if not self.pool:
            return None
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT up_price, down_price, timestamp
                FROM polymarket_closes
                ORDER BY timestamp DESC
                LIMIT 1
            ''')
            if row:
                return {
                    'up_price': float(row['up_price']),
                    'down_price': float(row['down_price']),
                    'timestamp': row['timestamp']
                }
            return None


# ============== YAHOO FINANCE API ==============
class YahooFinanceAPI:
    """Busca dados do Yahoo Finance"""
    
    def __init__(self):
        self.es_ticker = yf.Ticker("ES=F")
        self.spx_ticker = yf.Ticker("^GSPC")
    
    def get_es_futures_price(self) -> Optional[float]:
        """Retorna preço atual do ES=F"""
        try:
            fast_info = self.es_ticker.fast_info
            return float(fast_info.last_price)
        except Exception as e:
            logger.error(f"Erro ao buscar ES=F: {e}")
            return None
    
    def get_latest_spx_close(self) -> Optional[float]:
        """
        Retorna o fechamento mais recente do SPX.
        - Se o mercado já fechou hoje (após 16h ET), retorna o fechamento de hoje
        - Se o mercado ainda não fechou, retorna o fechamento do dia anterior
        """
        try:
            now_et = datetime.now(ET_TIMEZONE)
            fast_info = self.spx_ticker.fast_info
            
            # Se é dia útil e depois das 16h, o mercado já fechou hoje
            # Nesse caso, last_price é o fechamento de hoje
            if now_et.weekday() < 5 and now_et.hour >= 16:
                return float(fast_info.last_price)
            
            # Se é fim de semana, retorna o último fechamento (sexta)
            if now_et.weekday() >= 5:
                return float(fast_info.previous_close)
            
            # Se é dia útil antes das 16h, mercado ainda não fechou
            # Retorna o fechamento do dia anterior
            return float(fast_info.previous_close)
            
        except Exception as e:
            logger.error(f"Erro ao buscar fechamento ^GSPC: {e}")
            return None
    
    def get_spx_current_price(self) -> Optional[float]:
        """Retorna preço atual do SPX"""
        try:
            fast_info = self.spx_ticker.fast_info
            return float(fast_info.last_price)
        except Exception as e:
            logger.error(f"Erro ao buscar preço atual ^GSPC: {e}")
            return None


# ============== POLYMARKET API ==============
class PolymarketAPI:
    """Busca dados do Polymarket via Gamma API"""
    
    BASE_URL = "https://gamma-api.polymarket.com"
    
    MONTHS = {
        1: "january", 2: "february", 3: "march", 4: "april",
        5: "may", 6: "june", 7: "july", 8: "august",
        9: "september", 10: "october", 11: "november", 12: "december"
    }
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _get_target_trading_day(self) -> datetime:
        """Retorna o próximo dia de trading relevante"""
        now_et = datetime.now(ET_TIMEZONE)
        
        # Se é dia de semana e antes das 16h, o mercado é de hoje
        if now_et.weekday() < 5 and now_et.hour < 16:
            return now_et
        
        # Senão, próximo dia útil
        target = now_et + timedelta(days=1)
        while target.weekday() >= 5:  # Sábado ou Domingo
            target += timedelta(days=1)
        
        return target
    
    def _build_opens_slug(self, target_date: datetime) -> str:
        """Constrói o slug do evento Opens Up or Down"""
        month = self.MONTHS[target_date.month]
        day = target_date.day
        year = target_date.year
        return f"spx-opens-up-or-down-on-{month}-{day}-{year}"
    
    def _build_closes_slug(self, target_date: datetime) -> str:
        """Constrói o slug do evento Up or Down (fechamento)"""
        month = self.MONTHS[target_date.month]
        day = target_date.day
        year = target_date.year
        return f"spx-up-or-down-on-{month}-{day}-{year}"
    
    def get_opens_link(self) -> str:
        """Retorna link do mercado Opens"""
        target = self._get_target_trading_day()
        slug = self._build_opens_slug(target)
        return f"https://polymarket.com/event/{slug}"
    
    def get_closes_link(self) -> str:
        """Retorna link do mercado Up or Down"""
        target = self._get_target_trading_day()
        slug = self._build_closes_slug(target)
        return f"https://polymarket.com/event/{slug}"
    
    def get_market_date(self) -> datetime:
        """Retorna data do mercado atual"""
        return self._get_target_trading_day()
    
    async def fetch_opens_odds(self) -> Optional[dict]:
        """Busca odds do mercado Opens Up or Down"""
        target = self._get_target_trading_day()
        slug = self._build_opens_slug(target)
        return await self._fetch_market_odds(slug)
    
    async def fetch_closes_odds(self) -> Optional[dict]:
        """Busca odds do mercado Up or Down (fechamento)"""
        target = self._get_target_trading_day()
        slug = self._build_closes_slug(target)
        return await self._fetch_market_odds(slug)
    
    async def _fetch_market_odds(self, slug: str) -> Optional[dict]:
        """Busca odds de um mercado específico"""
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
        """Parse das odds do evento SPX Up/Down (mercado binário)"""
        if not data or len(data) == 0:
            return None
        
        event = data[0]
        markets = event.get("markets", [])
        
        if not markets:
            return None
        
        # Pegar o primeiro market ativo
        market = None
        for m in markets:
            if m.get("active", False):
                market = m
                break
        
        if not market:
            return None
        
        try:
            # outcomes: ["Up", "Down"]
            # outcomePrices: ["0.475", "0.525"]
            outcomes = market.get("outcomes", "[]")
            outcomes_prices = market.get("outcomePrices", "[]")
            
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(outcomes_prices, str):
                outcomes_prices = json.loads(outcomes_prices)
            
            if len(outcomes) < 2 or len(outcomes_prices) < 2:
                return None
            
            # Mapear outcomes para preços
            up_price = None
            down_price = None
            
            for i, outcome in enumerate(outcomes):
                outcome_lower = outcome.lower()
                if outcome_lower == "up":
                    up_price = float(outcomes_prices[i])
                elif outcome_lower == "down":
                    down_price = float(outcomes_prices[i])
            
            if up_price is not None and down_price is not None:
                return {
                    'up_price': up_price,
                    'down_price': down_price
                }
                
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
        
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        
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
    
    async def send_startup_message(self, es_price: float, spx_close: float, 
                                    opens_odds: dict, closes_odds: dict,
                                    polymarket: 'PolymarketAPI'):
        """Envia mensagem de inicialização"""
        above_below = "acima" if es_price > spx_close else "abaixo"
        change_pct = ((es_price - spx_close) / spx_close) * 100
        
        lines = [
            "🤖 <b>SPX Monitor iniciado!</b>\n",
            f"📊 <b>ES=F:</b> {es_price:,.2f}",
            f"📈 <b>SPX Close:</b> {spx_close:,.2f}",
            f"📍 Futuros {above_below} do fechamento ({change_pct:+.2f}%)\n",
        ]
        
        if opens_odds:
            lines.append(f"🎯 <b>Opens:</b> Up {opens_odds['up_price']*100:.1f}¢ | Down {opens_odds['down_price']*100:.1f}¢")
        
        if closes_odds:
            lines.append(f"🎯 <b>Closes:</b> Up {closes_odds['up_price']*100:.1f}¢ | Down {closes_odds['down_price']*100:.1f}¢")
        
        lines.append(f"\n🔗 <a href='{polymarket.get_opens_link()}'>Opens Market</a>")
        lines.append(f"🔗 <a href='{polymarket.get_closes_link()}'>Closes Market</a>")
        lines.append(f"\n⏰ {datetime.now(BR_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} BRT")
        
        await self.send_message("\n".join(lines))
    
    async def send_heartbeat(self, es_price: float, spx_close: float,
                              opens_odds: dict, closes_odds: dict,
                              uptime_hours: float, checks_count: int,
                              polymarket: 'PolymarketAPI', esf_status: str = ""):
        """Envia heartbeat"""
        lines = [
            "💓 <b>HEARTBEAT - SPX Monitor</b>\n",
        ]
        
        # Adicionar status do mercado ES=F
        if esf_status:
            lines.append(f"📡 <b>ES=F Status:</b> {esf_status}")
        
        # Mostrar dados do ES=F se disponíveis
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
        
        # Estado
        self.last_price: Optional[float] = None  # Última medição (1 min atrás)
        self.last_saved_price: Optional[float] = None  # Último preço salvo no banco
        self.last_saved_time: Optional[datetime] = None
        self.last_above_close: Optional[bool] = None
        
        # Anti-ruído
        self.suspect_price: Optional[float] = None  # Preço suspeito aguardando confirmação
        self.suspect_baseline: Optional[float] = None  # Preço de referência (t-1)
        self.first_run = True
    
    def check(self, current_price: float, previous_close: float, current_time: datetime) -> Optional[SaveReason]:
        """
        Verifica se deve salvar o registro atual.
        Retorna SaveReason se deve salvar, None se não.
        """
        current_above_close = current_price > previous_close
        
        # Primeira execução
        if self.first_run:
            self.last_price = current_price
            self.last_saved_price = current_price
            self.last_saved_time = current_time
            self.last_above_close = current_above_close
            self.first_run = False
            return SaveReason.INITIAL
        
        save_reason = None
        
        # 1. Verificar cruzamento do fechamento
        if self.last_above_close is not None and current_above_close != self.last_above_close:
            save_reason = SaveReason.CROSSED_CLOSE
            self._reset_suspect()
        
        # 2. Verificar heartbeat (20 min desde último salvamento)
        elif self.last_saved_time and (current_time - self.last_saved_time).total_seconds() >= HEARTBEAT_INTERVAL:
            save_reason = SaveReason.TIME_INTERVAL
            self._reset_suspect()
        
        # 3. Verificar variação acumulada desde último salvamento (0.5%)
        elif self.last_saved_price:
            change_accumulated = abs(current_price - self.last_saved_price) / self.last_saved_price
            if change_accumulated >= self.threshold_accumulated:
                save_reason = SaveReason.PRICE_CHANGE_ACCUMULATED
                self._reset_suspect()
        
        # 4. Verificar variação rápida (0.2% em 1 min) com confirmação anti-ruído
        if save_reason is None and self.last_price:
            change_1m = abs(current_price - self.last_price) / self.last_price
            
            if change_1m >= self.threshold_1m:
                # Detectou movimento rápido
                if self.suspect_price is None:
                    # Primeira detecção - marca como suspeito, não salva ainda
                    self.suspect_price = current_price
                    self.suspect_baseline = self.last_price
                    logger.info(f"Movimento suspeito detectado: {self.last_price:.2f} -> {current_price:.2f} ({change_1m*100:.2f}%)")
                else:
                    # Segunda leitura - verificar se confirmou
                    if self.suspect_baseline:
                        change_from_baseline = abs(current_price - self.suspect_baseline) / self.suspect_baseline
                        if change_from_baseline >= self.threshold_1m:
                            # Confirmado - não reverteu
                            save_reason = SaveReason.PRICE_CHANGE_1M
                            logger.info(f"Movimento confirmado: {self.suspect_baseline:.2f} -> {current_price:.2f} ({change_from_baseline*100:.2f}%)")
                        else:
                            logger.info(f"Movimento reverteu, descartando")
                    self._reset_suspect()
            else:
                # Sem movimento significativo - reseta suspeito se existir
                if self.suspect_price is not None:
                    self._reset_suspect()
        
        # Atualizar estado
        self.last_price = current_price
        self.last_above_close = current_above_close
        
        if save_reason:
            self.last_saved_price = current_price
            self.last_saved_time = current_time
        
        return save_reason
    
    def _reset_suspect(self):
        """Reseta estado de suspeito"""
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
        """
        Verifica se deve salvar.
        Retorna (should_save, reason)
        """
        if self.first_run:
            self.last_up_price = up_price
            self.first_run = False
            return True, "initial"
        
        if self.last_up_price is None or self.last_up_price == 0:
            self.last_up_price = up_price
            return True, "initial"
        
        change_cents = abs(up_price - self.last_up_price)
        change_percent = change_cents / self.last_up_price
        
        # Deve atender AMBOS os critérios: >= 5% E >= 1¢
        if change_percent >= self.threshold_percent and change_cents >= self.threshold_cents:
            self.last_up_price = up_price
            direction = "up" if up_price > self.last_up_price else "down"
            return True, f"price_change_{direction}"
        
        return False, ""


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
        
        self.running = False
        self.start_time: Optional[datetime] = None
        self.checks_count = 0
        
        # Cache
        self.current_spx_close: Optional[float] = None
        self.last_es_price: Optional[float] = None
        self.last_opens_odds: Optional[dict] = None
        self.last_closes_odds: Optional[dict] = None
    
    def _is_esf_trading_hours(self) -> bool:
        """
        Verifica se o ES=F está em horário de negociação.
        ES=F negocia de domingo 18h ET até sexta 17h ET, com pausa diária 17h-18h.
        Retorna False de sexta 18h ET até domingo 17h ET (fim de semana).
        """
        now_et = datetime.now(ET_TIMEZONE)
        weekday = now_et.weekday()  # 0=segunda, 4=sexta, 5=sábado, 6=domingo
        hour = now_et.hour
        
        # Sábado: sempre fechado
        if weekday == 5:
            return False
        
        # Domingo: fechado até 17h, abre às 18h
        if weekday == 6:
            return hour >= 18
        
        # Sexta: fecha às 17h, não reabre
        if weekday == 4:
            return hour < 17
        
        # Segunda a quinta: pausa das 17h às 18h
        if 17 <= hour < 18:
            return False
        
        return True
    
    async def start(self):
        """Inicia o monitor"""
        logger.info("Iniciando SPX Monitor...")
        self.running = True
        self.start_time = datetime.now()
        
        # Conectar ao banco
        await self.db.connect()
        
        # Buscar dados iniciais
        try:
            # SPX Close - pega o fechamento mais recente disponível
            self.current_spx_close = await self.db.get_latest_spx_close()
            if not self.current_spx_close:
                self.current_spx_close = self.yahoo.get_latest_spx_close()
                if self.current_spx_close:
                    await self.db.save_spx_close(SPXClose(
                        date=datetime.now(ET_TIMEZONE),
                        close_price=self.current_spx_close
                    ))
            
            if not self.current_spx_close:
                logger.error("Não foi possível obter fechamento do SPX")
                return
            
            logger.info(f"SPX Close de referência: {self.current_spx_close}")
            
            # ES=F
            self.last_es_price = self.yahoo.get_es_futures_price()
            if not self.last_es_price:
                logger.error("Não foi possível obter preço do ES=F")
                return
            
            logger.info(f"ES=F atual: {self.last_es_price}")
            
            # Polymarket
            self.last_opens_odds = await self.polymarket.fetch_opens_odds()
            self.last_closes_odds = await self.polymarket.fetch_closes_odds()
            
            logger.info(f"Opens odds: {self.last_opens_odds}")
            logger.info(f"Closes odds: {self.last_closes_odds}")
            
            # Enviar mensagem de startup
            await self.telegram.send_startup_message(
                self.last_es_price, 
                self.current_spx_close,
                self.last_opens_odds,
                self.last_closes_odds,
                self.polymarket
            )
            
        except Exception as e:
            logger.error(f"Erro na inicialização: {e}")
            return
        
        # Iniciar loops
        await asyncio.gather(
            self._check_loop(),
            self._heartbeat_loop(),
            self._telegram_heartbeat_loop(),
            self._close_capture_loop(),
        )
        
        await self.cleanup()
    
    async def _check_loop(self):
        """Loop principal de verificação (1 min)"""
        while self.running:
            try:
                await asyncio.sleep(CHECK_INTERVAL)
                await self._check_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no check loop: {e}")
                await asyncio.sleep(30)
    
    async def _heartbeat_loop(self):
        """Loop de heartbeat para salvamento no banco (20 min) - sem notificação Telegram"""
        while self.running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                # Só salva no banco, não envia Telegram
                # O salvamento é feito pelo _check_once com save_reason=time_interval
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no heartbeat loop: {e}")
                await asyncio.sleep(60)
    
    async def _telegram_heartbeat_loop(self):
        """Loop de heartbeat do Telegram (12h) - 24/7"""
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
        """Loop para capturar fechamento do SPX às 16h05 ET"""
        while self.running:
            try:
                now_et = datetime.now(ET_TIMEZONE)
                
                # Calcular próximo 16h05
                target_time = now_et.replace(hour=16, minute=5, second=0, microsecond=0)
                if now_et >= target_time:
                    target_time += timedelta(days=1)
                
                # Pular fins de semana
                while target_time.weekday() >= 5:
                    target_time += timedelta(days=1)
                
                wait_seconds = (target_time - now_et).total_seconds()
                logger.info(f"Próxima captura de fechamento em {wait_seconds/3600:.1f}h ({target_time})")
                
                await asyncio.sleep(wait_seconds)
                
                # Capturar fechamento
                close_price = self.yahoo.get_spx_current_price()
                if close_price:
                    self.current_spx_close = close_price
                    await self.db.save_spx_close(SPXClose(
                        date=datetime.now(ET_TIMEZONE),
                        close_price=close_price
                    ))
                    logger.info(f"Fechamento SPX capturado: {close_price}")
                    
                    # Resetar detectores para novo dia
                    self.es_detector = ESFuturesDetector(ESF_THRESHOLD_1M, ESF_THRESHOLD_ACCUMULATED)
                    self.opens_detector = PolymarketDetector(PM_THRESHOLD_PERCENT, PM_THRESHOLD_CENTS)
                    self.closes_detector = PolymarketDetector(PM_THRESHOLD_PERCENT, PM_THRESHOLD_CENTS)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no close capture loop: {e}")
                await asyncio.sleep(60)
    
    async def _check_once(self):
        """Executa uma verificação"""
        self.checks_count += 1
        now = datetime.now(ET_TIMEZONE)
        
        # 1. Verificar ES=F (só durante horário de negociação)
        if self._is_esf_trading_hours():
            es_price = self.yahoo.get_es_futures_price()
            if es_price and self.current_spx_close:
                self.last_es_price = es_price
                
                save_reason = self.es_detector.check(es_price, self.current_spx_close, now)
                
                if save_reason:
                    above_close = es_price > self.current_spx_close
                    change_pct = ((es_price - self.current_spx_close) / self.current_spx_close) * 100
                    
                    await self.db.save_futures(ESFuturesData(
                        timestamp=now,
                        price=es_price,
                        previous_close=self.current_spx_close,
                        above_close=above_close,
                        change_from_close_pct=change_pct,
                        save_reason=save_reason
                    ))
                    logger.info(f"ES=F salvo: {es_price:.2f} ({save_reason.value})")
        
        # 2. Verificar Polymarket Opens (24/7)
        opens_odds = await self.polymarket.fetch_opens_odds()
        if opens_odds:
            self.last_opens_odds = opens_odds
            should_save, reason = self.opens_detector.check(opens_odds['up_price'])
            
            if should_save:
                await self.db.save_polymarket_opens(PolymarketOdds(
                    timestamp=now,
                    market_date=self.polymarket.get_market_date(),
                    up_price=opens_odds['up_price'],
                    down_price=opens_odds['down_price'],
                    save_reason=reason
                ))
                logger.info(f"Opens odds salvo: Up {opens_odds['up_price']*100:.1f}¢ ({reason})")
        
        # 3. Verificar Polymarket Closes
        closes_odds = await self.polymarket.fetch_closes_odds()
        if closes_odds:
            self.last_closes_odds = closes_odds
            should_save, reason = self.closes_detector.check(closes_odds['up_price'])
            
            if should_save:
                await self.db.save_polymarket_closes(PolymarketOdds(
                    timestamp=now,
                    market_date=self.polymarket.get_market_date(),
                    up_price=closes_odds['up_price'],
                    down_price=closes_odds['down_price'],
                    save_reason=reason
                ))
                logger.info(f"Closes odds salvo: Up {closes_odds['up_price']*100:.1f}¢ ({reason})")
        
        # Log periódico
        if self.checks_count % 10 == 0:
            logger.info(f"Check #{self.checks_count} - ES=F: {es_price:.2f if es_price else 'N/A'}")
    
    async def _send_heartbeat(self):
        """Heartbeat interno - não usado mais para Telegram"""
        pass
    
    async def _send_telegram_heartbeat(self):
        """Envia heartbeat pro Telegram a cada 12 horas"""
        uptime_hours = (datetime.now() - self.start_time).total_seconds() / 3600 if self.start_time else 0
        
        # Verificar se ES=F está em horário de negociação
        esf_status = "🟢 Mercado aberto" if self._is_esf_trading_hours() else "🔴 Mercado fechado"
        
        await self.telegram.send_heartbeat(
            self.last_es_price,
            self.current_spx_close,
            self.last_opens_odds,
            self.last_closes_odds,
            uptime_hours,
            self.checks_count,
            self.polymarket,
            esf_status
        )
        logger.info(f"Telegram heartbeat enviado - Uptime: {uptime_hours:.1f}h")
    
    async def cleanup(self):
        """Limpa recursos"""
        await self.polymarket.close()
        await self.telegram.close()
        await self.db.close()
        logger.info("Recursos liberados")
    
    def stop(self):
        """Para o monitor"""
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
