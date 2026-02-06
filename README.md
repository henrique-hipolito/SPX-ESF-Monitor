# SPX Polymarket Monitor

Bot que monitora os futuros do E-mini S&P 500 (ES=F) e as odds do Polymarket para detectar divergências e oportunidades de arbitragem.

## O que faz

- Monitora o preço do ES=F (E-mini S&P 500 Futures) via Yahoo Finance a cada minuto
- Monitora as odds dos mercados "SPX Opens Up or Down" e "SPX Up or Down" no Polymarket
- Salva histórico no PostgreSQL para análise posterior
- Envia heartbeat no Telegram a cada 20 minutos

## Regras de salvamento ES=F

- **Variação rápida:** salva se variar >= 0.2% em 1 minuto (com confirmação anti-ruído)
- **Variação acumulada:** salva se variar >= 0.5% desde o último salvamento
- **Heartbeat:** salva a cada 20 minutos independente de variação
- **Cruzamento:** salva sempre que cruzar o fechamento anterior (positivo <-> negativo)

### Confirmação anti-ruído

Quando detecta variação >= 0.2% em 1 minuto, marca como "suspeito" e espera a próxima leitura. Só salva se a variação persistir (não reverteu).

## Regras de salvamento Polymarket

- Salva se variou >= 5% E >= 1¢ desde o último registro

## Tabelas no banco

- `spx_futures` - registros do ES=F
- `spx_closes` - fechamentos diários do SPX (capturados às 16h05 ET)
- `polymarket_opens` - odds do mercado "Opens Up or Down"
- `polymarket_closes` - odds do mercado "Up or Down"

## Setup

### 1. Clonar repositório

```bash
git clone https://github.com/seu-usuario/spx-polymarket-monitor.git
cd spx-polymarket-monitor
```

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
# Editar .env com suas credenciais
```

### 3. Rodar com Docker

```bash
docker-compose up -d
```

### 4. Ver logs

```bash
docker logs -f spx-polymarket-monitor
```

## Rodar localmente (desenvolvimento)

```bash
pip install -r requirements.txt
python main.py
```

## Variáveis de ambiente

| Variável | Descrição | Default |
|----------|-----------|---------|
| TELEGRAM_BOT_TOKEN | Token do bot do Telegram | - |
| TELEGRAM_CHAT_ID | ID do chat do Telegram | - |
| DATABASE_URL | URL de conexão do PostgreSQL | - |
| CHECK_INTERVAL | Intervalo de checagem em segundos | 60 |
| HEARTBEAT_INTERVAL | Intervalo de heartbeat em segundos | 1200 |
| ESF_THRESHOLD_1M | Threshold de variação 1 min | 0.002 |
| ESF_THRESHOLD_ACCUMULATED | Threshold de variação acumulada | 0.005 |
| PM_THRESHOLD_PERCENT | Threshold % Polymarket | 0.05 |
| PM_THRESHOLD_CENTS | Threshold ¢ Polymarket | 0.01 |
