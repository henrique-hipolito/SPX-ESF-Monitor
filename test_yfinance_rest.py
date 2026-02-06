"""
Teste da API REST do Yahoo Finance para E-mini S&P 500 Futures (ES=F)
Objetivo: verificar o preço retornado e estimar o delay comparando com fonte em tempo real
"""

import yfinance as yf
from datetime import datetime

def main():
    print("=" * 60)
    print("TESTE - Yahoo Finance REST API")
    print("Ticker: ES=F (E-mini S&P 500 Futures)")
    print("=" * 60)
    print()
    
    ticker = yf.Ticker("ES=F")
    
    # Teste 1: fast_info
    print("1. FAST_INFO")
    print("-" * 40)
    try:
        fast = ticker.fast_info
        print(f"   last_price: {fast.get('last_price', 'N/A')}")
        print(f"   previous_close: {fast.get('previous_close', 'N/A')}")
        print(f"   open: {fast.get('open', 'N/A')}")
        print(f"   day_high: {fast.get('day_high', 'N/A')}")
        print(f"   day_low: {fast.get('day_low', 'N/A')}")
        print()
        # Mostrar todos os campos disponíveis
        print("   Todos os campos:")
        for key in dir(fast):
            if not key.startswith('_'):
                try:
                    val = getattr(fast, key)
                    if not callable(val):
                        print(f"      {key}: {val}")
                except:
                    pass
    except Exception as e:
        print(f"   Erro: {e}")
    
    print()
    
    # Teste 2: info (mais completo mas mais lento)
    print("2. INFO")
    print("-" * 40)
    try:
        info = ticker.info
        campos_relevantes = [
            'regularMarketPrice', 'regularMarketPreviousClose',
            'regularMarketOpen', 'regularMarketDayHigh', 'regularMarketDayLow',
            'regularMarketTime', 'regularMarketChangePercent'
        ]
        for campo in campos_relevantes:
            print(f"   {campo}: {info.get(campo, 'N/A')}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    print()
    
    # Timestamp local para comparação
    print("3. TIMESTAMP LOCAL")
    print("-" * 40)
    print(f"   Hora local: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("=" * 60)
    print("Compare o 'last_price' com o preço atual em:")
    print("- https://finance.yahoo.com/quote/ES=F")
    print("- https://www.tradingview.com/symbols/CME_MINI-ES1!/")
    print("=" * 60)

if __name__ == "__main__":
    main()
