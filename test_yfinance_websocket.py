"""
Teste do WebSocket do Yahoo Finance para E-mini S&P 500 Futures (ES=F)
Objetivo: verificar se funciona, qual o formato dos dados, e se é tempo real
"""

import yfinance as yf
from datetime import datetime

def message_handler(message):
    """Callback que processa cada mensagem recebida"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {message}")
    print("-" * 60)

def main():
    print("=" * 60)
    print("TESTE - Yahoo Finance WebSocket")
    print("Ticker: ES=F (E-mini S&P 500 Futures)")
    print("=" * 60)
    print()
    print("Conectando ao WebSocket...")
    print("(Pressione Ctrl+C para encerrar)")
    print()
    
    try:
        with yf.WebSocket(verbose=True) as ws:
            ws.subscribe(["ES=F"])
            ws.listen(message_handler)
    except KeyboardInterrupt:
        print("\n\nEncerrado pelo usuário.")
    except Exception as e:
        print(f"\nErro: {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
