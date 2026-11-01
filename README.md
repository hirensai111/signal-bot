# Signal Bot

Personal crypto futures scalping signal bot with automated execution on Delta Exchange India.

## Quick Start

1. Copy `.env.example` to `.env` and fill in your credentials.
2. Install dependencies: `pip install -e ".[dev]"`
3. Run tests: `pytest tests/`
4. Start the bot: `python -m signal_bot.main`

## Architecture

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Ingest | `ingest/binance_ws.py` | WebSocket + REST backfill for Binance klines & aggTrades |
| Storage | `storage/writer.py` | Batched DB inserts + closed-candle fan-out |
| Features | `features/htf_levels.py` | Swing high/low detection on 1h/4h |
| Features | `features/microstructure.py` | CVD, volume z-score, ratio per candle |
| Signals | `signals/sweep_detector.py` | 4-confluence sweep setup detection |
| Signals | `signals/risk_sizer.py` | Position sizing & leverage capping |
| Delivery | `delivery/telegram_bot.py` | Signal alerts & status command |
| Tracking | `tracking/outcome_monitor.py` | SL/TP fill monitoring & daily summary |
| Execution | `delta/client.py` | Delta Exchange India REST + WebSocket wrapper |
| Backtest | `backtest/runner.py` | Historical replay with cost model |

## Delta Exchange India Client

The `delta/client.py` module provides:

- **REST API** with HMAC-SHA256 request signing
- **WebSocket** private channel (positions + orders) with auto-reconnect
- **Symbol mapping** between Binance (`BTCUSDT`) and Delta (`BTCUSD`) symbols
- **Bracket order** placement with embedded SL/TP

### Environment Variables

```bash
DELTA_MODE=testnet          # "testnet" or "production"
DELTA_API_KEY=your_key
DELTA_API_SECRET=your_secret
```

### CLI Sanity Check

```bash
python -m signal_bot.delta.client --check
```

### Usage Example

```python
import asyncio
from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        await client.symbol_map.load()
        balance = await client.get_balance()
        positions = await client.get_open_positions()
        print(balance, positions)


asyncio.run(main())
```

## Configuration

All settings are loaded from `.env` via `pydantic-settings`.

See `config.py` for the full list of environment variables.

## Testing

```bash
# Run only Delta client tests (no PostgreSQL required)
pytest tests/test_delta_client.py -v

# Run all tests (requires PostgreSQL running)
pytest tests/ -v
```

## License

Private — for personal use only.
