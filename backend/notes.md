1. fetch API
2. process data
3. ingest to MiniIO

The Recommended Gameplan

    Start with Yahoo Finance (yfinance library): Pull historical OHLCV prices, sector classifications, and quarterly earnings.

    Land Raw JSON in MinIO (Bronze): Partition by year=YYYY/month=MM/day=DD.

    Run a Transformation Job (Silver): Convert Bronze JSONs into partitioned Parquet files using Python (Polars or Pandas).

    (Optional Phase 2) Add CoinGecko: Pull Bitcoin/Ethereum historical data and blend it into your Silver/Gold layers to run cross-market analytics.
