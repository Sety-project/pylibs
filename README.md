# Optimal CEX Basis portfolio optimizer
This repo contains a set of services to trade CEX basis algorithmically; each is dockerized separately.

Initially this was only designed to backtest the strat on ftx, but later on was expanded to binance, and to execute live.

It's also me learning python, async, etc...
## 1. histfeed (Supports ftx, binance, deribit)
Uses ccxt to get consolidated historical data into csv files (that was before i heard about databases...).
## 2. pfoptimizer (ftx)
Builds an optimal portoflio using SLSQP. Maximizes carry after slippage, under margin and risk contraints.
## 3. riskpnl (ftx)
Computes risk, pnl and pnl attribution for portofolios with spot, perp, futs.
## 4. staticdata
Metadata for CEX.
## 5. tradexecutor
A pretty generic architecture for event-based algo trading, robust to network disconnect and rigorous about order tracking. This was designed to execute spread trades on a z score signal , using a limit order on one leg and and a market order on the other. Strategy has several StrategyEnabler: 

- VenueAPI: interface to each exchange API. Dispatches orderbook updates, implements pegged orders, executes trades upon signals. 

- PositionManager: owns pv,risk,margin.

- OrderManager: manages and records order state transitions. Follows FIX convention even if exchanges doesn't have FIX.

- SignalEngine: computes derived data from venue_api and/or externally generated client orders.
## 6. ux
A telegram bot to operate the trading bots, and jupyter notebook to display backetest, pnl, latency..
