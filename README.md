# (OLD DRAFT) Multi Asset Trading Bot Environment

This repository contains a high frequency trading bot (Misso) that I wrote in Python.
It was customized for FTX and since their bankruptcy, I have not updated it to work with any other exchange.
Since I am no longer using this bot, I am making it public for anyone to use.
It is based on ccxt library so in theory it can easily be adapted to work with any exchange.
But I have seen there are some custom logic how Exchanges actualy open and close orders, so it will not work out of the box.

## Remarks:
Because this project was never finished and as it is not intended for public use,
it is not well documented and not commented. It was firstly written as learning project and got more attention as it actually worked.


## Performance:
While this bot was functional, it won the first price on Trader Make Money for two months in a row.
It was stable and profitable, but never tested with large amounts of capital.


## Strategies and design:

There are two main bot designs in this repository. Despite what I have seen in other projects this 'TradingEnvironment's
are designed to be used with multiple assets >50. Where other projects are focused on developing and testing a strategy for each asset,
the strategies in this project are designed to dynamically select assets on a set of criteria and open trades.
So the main work is done in managing risk while having a large number of open trades. Each trade should be closed as fast as possible.
While markets are not in your favor, the bot will be dca negative positions based on vwap bands and exponential position sizing.
