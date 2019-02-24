import arrow
import asyncio
from datetime import datetime

from binance.client import Client
from binance.helpers import date_to_milliseconds

from config import DATE_FORMAT, TIME_FORMAT


def get_btc_symbols(binance_client: Client):
    exchange_info = binance_client.get_exchange_info()

    symbols = []
    for symbol in exchange_info['symbols']:
        symbol = symbol['symbol']
        if symbol.endswith('BTC'):
            symbols.append(symbol)

    symbols = sorted(symbols)
    return symbols


def normalize_row(row):
    ts = datetime.utcfromtimestamp(int(row[6] / 1000))
    dt = arrow.get(ts).to("Europe/Moscow")

    date, time = dt.strftime(DATE_FORMAT), dt.strftime(TIME_FORMAT)

    return (
        date,
        time,
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
    )


sem = asyncio.Semaphore(10)


async def get_exchange_data(binance_client: Client, symbol: str, from_date: str, logger):
    async with sem:
        data = await SymbolData(binance_client, symbol, from_date)
        print(data)
        await asyncio.sleep(3)


class SymbolData:

    def __init__(self, binance_client: Client, symbol: str, from_date: str, to_date=None):
        self.client = binance_client
        self.symbol = symbol
        self.from_date = date_to_milliseconds(from_date)
        self.to_date = to_date

    def _fetch(self):
        data = self.client.get_historical_klines(
            self.symbol,
            self.client.KLINE_INTERVAL_1MINUTE,
            self.from_date,
            self.to_date,
        )
        return data

    def __await__(self):
        loop = asyncio.get_event_loop()
        r = yield from loop.run_in_executor(None, self._fetch)
        return r
