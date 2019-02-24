import asyncio
import logging

import gspread
from gspread import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

from binance.client import Client

from exchange.data import get_btc_symbols, SymbolData, normalize_row
from config import (
    API_KEY, API_SECRET, MAX_CONCURRENCY_BINANCE_TASK, MAX_CONCURRENCY_GOOGLE_SHEET_TASK, CREDS_PATH, UPDATE_INTERVAL,
    DOCUMENT_NAME,
    SHARE_TO_EMAIL)

logging.basicConfig(format='%(levelname)s - %(asctime)s - %(name)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('main')

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDS_PATH,
    scope,
)

google_client = gspread.authorize(credentials)

binance_client = Client(API_KEY, API_SECRET)

symbols = get_btc_symbols(binance_client)

spread_sheet = google_client.open(DOCUMENT_NAME)
spread_sheet.share(SHARE_TO_EMAIL, perm_type='user', role='writer')


async def process_symbol(symbol, b_sem):
    await b_sem.acquire()
    data = await SymbolData(binance_client, symbol, '1 min ago UTC')
    logger.info('{} processed'.format(symbol))

    b_sem.release()
    return symbol, data


async def prepare_worksheet(symbol, sem):
    await sem.acquire()

    try:
        worksheet = spread_sheet.worksheet(symbol)
        logger.info('{} opened'.format(symbol))
    except WorksheetNotFound:
        worksheet = spread_sheet.add_worksheet(symbol, rows=0, cols=7)
        logger.info('{} created'.format(symbol))
    await asyncio.sleep(10)
    sem.release()
    return symbol, worksheet


def _create_rows(rows):
    result = []
    for row in rows:
        result.append({
            'values': [
                {'userEnteredValue': {'stringValue': str(v)}}
                for v in normalize_row(row)
            ]
        })
    return result


def _create_request(s_id, rows):
    return {'appendCells': {
        'sheetId': s_id,
        'rows': _create_rows(rows),
        'fields': '*',
    }}


def export_data(worksheets, exchange_data):
    requests = {'requests': [
        _create_request(worksheets[s].id, data)
        for s, data in exchange_data
    ]}
    spread_sheet.batch_update(requests)


async def main():
    binance_semaphore = asyncio.Semaphore(MAX_CONCURRENCY_BINANCE_TASK)
    google_semaphore = asyncio.Semaphore(MAX_CONCURRENCY_GOOGLE_SHEET_TASK)

    worksheets = [
        prepare_worksheet(symbol, google_semaphore)

        for symbol in symbols
    ]

    worksheets = await asyncio.gather(*worksheets)
    worksheets = dict(worksheets)

    while True:
        logger.info('Exporting data to Google sheets...')
        loop = asyncio.get_event_loop()
        start_time = loop.time()

        binance_tasks = [
            process_symbol(symbol, binance_semaphore)
            for symbol in symbols
        ]

        exchange_data = await asyncio.gather(*binance_tasks)
        export_data(worksheets, exchange_data)
        logger.info('Data exported to Google Sheets!')

        end_time = loop.time()

        extra_time = start_time + UPDATE_INTERVAL - end_time

        if extra_time > 0:
            logger.info('Next update will be in {} seconds'.format(extra_time))
            await asyncio.sleep(extra_time)


logger.info('Start!')

asyncio.run(main())
