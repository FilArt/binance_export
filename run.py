import asyncio
import logging
from datetime import datetime

import gspread
from binance.client import Client
from config import (API_KEY, API_SECRET, CREDS_PATH, DOCUMENT_NAME,
                    MAX_CONCURRENCY_BINANCE_TASK,
                    MAX_CONCURRENCY_GOOGLE_SHEET_TASK, SHARE_TO_EMAIL,
                    UPDATE_INTERVAL)
from exchange.data import SymbolData, get_btc_symbols, normalize_row
from gspread import SpreadsheetNotFound, WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

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

try:
    spread_sheet = google_client.open(DOCUMENT_NAME)
except SpreadsheetNotFound:
    spread_sheet = google_client.create(DOCUMENT_NAME)

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


def _create_request(s_id, rows):
    requests = []
    for row in rows:
        requests.extend((
            {'insertRange': {
                'range': {
                    "sheetId": s_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 7,
                },
                'shiftDimension': 'ROWS',
            }},
            {'pasteData': {
                'coordinate': {
                    'sheetId': s_id,
                    'rowIndex': 0,
                    'columnIndex': 0,
                },
                'data': '_'.join([str(item) for item in normalize_row(row)]),
                'type': 'PASTE_NORMAL',
                'delimiter': '_',
            }}
        ))
    return requests


def export_data(worksheets, exchange_data):
    request_body = []
    for s, data in exchange_data:
        request_body.extend(_create_request(worksheets[s].id, data))

    batch_requests = {'requests': request_body}

    spread_sheet.batch_update(batch_requests)


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
