# This single script combines the daily data update, screener analysis, and API server.

import boto3
import pandas as pd
import pyotp
import sys
import time
import json
from decimal import Decimal
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from boto3.dynamodb.conditions import Key
from SmartApi import SmartConnect
from flask import Flask, jsonify
from flask_cors import CORS
import threading

# ==============================================================================
# --- 1. UNIFIED CONFIGURATION ---
# ==============================================================================
AWS_REGION = "ap-south-1"
DATA_TABLE_NAME = "daily_stock_data"
RESULTS_TABLE_NAME = "screener_results"

# Angel One API Credentials
API_KEY = "oNNHQHKU"
CLIENT_CODE = "D355432"
MPIN = "1234"
TOTP_SECRET = "QHO5IWOISV56Z2BFTPFSRSQVRQ"

# --- Development Settings ---
TEST_MODE = True
TEST_STOCK_LIMIT = 200

# ==============================================================================
# --- 2. INITIALIZATION ---
# ==============================================================================

app = Flask(__name__)
CORS(app)

print("Connecting to AWS DynamoDB...", flush=True)
try:
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    data_table = dynamodb.Table(DATA_TABLE_NAME)
    results_table = dynamodb.Table(RESULTS_TABLE_NAME)
    data_table.load()
    results_table.load()
    print("✅ Successfully connected to both DynamoDB tables.", flush=True)
except Exception as e:
    print(f"❌ Could not connect to DynamoDB. Error: {e}", flush=True)
    sys.exit()

smartApi = SmartConnect(API_KEY)

# Locks to prevent concurrent task runs
eod_task_lock = threading.Lock()
intraday_task_lock = threading.Lock()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# ==============================================================================
# --- 3. END-OF-DAY DATA UPDATE LOGIC ---
# ==============================================================================
def run_daily_data_update():
    print("\nStarting daily data update process...", flush=True)
    try:
        print("Connecting to Angel One SmartAPI...", flush=True)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        session_data = smartApi.generateSession(CLIENT_CODE, MPIN, totp)
        if not session_data['status']:
            print(f"❌ Angel One Login Failed: {session_data['message']}", flush=True)
            return False
        else:
            print("✅ Angel One session created successfully.", flush=True)
    except Exception as e:
        print(f"❌ Error during Angel One login: {e}", flush=True)
        return False

    stocks_to_update = []
    print("Scanning data table for existing stocks...", flush=True)
    try:
        if TEST_MODE:
            print(f"⚠️ TEST MODE IS ON. Scanning until {TEST_STOCK_LIMIT} unique stocks are found.", flush=True)
            unique_stocks_map = {}
            scan_kwargs = {}
            while len(unique_stocks_map) < TEST_STOCK_LIMIT:
                response = data_table.scan(ProjectionExpression='instrument_token, symbol', **scan_kwargs)
                items = response.get('Items', [])
                if not items: break

                for item in items:
                    if 'instrument_token' in item and 'symbol' in item:
                        token = int(item['instrument_token'])
                        if token not in unique_stocks_map:
                            unique_stocks_map[token] = item['symbol']
                            if len(unique_stocks_map) >= TEST_STOCK_LIMIT:
                                break
                
                if 'LastEvaluatedKey' in response and len(unique_stocks_map) < TEST_STOCK_LIMIT:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                else:
                    break
            stocks_to_update = [{'token': token, 'symbol': symbol} for token, symbol in unique_stocks_map.items()]
        else:
            print("Scanning for all unique stock tokens...", flush=True)
            all_items = []
            scan_kwargs = {'ProjectionExpression': 'instrument_token, symbol'}
            response = data_table.scan(**scan_kwargs)
            all_items.extend(response.get('Items', []))
            while 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = data_table.scan(**scan_kwargs)
                all_items.extend(response.get('Items', []))
            
            processed_tokens = set()
            for item in all_items:
                if 'instrument_token' in item and 'symbol' in item:
                    token = int(item['instrument_token'])
                    if token not in processed_tokens:
                        stocks_to_update.append({'token': token, 'symbol': item['symbol']})
                        processed_tokens.add(token)
        
        if not stocks_to_update:
            print("⚠️ No stocks found in DB to update.", flush=True)
            return False
        print(f"✅ Found {len(stocks_to_update)} unique stocks to update.", flush=True)

    except Exception as e:
        print(f"❌ Error scanning DynamoDB for stocks: {e}", flush=True)
        return False

    latest_trading_day = None
    print("Finding the most recent trading day...", flush=True)

    test_stocks = stocks_to_update[:5]
    print(f"  -> DEBUG: Will attempt to find date using up to {len(test_stocks)} stocks: {[s['symbol'] for s in test_stocks]}", flush=True)

    # --- MODIFIED: More robust date-finding logic based on wider date range query ---
    for test_stock in test_stocks:
        print(f"\n--- Attempting with test stock: {test_stock['symbol']} ---", flush=True)
        try:
            # We check a wide range (15 days) as the API is more reliable with this.
            to_date_check = datetime.now()
            from_date_check = to_date_check - timedelta(days=15)
            
            hist_params = {
                "exchange": "NSE", 
                "symboltoken": str(test_stock['token']), 
                "interval": "ONE_DAY", 
                "fromdate": f"{from_date_check.strftime('%Y-%m-%d')} 09:15", 
                "todate": f"{to_date_check.strftime('%Y-%m-%d')} 15:30"
            }
            
            print(f"  -> DEBUG: API Request Params: {hist_params}", flush=True)
            api_response = smartApi.getCandleData(hist_params)
            print(f"  -> DEBUG: API Response: {api_response}", flush=True)

            if api_response and api_response.get('status') and api_response.get('data'):
                # The last candle in the response is the latest trading day
                last_candle_str = api_response['data'][-1][0]
                latest_trading_day = datetime.strptime(last_candle_str.split('T')[0], '%Y-%m-%d')
                print(f"✅ Success! Latest trading day is {latest_trading_day.strftime('%Y-%m-%d')} (found using {test_stock['symbol']})", flush=True)
                break # Exit the loop once we've found the date
            else:
                print(f"  -> INFO: No data returned for {test_stock['symbol']}, trying next stock.", flush=True)
            
            time.sleep(0.4)

        except Exception as e:
            print(f"  -> DEBUG: An exception occurred during API call for {test_stock['symbol']}: {e}", flush=True)
            continue # Try the next stock
    
    if not latest_trading_day:
        print("❌ Could not determine latest trading day after trying all test stocks.", flush=True)
        return False

    from_date = latest_trading_day.strftime('%Y-%m-%d 09:15')
    to_date = latest_trading_day.strftime('%Y-%m-%d 15:30')
    date_checked = latest_trading_day.strftime('%Y-%m-%d')
    print(f"\nFetching all stock data for the confirmed latest trading day: {date_checked}...", flush=True)
    
    with data_table.batch_writer() as batch:
        for index, stock in enumerate(stocks_to_update):
            print(f"  -> [{index + 1}/{len(stocks_to_update)}] Fetching {stock['symbol']}...", flush=True)
            try:
                # Now we query for the single specific day we found
                hist_params = {"exchange": "NSE", "symboltoken": str(stock['token']), "interval": "ONE_DAY", "fromdate": from_date, "todate": to_date}
                api_response = smartApi.getCandleData(hist_params)
                if api_response and api_response.get('status') and api_response.get('data'):
                    candle = api_response['data'][0]
                    batch.put_item(Item={
                        'instrument_token': stock['token'], 'date': candle[0].split('T')[0],
                        'symbol': stock['symbol'], 'open': Decimal(str(candle[1])),
                        'high': Decimal(str(candle[2])), 'low': Decimal(str(candle[3])),
                        'close': Decimal(str(candle[4])), 'volume': int(candle[5])
                    })
                time.sleep(0.4)
            except Exception as e:
                print(f"     -> ERROR for {stock['symbol']}: {e}", flush=True)
    
    print("✅ Daily data update complete!", flush=True)
    return True

# ==============================================================================
# --- 4. END-OF-DAY SCREENER ANALYSIS LOGIC ---
# ==============================================================================
def _format_result_eod(latest, previous):
    return {"symbol": latest['symbol'], "changePct": ((latest['close'] - previous['close']) / previous['close']) * 100, "price": latest['close'], "volume": int(latest['volume'])}

# --- All End-of-Day screener functions ---
# (Note: Functions are collapsed for brevity)
def run_screener_near_ath(all_tokens):
    screener_id = 'near_ath'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 22: continue
            df = pd.DataFrame(stock_data)
            all_time_high = df['high'].max()
            latest = df.sort_values(by='date', ascending=False).iloc[0]
            previous = df.sort_values(by='date', ascending=False).iloc[1]
            if latest['close'] >= (all_time_high * Decimal('0.75')): final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_recent_ipos(all_tokens): return 'recent_ipos', []

def run_screener_above_200_sma(all_tokens):
    screener_id = 'above_200_sma'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 200: continue
            stock_data.sort(key=lambda x: x['date'])
            df = pd.DataFrame(stock_data)
            df['sma_200'] = df['close'].rolling(window=200).mean()
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            if pd.notna(latest['sma_200']) and latest['close'] > latest['sma_200']: final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_multi_year_breakout(all_tokens):
    screener_id = 'multi_year_breakout'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 365: continue
            df = pd.DataFrame(stock_data).sort_values(by='date')
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            breakout_level = df.iloc[:-22]['high'].max() 
            if latest['close'] > breakout_level: final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_monthly_high_breakout(all_tokens):
    screener_id = 'monthly_high_breakout'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    today = date.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_prev_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_prev_month = last_day_of_prev_month.replace(day=1)
    for token in all_tokens:
        try:
            start_date_str = first_day_of_prev_month.strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 22: continue
            df = pd.DataFrame(stock_data)
            df['date_dt'] = pd.to_datetime(df['date'])
            prev_month_df = df[df['date_dt'].dt.month == first_day_of_prev_month.month]
            if prev_month_df.empty: continue
            prev_month_high = prev_month_df['high'].max()
            latest = df.sort_values(by='date', ascending=False).iloc[0]
            previous = df.sort_values(by='date', ascending=False).iloc[1]
            if latest['close'] > prev_month_high: final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_five_pct_breakout(all_tokens):
    screener_id = 'five_pct_breakout'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token), ScanIndexForward=False, Limit=2)
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 2: continue
            latest = stock_data[0]
            previous = stock_data[1]
            if latest['open'] > (previous['close'] * Decimal('1.05')): final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_monthly_inside_candle(all_tokens):
    screener_id = 'monthly_inside_candle'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    today = date.today()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    for token in all_tokens:
        try:
            start_date_str = (first_day_prev_month - relativedelta(months=1)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 40: continue
            df = pd.DataFrame(stock_data)
            df['date_dt'] = pd.to_datetime(df['date'])
            current_month_df = df[df['date_dt'].dt.month == today.month]
            prev_month_df = df[df['date_dt'].dt.month == first_day_prev_month.month]
            if current_month_df.empty or prev_month_df.empty: continue
            current_high, current_low = current_month_df['high'].max(), current_month_df['low'].min()
            prev_high, prev_low = prev_month_df['high'].max(), prev_month_df['low'].min()
            if current_high < prev_high and current_low > prev_low:
                latest = df.sort_values(by='date', ascending=False).iloc[0]
                previous = df.sort_values(by='date', ascending=False).iloc[1]
                final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_tight_weekly_base(all_tokens):
    screener_id = 'tight_weekly_base'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(weeks=10)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 25: continue
            df = pd.DataFrame(stock_data)
            df.set_index(pd.to_datetime(df['date']), inplace=True)
            weekly_df = df.resample('W').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
            if len(weekly_df) < 4: continue
            last_3_weeks = weekly_df.iloc[-3:].copy()
            last_3_weeks['range_pct'] = (last_3_weeks['high'] - last_3_weeks['low']) / last_3_weeks['low']
            if (last_3_weeks['range_pct'] < Decimal('0.1')).all():
                latest = df.iloc[-1]
                previous = df.iloc[-2]
                final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_screener_tight_daily_base(all_tokens):
    screener_id = 'tight_daily_base'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token), ScanIndexForward=False, Limit=10)
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 10: continue
            df = pd.DataFrame(stock_data)
            if (df['high'].max() - df['low'].min()) / df['low'].min() < Decimal('0.08'):
                final_results.append(_format_result_eod(stock_data[0], stock_data[1]))
        except Exception: continue
    return screener_id, final_results

def run_screener_low_of_highest_up_candle(all_tokens):
    screener_id = 'low_of_highest_up_candle'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 22: continue
            df = pd.DataFrame(stock_data)
            df['gain_pct'] = (df['close'] - df['open']) / df['open']
            support_level = df.loc[df['gain_pct'].idxmax()]['low']
            latest = df.sort_values(by='date', ascending=False).iloc[0]
            if latest['close'] <= (support_level * Decimal('1.02')):
                final_results.append(_format_result_eod(latest, df.sort_values(by='date', ascending=False).iloc[1]))
        except Exception: continue
    return screener_id, final_results

def run_screener_low_of_high_volume_candle(all_tokens):
    screener_id = 'low_of_high_volume_candle'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 22: continue
            df = pd.DataFrame(stock_data)
            support_level = df.loc[df['volume'].idxmax()]['low']
            latest = df.sort_values(by='date', ascending=False).iloc[0]
            if latest['close'] <= (support_level * Decimal('1.02')):
                final_results.append(_format_result_eod(latest, df.sort_values(by='date', ascending=False).iloc[1]))
        except Exception: continue
    return screener_id, final_results

def run_screener_low_of_3_pct_down_candle(all_tokens):
    screener_id = 'low_of_3_pct_down_candle'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 22: continue
            df = pd.DataFrame(stock_data).sort_values(by='date')
            df['change_pct'] = (df['close'] - df['open']) / df['open']
            down_candles = df[df['change_pct'] <= Decimal('-0.03')]
            if down_candles.empty: continue
            support_level = down_candles.iloc[-1]['low']
            latest = df.iloc[-1]
            if latest['close'] <= (support_level * Decimal('1.02')):
                final_results.append(_format_result_eod(latest, df.iloc[-2]))
        except Exception: continue
    return screener_id, final_results

def run_screener_above_10_sma(all_tokens):
    screener_id = 'above_10_sma'
    print(f"--- Running Screener: {screener_id} ---", flush=True)
    final_results = []
    for token in all_tokens:
        try:
            start_date_str = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date_str))
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 10: continue
            stock_data.sort(key=lambda x: x['date'])
            df = pd.DataFrame(stock_data)
            df['sma_10'] = df['close'].rolling(window=10).mean()
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            if pd.notna(latest['sma_10']) and latest['close'] > latest['sma_10']:
                final_results.append(_format_result_eod(latest, previous))
        except Exception: continue
    return screener_id, final_results

def run_all_eod_screeners():
    print("\nStarting all end-of-day screener analyses...", flush=True)
    try:
        all_tokens = set()
        if TEST_MODE:
            print(f"⚠️ TEST MODE IS ON. Scanning until {TEST_STOCK_LIMIT} unique stocks are found.", flush=True)
            scan_kwargs = {}
            while len(all_tokens) < TEST_STOCK_LIMIT:
                response = data_table.scan(ProjectionExpression='instrument_token', **scan_kwargs)
                items = response.get('Items', [])
                if not items: break
                for item in items:
                    if 'instrument_token' in item:
                        all_tokens.add(int(item['instrument_token']))
                        if len(all_tokens) >= TEST_STOCK_LIMIT:
                            break
                if 'LastEvaluatedKey' in response and len(all_tokens) < TEST_STOCK_LIMIT:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                else:
                    break
        else: # Full Scan
            print("Scanning for all unique stock tokens...", flush=True)
            scan_kwargs = {'ProjectionExpression': 'instrument_token'}
            response = data_table.scan(**scan_kwargs)
            all_items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = data_table.scan(**scan_kwargs)
                all_items.extend(response.get('Items', []))
            all_tokens = {int(item['instrument_token']) for item in all_items}

        print(f"Found {len(all_tokens)} unique stocks to analyze.", flush=True)

        eod_screener_functions = [
            run_screener_near_ath, run_screener_recent_ipos, run_screener_above_200_sma,
            run_screener_multi_year_breakout, run_screener_monthly_high_breakout,
            run_screener_five_pct_breakout, run_screener_monthly_inside_candle,
            run_screener_tight_weekly_base, run_screener_tight_daily_base,
            run_screener_low_of_highest_up_candle, run_screener_low_of_high_volume_candle,
            run_screener_low_of_3_pct_down_candle, run_screener_above_10_sma,
        ]

        with results_table.batch_writer() as batch:
            for func in eod_screener_functions:
                screener_id, results = func(all_tokens)
                if results is not None:
                    print(f"  -> {screener_id} found {len(results)} matching stocks.", flush=True)
                    batch.put_item(Item={
                        'screener_name': screener_id,
                        'results': results,
                        'last_updated': datetime.now().isoformat()
                    })
        
        print("\n✅ All EOD screeners finished and results saved.", flush=True)
        return True
    except Exception as e:
        print(f"❌ An error occurred during the main EOD screener run: {e}", flush=True)
        return False

# ==============================================================================
# --- 5. NEW: INTRADAY SCREENER ANALYSIS LOGIC ---
# ==============================================================================
def _format_result_intraday(symbol, candle_5min):
    return {
        "symbol": symbol,
        "price": Decimal(str(candle_5min[4])), # Close price
        "volume": int(candle_5min[5]),
        "changePct": ((Decimal(str(candle_5min[4])) - Decimal(str(candle_5min[1]))) / Decimal(str(candle_5min[1]))) * 100 # Change from open
    }

def run_intraday_screeners():
    print("\nStarting INTRADAY screener analysis...", flush=True)
    if not intraday_task_lock.acquire(blocking=False):
        print("An intraday task is already running. Skipping.", flush=True)
        return

    try:
        # --- Authenticate with Angel One ---
        print("Connecting to Angel One SmartAPI for intraday data...", flush=True)
        try:
            totp = pyotp.TOTP(TOTP_SECRET).now()
            session_data = smartApi.generateSession(CLIENT_CODE, MPIN, totp)
            if not session_data['status']:
                print(f"❌ Angel One Login Failed: {session_data['message']}", flush=True)
                return
            print("✅ Angel One session created successfully.", flush=True)
        except Exception as e:
            print(f"❌ Error during Angel One login: {e}", flush=True)
            return

        all_stocks = []
        if TEST_MODE:
            print(f"⚠️ TEST MODE IS ON. Scanning until {TEST_STOCK_LIMIT} unique stocks are found.", flush=True)
            unique_stocks_map = {}
            scan_kwargs = {}
            while len(unique_stocks_map) < TEST_STOCK_LIMIT:
                response = data_table.scan(ProjectionExpression='instrument_token, symbol', **scan_kwargs)
                items = response.get('Items', [])
                if not items: break
                for item in items:
                    if 'instrument_token' in item and 'symbol' in item:
                        token = int(item['instrument_token'])
                        if token not in unique_stocks_map:
                            unique_stocks_map[token] = item['symbol']
                            if len(unique_stocks_map) >= TEST_STOCK_LIMIT:
                                break
                if 'LastEvaluatedKey' in response and len(unique_stocks_map) < TEST_STOCK_LIMIT:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                else:
                    break
            all_stocks = [{'token': token, 'symbol': symbol} for token, symbol in unique_stocks_map.items()]
        else: # Full scan
            print("Scanning for all unique stock tokens...", flush=True)
            scan_kwargs = {'ProjectionExpression': 'instrument_token, symbol'}
            response = data_table.scan(**scan_kwargs)
            all_items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = data_table.scan(**scan_kwargs)
                all_items.extend(response.get('Items', []))
            
            unique_stocks_map = {}
            for item in all_items:
                if 'instrument_token' in item and 'symbol' in item:
                    token = int(item['instrument_token'])
                    if token not in unique_stocks_map:
                        unique_stocks_map[token] = item['symbol']
            all_stocks = [{'token': token, 'symbol': symbol} for token, symbol in unique_stocks_map.items()]

        print(f"Found {len(all_stocks)} unique stocks to analyze for intraday screeners.", flush=True)

        # --- Initialize results lists ---
        open_low_results = []
        open_high_results = []
        orh_breakout_results = []

        # --- Set time for the first 5-min candle ---
        today_str = datetime.now().strftime('%Y-%m-%d')
        from_date = f"{today_str} 09:15"
        to_date = f"{today_str} 09:20"

        # --- Loop through stocks and perform checks ---
        for index, stock in enumerate(all_stocks):
            print(f"  -> [{index + 1}/{len(all_stocks)}] Analyzing {stock['symbol']}...", flush=True)
            try:
                # 1. Fetch the first 5-minute candle of the day
                hist_params = {"exchange": "NSE", "symboltoken": str(stock['token']), "interval": "FIVE_MINUTE", "fromdate": from_date, "todate": to_date}
                api_response = smartApi.getCandleData(hist_params)
                
                if not (api_response and api_response.get('status') and api_response.get('data')):
                    time.sleep(0.4)
                    continue
                
                first_candle = api_response['data'][0]
                candle_open = Decimal(str(first_candle[1]))
                candle_high = Decimal(str(first_candle[2]))
                candle_low = Decimal(str(first_candle[3]))
                candle_close = Decimal(str(first_candle[4]))

                # 2. Check for Open=Low and Open=High
                if candle_open == candle_low:
                    open_low_results.append(_format_result_intraday(stock['symbol'], first_candle))
                if candle_open == candle_high:
                    open_high_results.append(_format_result_intraday(stock['symbol'], first_candle))

                # 3. Check for ORH Breakout
                # Get the highs of the previous 3 trading days from our database
                prev_days_data = data_table.query(
                    KeyConditionExpression=Key('instrument_token').eq(stock['token']),
                    ScanIndexForward=False, # Sort by date descending
                    Limit=3
                )
                if prev_days_data.get('Count', 0) == 3:
                    prev_highs = [item['high'] for item in prev_days_data['Items']]
                    resistance_level = max(prev_highs)
                    if candle_close > resistance_level:
                        orh_breakout_results.append(_format_result_intraday(stock['symbol'], first_candle))

                time.sleep(0.4) # API rate limit

            except Exception as e:
                print(f"     -> ERROR for {stock['symbol']}: {e}", flush=True)
                continue
        
        # --- Save results to DynamoDB ---
        print("Saving intraday screener results...", flush=True)
        with results_table.batch_writer() as batch:
            batch.put_item(Item={'screener_name': 'intraday_open_low', 'results': open_low_results, 'last_updated': datetime.now().isoformat()})
            batch.put_item(Item={'screener_name': 'intraday_open_high', 'results': open_high_results, 'last_updated': datetime.now().isoformat()})
            batch.put_item(Item={'screener_name': 'intraday_orh_breakout', 'results': orh_breakout_results, 'last_updated': datetime.now().isoformat()})
        print("✅ Intraday screeners finished and results saved.", flush=True)

    finally:
        intraday_task_lock.release()

# ==============================================================================
# --- 6. API SERVER LOGIC ---
# ==============================================================================
@app.route('/api/screeners/<screener_id>', methods=['GET'])
def get_screener_results(screener_id):
    """A single, dynamic endpoint to fetch results for any screener."""
    if not screener_id:
        return jsonify({"error": "Screener ID is required"}), 400
    
    print(f"API request received for screener: {screener_id}", flush=True)
    try:
        response = results_table.get_item(Key={'screener_name': screener_id})
        item = response.get('Item')
        if item:
            return app.response_class(
                response=json.dumps(item, cls=DecimalEncoder),
                status=200,
                mimetype='application/json'
            )
        else:
            return jsonify({'screener_name': screener_id, 'results': [], 'last_updated': datetime.now().isoformat()}), 200
    except Exception as e:
        print(f"API Error for {screener_id}: {e}", flush=True)
        return jsonify({"error": "An internal server error occurred"}), 500

# ==============================================================================
# --- 7. SCHEDULING & EXECUTION ---
# ==============================================================================
def run_eod_tasks():
    if not eod_task_lock.acquire(blocking=False):
        print("An EOD task is already running. Skipping this trigger.", flush=True)
        return
    try:
        print("="*50, flush=True)
        print(f"Starting scheduled EOD tasks at {datetime.now()}", flush=True)
        print("="*50, flush=True)
        update_success = run_daily_data_update()
        if update_success:
            run_all_eod_screeners()
        else:
            print("Skipping EOD screener analysis due to data update failure.", flush=True)
        print("\nAll scheduled EOD tasks finished.", flush=True)
    finally:
        eod_task_lock.release()

@app.route('/run-eod-tasks', methods=['POST', 'GET'])
def trigger_eod_tasks():
    if eod_task_lock.locked():
        return jsonify({"status": "EOD tasks are already in progress."}), 429
    task_thread = threading.Thread(target=run_eod_tasks)
    task_thread.start()
    return jsonify({"status": "EOD tasks triggered successfully in the background."}), 202

@app.route('/run-intraday-tasks', methods=['POST', 'GET'])
def trigger_intraday_tasks():
    if intraday_task_lock.locked():
        return jsonify({"status": "Intraday tasks are already in progress."}), 429
    task_thread = threading.Thread(target=run_intraday_screeners)
    task_thread.start()
    return jsonify({"status": "Intraday tasks triggered successfully in the background."}), 202

if __name__ == '__main__':
    print("Starting the main application...", flush=True)
    print("Triggering initial EOD data update and screener analysis in the background...", flush=True)
    initial_eod_thread = threading.Thread(target=run_eod_tasks)
    initial_eod_thread.start()
    app.run(host='0.0.0.0', port=5000)

