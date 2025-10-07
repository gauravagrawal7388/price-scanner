# This single script combines the daily data update, screener analysis, and API server.
# MODIFIED: Now includes a self-contained scheduler for automated task execution.

import boto3
import pandas as pd
import pyotp
import sys
import time
import json
import random
from decimal import Decimal
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
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
INSTRUMENT_TABLE_NAME = "instrument_list" 

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

print("Connecting to AWS DynamoDB...")
try:
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    data_table = dynamodb.Table(DATA_TABLE_NAME)
    results_table = dynamodb.Table(RESULTS_TABLE_NAME)
    instrument_table = dynamodb.Table(INSTRUMENT_TABLE_NAME)
    data_table.load()
    results_table.load()
    instrument_table.load()
    print("✅ Successfully connected to all DynamoDB tables.")
except Exception as e:
    print(f"❌ Could not connect to DynamoDB. Error: {e}")
    sys.exit()

smartApi = SmartConnect(API_KEY)

# Locks to prevent concurrent task runs
eod_task_lock = threading.Lock()
intraday_task_lock = threading.Lock()
symbol_map_lock = threading.Lock()
# ADDED: A lock to ensure only one thread tries to log in at a time.
api_session_lock = threading.Lock()

# Global map for fast symbol-to-token lookups
symbol_to_token_map = {}

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# ==============================================================================
# --- 3. API SESSION MANAGEMENT ---
# ==============================================================================

def ensure_api_session():
    """
    Checks if the SmartAPI session is active and creates a new one if not.
    This function is thread-safe to prevent multiple login attempts at once.
    """
    with api_session_lock:
        # A simple check: if access_token is None, the session is definitely not valid.
        # A more robust check for a real-world scenario might involve checking a token's expiry time.
        if smartApi.access_token is None:
            print("API session is invalid or expired. Attempting to log in...")
            try:
                totp = pyotp.TOTP(TOTP_SECRET).now()
                session_data = smartApi.generateSession(CLIENT_CODE, MPIN, totp)
                if not session_data['status']:
                    print(f"❌ Angel One Login Failed within ensure_api_session: {session_data['message']}")
                    return False
                else:
                    print("✅ New API session created successfully.")
                    return True
            except Exception as e:
                print(f"❌ Error during Angel One login within ensure_api_session: {e}")
                return False
        else:
            # If a token exists, we assume it's valid for this session.
            print("DEBUG: API session appears to be valid.")
            return True

# ==============================================================================
# --- 4. END-OF-DAY DATA UPDATE LOGIC ---
# ==============================================================================
def run_daily_data_update():
    print("\nStarting daily data update process...")
    
    # MODIFIED: Use the centralized login function.
    if not ensure_api_session():
        print("Skipping daily data update due to API login failure.")
        return False

    stocks_to_update = []
    print(f"Scanning '{INSTRUMENT_TABLE_NAME}' for stocks to update...")
    try:
        response = instrument_table.scan()
        all_instruments = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = instrument_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            all_instruments.extend(response.get('Items', []))

        stocks_to_update = [{'token': int(item['instrument_token']), 'symbol': item['symbol']} for item in all_instruments]
        
        if TEST_MODE:
            print(f"⚠️ TEST MODE IS ON. Using a subset of {TEST_STOCK_LIMIT} stocks.")
            stocks_to_update = stocks_to_update[:TEST_STOCK_LIMIT]

        if not stocks_to_update:
            print(f"⚠️ No stocks found in '{INSTRUMENT_TABLE_NAME}' to update.")
            return False
        print(f"✅ Found {len(stocks_to_update)} unique stocks to update.")

    except Exception as e:
        print(f"❌ Error scanning DynamoDB for stocks: {e}")
        return False

    latest_trading_day = None
    print("Finding the most recent trading day...")
    test_stocks = stocks_to_update[:5]

    for test_stock in test_stocks:
        try:
            to_date_check = datetime.now()
            from_date_check = to_date_check - timedelta(days=15)
            hist_params = {"exchange": "NSE", "symboltoken": str(test_stock['token']), "interval": "ONE_DAY", "fromdate": f"{from_date_check.strftime('%Y-%m-%d')} 09:15", "todate": f"{to_date_check.strftime('%Y-%m-%d')} 15:30"}
            api_response = smartApi.getCandleData(hist_params)

            if api_response and api_response.get('status') and api_response.get('data'):
                last_candle_str = api_response['data'][-1][0]
                latest_trading_day = datetime.strptime(last_candle_str.split('T')[0], '%Y-%m-%d')
                print(f"✅ Success! Latest trading day is {latest_trading_day.strftime('%Y-%m-%d')}")
                break
            time.sleep(0.4)
        except Exception as e:
            print(f"  -> DEBUG: An exception occurred during API call for {test_stock['symbol']}: {e}")
            continue
    
    if not latest_trading_day:
        print("❌ Could not determine latest trading day.")
        return False

    from_date = latest_trading_day.strftime('%Y-%m-%d 09:15')
    to_date = latest_trading_day.strftime('%Y-%m-%d 15:30')
    print(f"\nFetching all stock data for the confirmed latest trading day: {to_date.split(' ')[0]}...")
    
    with data_table.batch_writer() as batch:
        for index, stock in enumerate(stocks_to_update):
            print(f"  -> [{index + 1}/{len(stocks_to_update)}] Fetching {stock['symbol']}...")
            try:
                hist_params = {"exchange": "NSE", "symboltoken": str(stock['token']), "interval": "ONE_DAY", "fromdate": from_date, "todate": to_date}
                api_response = smartApi.getCandleData(hist_params)
                if api_response and api_response.get('status') and api_response.get('data'):
                    candle = api_response['data'][0]
                    batch.put_item(Item={'instrument_token': stock['token'], 'date': candle[0].split('T')[0], 'symbol': stock['symbol'], 'open': Decimal(str(candle[1])), 'high': Decimal(str(candle[2])), 'low': Decimal(str(candle[3])), 'close': Decimal(str(candle[4])), 'volume': int(candle[5])})
                time.sleep(0.4)
            except Exception as e:
                print(f"     -> ERROR for {stock['symbol']}: {e}")
    
    print("✅ Daily data update complete!")
    return True

# ==============================================================================
# --- 5. END-OF-DAY SCREENER ANALYSIS LOGIC ---
# ==============================================================================
def _format_result_eod(latest, previous):
    return {"symbol": latest['symbol'], "changePct": ((latest['close'] - previous['close']) / previous['close']) * 100, "price": latest['close'], "volume": int(latest['volume'])}

def run_screener_near_ath(all_tokens):
    screener_id = 'near_ath'
    final_results = []
    for token in all_tokens:
        try:
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token))
            data = res.get('Items', [])
            if len(data) < 22: continue
            df = pd.DataFrame(data)
            ath = df['high'].max()
            latest, prev = df.sort_values(by='date', ascending=False).iloc[0], df.sort_values(by='date', ascending=False).iloc[1]
            if latest['close'] >= (ath * Decimal('0.75')): final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_recent_ipos(all_tokens): return 'recent_ipos', []

def run_screener_above_200_sma(all_tokens):
    screener_id = 'above_200_sma'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 200: continue
            data.sort(key=lambda x: x['date'])
            df = pd.DataFrame(data)
            df['sma_200'] = df['close'].rolling(window=200).mean()
            latest, prev = df.iloc[-1], df.iloc[-2]
            if pd.notna(latest['sma_200']) and latest['close'] > latest['sma_200']: final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_multi_year_breakout(all_tokens):
    screener_id = 'multi_year_breakout'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 365: continue
            df = pd.DataFrame(data).sort_values(by='date')
            latest, prev = df.iloc[-1], df.iloc[-2]
            level = df.iloc[:-22]['high'].max() 
            if latest['close'] > level: final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_monthly_high_breakout(all_tokens):
    screener_id = 'monthly_high_breakout'
    final_results = []
    today = date.today()
    first_day = today.replace(day=1)
    last_day_prev = first_day - timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    for token in all_tokens:
        try:
            start_date = first_day_prev.strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 22: continue
            df = pd.DataFrame(data)
            df['date_dt'] = pd.to_datetime(df['date'])
            prev_month_df = df[df['date_dt'].dt.month == first_day_prev.month]
            if prev_month_df.empty: continue
            high = prev_month_df['high'].max()
            latest, prev = df.sort_values(by='date', ascending=False).iloc[0], df.sort_values(by='date', ascending=False).iloc[1]
            if latest['close'] > high: final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_five_pct_breakout(all_tokens):
    screener_id = 'five_pct_breakout'
    final_results = []
    for token in all_tokens:
        try:
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token), ScanIndexForward=False, Limit=2)
            data = res.get('Items', [])
            if len(data) < 2: continue
            latest, prev = data[0], data[1]
            if latest['open'] > (prev['close'] * Decimal('1.05')): final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_monthly_inside_candle(all_tokens):
    screener_id = 'monthly_inside_candle'
    final_results = []
    today = date.today()
    first_day = today.replace(day=1)
    last_day_prev = first_day - timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    for token in all_tokens:
        try:
            start_date = (first_day_prev - relativedelta(months=1)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 40: continue
            df = pd.DataFrame(data)
            df['date_dt'] = pd.to_datetime(df['date'])
            curr_df = df[df['date_dt'].dt.month == today.month]
            prev_df = df[df['date_dt'].dt.month == first_day_prev.month]
            if curr_df.empty or prev_df.empty: continue
            curr_h, curr_l = curr_df['high'].max(), curr_df['low'].min()
            prev_h, prev_l = prev_df['high'].max(), prev_df['low'].min()
            if curr_h < prev_h and curr_l > prev_l:
                latest, prev = df.sort_values(by='date', ascending=False).iloc[0], df.sort_values(by='date', ascending=False).iloc[1]
                final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_tight_weekly_base(all_tokens):
    screener_id = 'tight_weekly_base'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(weeks=10)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 25: continue
            df = pd.DataFrame(data)
            df.set_index(pd.to_datetime(df['date']), inplace=True)
            w_df = df.resample('W').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
            if len(w_df) < 4: continue
            last_3 = w_df.iloc[-3:].copy()
            last_3['range_pct'] = (last_3['high'] - last_3['low']) / last_3['low']
            if (last_3['range_pct'] < Decimal('0.1')).all():
                latest, prev = df.iloc[-1], df.iloc[-2]
                final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_screener_tight_daily_base(all_tokens):
    screener_id = 'tight_daily_base'
    final_results = []
    for token in all_tokens:
        try:
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token), ScanIndexForward=False, Limit=10)
            data = res.get('Items', [])
            if len(data) < 10: continue
            df = pd.DataFrame(data)
            if (df['high'].max() - df['low'].min()) / df['low'].min() < Decimal('0.08'):
                final_results.append(_format_result_eod(data[0], data[1]))
        except Exception: continue
    return screener_id, final_results

def run_screener_low_of_highest_up_candle(all_tokens):
    screener_id = 'low_of_highest_up_candle'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 22: continue
            df = pd.DataFrame(data)
            df['gain_pct'] = (df['close'] - df['open']) / df['open']
            level = df.loc[df['gain_pct'].idxmax()]['low']
            latest = df.sort_values(by='date', ascending=False).iloc[0]
            if latest['close'] <= (level * Decimal('1.02')):
                final_results.append(_format_result_eod(latest, df.sort_values(by='date', ascending=False).iloc[1]))
        except Exception: continue
    return screener_id, final_results

def run_screener_low_of_high_volume_candle(all_tokens):
    screener_id = 'low_of_high_volume_candle'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 22: continue
            df = pd.DataFrame(data)
            level = df.loc[df['volume'].idxmax()]['low']
            latest = df.sort_values(by='date', ascending=False).iloc[0]
            if latest['close'] <= (level * Decimal('1.02')):
                final_results.append(_format_result_eod(latest, df.sort_values(by='date', ascending=False).iloc[1]))
        except Exception: continue
    return screener_id, final_results

def run_screener_low_of_3_pct_down_candle(all_tokens):
    screener_id = 'low_of_3_pct_down_candle'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 22: continue
            df = pd.DataFrame(data).sort_values(by='date')
            df['change_pct'] = (df['close'] - df['open']) / df['open']
            down_candles = df[df['change_pct'] <= Decimal('-0.03')]
            if down_candles.empty: continue
            level = down_candles.iloc[-1]['low']
            latest = df.iloc[-1]
            if latest['close'] <= (level * Decimal('1.02')):
                final_results.append(_format_result_eod(latest, df.iloc[-2]))
        except Exception: continue
    return screener_id, final_results

def run_screener_above_10_sma(all_tokens):
    screener_id = 'above_10_sma'
    final_results = []
    for token in all_tokens:
        try:
            start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
            res = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token) & Key('date').gte(start_date))
            data = res.get('Items', [])
            if len(data) < 10: continue
            data.sort(key=lambda x: x['date'])
            df = pd.DataFrame(data)
            df['sma_10'] = df['close'].rolling(window=10).mean()
            latest, prev = df.iloc[-1], df.iloc[-2]
            if pd.notna(latest['sma_10']) and latest['close'] > latest['sma_10']:
                final_results.append(_format_result_eod(latest, prev))
        except Exception: continue
    return screener_id, final_results

def run_all_eod_screeners():
    print("\nStarting all end-of-day screener analyses...")
    try:
        with symbol_map_lock:
            all_tokens = list(symbol_to_token_map.values())
        if TEST_MODE: all_tokens = all_tokens[:TEST_STOCK_LIMIT]
        print(f"Found {len(all_tokens)} unique stocks to analyze.")
        eod_screener_functions = [run_screener_near_ath, run_screener_recent_ipos, run_screener_above_200_sma, run_screener_multi_year_breakout, run_screener_monthly_high_breakout, run_screener_five_pct_breakout, run_screener_monthly_inside_candle, run_screener_tight_weekly_base, run_screener_tight_daily_base, run_screener_low_of_highest_up_candle, run_screener_low_of_high_volume_candle, run_screener_low_of_3_pct_down_candle, run_screener_above_10_sma]
        with results_table.batch_writer() as batch:
            for func in eod_screener_functions:
                screener_id, results = func(all_tokens)
                if results is not None:
                    print(f"  -> {screener_id} found {len(results)} matching stocks.")
                    batch.put_item(Item={'screener_name': screener_id, 'results': results, 'last_updated': datetime.now().isoformat()})
        print("\n✅ All EOD screeners finished and results saved.")
        return True
    except Exception as e:
        print(f"❌ An error occurred during the main EOD screener run: {e}")
        return False

# ==============================================================================
# --- 6. INTRADAY SCREENER ANALYSIS LOGIC ---
# ==============================================================================
def run_intraday_screeners():
    print("\nStarting INTRADAY screener analysis...")
    if not intraday_task_lock.acquire(blocking=False): return
    try:
        if not ensure_api_session():
            print("Skipping intraday screeners due to login failure.")
            return
        with symbol_map_lock:
            all_stocks = [{'token': token, 'symbol': symbol} for symbol, token in symbol_to_token_map.items()]
        if TEST_MODE: all_stocks = all_stocks[:TEST_STOCK_LIMIT]
        print(f"Found {len(all_stocks)} unique stocks for intraday screeners.")
        open_low, open_high, orh_breakout = [], [], []
        today = datetime.now().strftime('%Y-%m-%d')
        from_t, to_t = f"{today} 09:15", f"{today} 09:20"
        for i, stock in enumerate(all_stocks):
            try:
                params = {"exchange": "NSE", "symboltoken": str(stock['token']), "interval": "FIVE_MINUTE", "fromdate": from_t, "todate": to_t}
                res = smartApi.getCandleData(params)
                if not (res and res.get('status') and res.get('data')):
                    time.sleep(0.4)
                    continue
                c = res['data'][0]
                o, h, l, cl = Decimal(str(c[1])), Decimal(str(c[2])), Decimal(str(c[3])), Decimal(str(c[4]))
                result = {"symbol": stock['symbol'], "price": cl, "volume": int(c[5]), "changePct": ((cl-o)/o)*100}
                if o == l: open_low.append(result)
                if o == h: open_high.append(result)
                prev_days = data_table.query(KeyConditionExpression=Key('instrument_token').eq(stock['token']), ScanIndexForward=False, Limit=3)
                if prev_days.get('Count', 0) == 3:
                    if cl > max([item['high'] for item in prev_days['Items']]):
                        orh_breakout.append(result)
                time.sleep(0.4)
            except Exception as e: continue
        with results_table.batch_writer() as batch:
            batch.put_item(Item={'screener_name': 'intraday_open_low', 'results': open_low, 'last_updated': datetime.now().isoformat()})
            batch.put_item(Item={'screener_name': 'intraday_open_high', 'results': open_high, 'last_updated': datetime.now().isoformat()})
            batch.put_item(Item={'screener_name': 'intraday_orh_breakout', 'results': orh_breakout, 'last_updated': datetime.now().isoformat()})
        print("✅ Intraday screeners finished.")
    finally:
        intraday_task_lock.release()

# ==============================================================================
# --- 7. API SERVER LOGIC ---
# ==============================================================================
@app.route('/api/screeners/<screener_id>', methods=['GET'])
def get_screener_results(screener_id):
    if not screener_id: return jsonify({"error": "Screener ID is required"}), 400
    try:
        res = results_table.get_item(Key={'screener_name': screener_id})
        item = res.get('Item')
        if item: return app.response_class(response=json.dumps(item, cls=DecimalEncoder), status=200, mimetype='application/json')
        else: return jsonify({'screener_name': screener_id, 'results': [], 'last_updated': datetime.now().isoformat()}), 200
    except Exception as e: return jsonify({"error": "An internal server error occurred"}), 500

@app.route('/api/history/<symbol>', methods=['GET'])
def get_stock_history(symbol):
    if not ensure_api_session():
        return jsonify({"error": "Could not establish API session with broker"}), 503

    print(f"\n--- History API Request for '{symbol}' ---")
    token = None
    with symbol_map_lock:
        token = symbol_to_token_map.get(symbol)
        if not token:
            symbol_with_eq = f"{symbol}-EQ"
            print(f"DEBUG: Exact symbol not found. Trying with suffix: '{symbol_with_eq}'")
            token = symbol_to_token_map.get(symbol_with_eq)
    
    if not token:
        print(f"DEBUG: Symbol '{symbol}' (with and without -EQ) NOT FOUND.")
        return jsonify({"error": f"Symbol '{symbol}' not found in map"}), 404

    print(f"DEBUG: Found token '{token}'. Fetching data...")
    try:
        to_date, from_date = datetime.now(), datetime.now() - relativedelta(years=1)
        params = {"exchange": "NSE", "symboltoken": str(token), "interval": "ONE_DAY", "fromdate": from_date.strftime('%Y-%m-%d 09:15'), "todate": to_date.strftime('%Y-%m-%d 15:30')}
        res = smartApi.getCandleData(params)

        if not (res and res.get('status') and res.get('data')):
            print(f"Could not fetch historical data for {symbol} from SmartAPI. Response: {res}")
            return jsonify({"error": "Could not fetch historical data from provider"}), 500

        data = [{'time': c[0].split('T')[0], 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]} for c in res['data']]
        return jsonify(data)

    except Exception as e:
        print(f"API Error for history of {symbol}: {e}")
        return jsonify({"error": "An internal server error occurred"}), 500

# ==============================================================================
# --- 8. SCHEDULING & EXECUTION ---
# ==============================================================================
def run_eod_tasks():
    if not eod_task_lock.acquire(blocking=False): return
    try:
        print("\n" + "="*50 + f"\nStarting scheduled EOD tasks at {datetime.now()}\n" + "="*50)
        if run_daily_data_update(): run_all_eod_screeners()
        else: print("Skipping EOD screener analysis due to data update failure.")
        print("\nAll scheduled EOD tasks finished.")
    finally:
        eod_task_lock.release()

def get_ist_time(): return datetime.now(ZoneInfo("Asia/Kolkata"))

def run_task_scheduler():
    print("✅ Internal Task Scheduler started.")
    last_intraday, last_eod = None, None
    while True:
        try:
            now = get_ist_time()
            today = now.date()
            if now.weekday() < 5 and now.hour == 9 and now.minute == 21 and today != last_intraday:
                print(f"\n[SCHEDULER] Triggering INTRADAY tasks for {today.strftime('%Y-%m-%d')}...")
                threading.Thread(target=run_intraday_screeners).start()
                last_intraday = today
            if now.weekday() < 5 and now.hour == 16 and now.minute == 0 and today != last_eod:
                print(f"\n[SCHEDULER] Triggering EOD tasks for {today.strftime('%Y-%m-%d')}...")
                threading.Thread(target=run_eod_tasks).start()
                last_eod = today
            time.sleep(30)
        except Exception as e:
            print(f"❌ Scheduler Error: {e}")
            time.sleep(60)

def populate_symbol_map_on_startup():
    print("--- Running Initial Symbol Map Population ---")
    global symbol_to_token_map
    try:
        res = instrument_table.scan()
        items = res.get('Items', [])
        while 'LastEvaluatedKey' in res:
            res = instrument_table.scan(ExclusiveStartKey=res['LastEvaluatedKey'])
            items.extend(res.get('Items', []))
        
        temp_map = {item['symbol']: int(item['instrument_token']) for item in items}
        with symbol_map_lock: symbol_to_token_map = temp_map
        print(f"✅✅✅ SYMBOL MAP POPULATED with {len(symbol_to_token_map)} unique symbols.")
    except Exception as e:
        print(f"❌❌❌ CRITICAL: Could not populate symbol map on startup. Error: {e}")

# ==============================================================================
# --- 9. APPLICATION STARTUP LOGIC ---
# ==============================================================================
print("--- Starting Application Setup ---")
populate_symbol_map_on_startup()
print("Starting background task scheduler...")
threading.Thread(target=run_task_scheduler, daemon=True).start()
print("Triggering initial EOD data update in the background...")
threading.Thread(target=run_eod_tasks).start()
print("--- Application Setup Complete. Server is ready. ---")

if __name__ == '__main__':
    print("Starting Flask development server...")
    app.run(host='0.0.0.0', port=5000)

