# This single script combines the daily data update, screener analysis, and API server.
# FINAL FIX: The EOD screener functions have been modified to correctly include the instrument_token.

import boto3
import pandas as pd
import pyotp
import sys
import time
import json
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
    data_table.load()
    results_table.load()
    print("✅ Successfully connected to both DynamoDB tables.")
except Exception as e:
    print(f"❌ Could not connect to DynamoDB. Error: {e}")
    sys.exit()

smartApi = SmartConnect(API_KEY)

eod_task_lock = threading.Lock()
intraday_task_lock = threading.Lock()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert pandas Decimals/numerics to float
            return float(obj)
        # Handle numpy int64 if pandas uses it
        if hasattr(obj, 'item'):
            return obj.item()
        return super(DecimalEncoder, self).default(obj)

# ==============================================================================
# --- 3. END-OF-DAY DATA UPDATE LOGIC ---
# ==============================================================================
def run_daily_data_update():
    # This function does not need changes.
    print("\nStarting daily data update process...")
    try:
        print("Connecting to Angel One SmartAPI...")
        totp = pyotp.TOTP(TOTP_SECRET).now()
        session_data = smartApi.generateSession(CLIENT_CODE, MPIN, totp)
        if not session_data['status']:
            print(f"❌ Angel One Login Failed: {session_data['message']}")
            return False
        else:
            print("✅ Angel One session created successfully.")
    except Exception as e:
        print(f"❌ Error during Angel One login: {e}")
        return False

    stocks_to_update = []
    print("Scanning data table for existing stocks...")
    try:
        if TEST_MODE:
            # ... (no changes to data gathering logic) ...
            print(f"⚠️ TEST MODE IS ON. Scanning until {TEST_STOCK_LIMIT} unique stocks are found.")
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
            # ... (no changes to data gathering logic) ...
            print("Scanning for all unique stock tokens...")
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
            print("⚠️ No stocks found in DB to update.")
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
            hist_params = { "exchange": "NSE", "symboltoken": str(test_stock['token']), "interval": "ONE_DAY", "fromdate": f"{from_date_check.strftime('%Y-%m-%d')} 09:15", "todate": f"{to_date_check.strftime('%Y-%m-%d')} 15:30"}
            api_response = smartApi.getCandleData(hist_params)
            if api_response and api_response.get('status') and api_response.get('data'):
                last_candle_str = api_response['data'][-1][0]
                latest_trading_day = datetime.strptime(last_candle_str.split('T')[0], '%Y-%m-%d')
                print(f"✅ Success! Latest trading day is {latest_trading_day.strftime('%Y-%m-%d')} (found using {test_stock['symbol']})")
                break
            time.sleep(0.4)
        except Exception as e:
            print(f"  -> DEBUG: An exception occurred during API call for {test_stock['symbol']}: {e}")
            continue
    
    if not latest_trading_day:
        print("❌ Could not determine latest trading day after trying all test stocks.")
        return False

    from_date = latest_trading_day.strftime('%Y-%m-%d 09:15')
    to_date = latest_trading_day.strftime('%Y-%m-%d 15:30')
    date_checked = latest_trading_day.strftime('%Y-%m-%d')
    print(f"\nFetching all stock data for the confirmed latest trading day: {date_checked}...")
    
    with data_table.batch_writer() as batch:
        for index, stock in enumerate(stocks_to_update):
            print(f"  -> [{index + 1}/{len(stocks_to_update)}] Fetching {stock['symbol']}...")
            try:
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
                print(f"     -> ERROR for {stock['symbol']}: {e}")
    
    print("✅ Daily data update complete!")
    return True


# ==============================================================================
# --- 4. END-OF-DAY SCREENER ANALYSIS LOGIC (FINAL FIX) ---
# ==============================================================================
def run_screener_near_ath(all_tokens):
    screener_id = 'near_ath'
    print(f"--- Running Screener: {screener_id} ---")
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
            if latest['close'] >= (all_time_high * Decimal('0.75')):
                final_results.append({
                    "symbol": latest['symbol'],
                    "token": int(latest['instrument_token']),
                    "changePct": ((latest['close'] - previous['close']) / previous['close']) * 100,
                    "price": latest['close'],
                    "volume": int(latest['volume'])
                })
        except Exception: continue
    return screener_id, final_results

def run_screener_recent_ipos(all_tokens): return 'recent_ipos', []

def run_screener_above_200_sma(all_tokens):
    screener_id = 'above_200_sma'
    print(f"--- Running Screener: {screener_id} ---")
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
            if pd.notna(latest['sma_200']) and latest['close'] > latest['sma_200']:
                final_results.append({
                    "symbol": latest['symbol'],
                    "token": int(latest['instrument_token']),
                    "changePct": ((latest['close'] - previous['close']) / previous['close']) * 100,
                    "price": latest['close'],
                    "volume": int(latest['volume'])
                })
        except Exception: continue
    return screener_id, final_results

def run_screener_tight_daily_base(all_tokens):
    screener_id = 'tight_daily_base'
    print(f"--- Running Screener: {screener_id} ---")
    final_results = []
    for token in all_tokens:
        try:
            query_response = data_table.query(KeyConditionExpression=Key('instrument_token').eq(token), ScanIndexForward=False, Limit=10)
            stock_data = query_response.get('Items', [])
            if len(stock_data) < 10: continue
            
            # Using dicts directly is safer than pandas here
            latest = stock_data[0]
            previous = stock_data[1]
            
            # Create a temporary DataFrame for calculation only
            df = pd.DataFrame(stock_data)
            if (df['high'].max() - df['low'].min()) / df['low'].min() < Decimal('0.08'):
                final_results.append({
                    "symbol": latest['symbol'],
                    "token": int(latest['instrument_token']),
                    "changePct": ((latest['close'] - previous['close']) / previous['close']) * 100,
                    "price": latest['close'],
                    "volume": int(latest['volume'])
                })
        except Exception: continue
    return screener_id, final_results

# ... (All other EOD screener functions should be updated with the same pattern as above) ...

def run_all_eod_screeners():
    print("\nStarting all end-of-day screener analyses...")
    try:
        all_tokens = set()
        if TEST_MODE:
            # ... (no change to token gathering logic) ...
            print(f"⚠️ TEST MODE IS ON. Scanning until {TEST_STOCK_LIMIT} unique stocks are found.")
            scan_kwargs = {}
            while len(all_tokens) < TEST_STOCK_LIMIT:
                response = data_table.scan(ProjectionExpression='instrument_token', **scan_kwargs)
                items = response.get('Items', [])
                if not items: break
                for item in items:
                    if 'instrument_token' in item:
                        all_tokens.add(int(item['instrument_token']))
                        if len(all_tokens) >= TEST_STOCK_LIMIT: break
                if 'LastEvaluatedKey' in response and len(all_tokens) < TEST_STOCK_LIMIT:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                else: break
        else: # Full Scan
             # ... (no change to token gathering logic) ...
            print("Scanning for all unique stock tokens...")
            scan_kwargs = {'ProjectionExpression': 'instrument_token'}
            response = data_table.scan(**scan_kwargs)
            all_items = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = data_table.scan(**scan_kwargs)
                all_items.extend(response.get('Items', []))
            all_tokens = {int(item['instrument_token']) for item in all_items}

        print(f"Found {len(all_tokens)} unique stocks to analyze.")
        eod_screener_functions = [
            run_screener_near_ath, run_screener_recent_ipos, run_screener_above_200_sma, run_screener_tight_daily_base
            # Add all other screener functions here
        ]
        with results_table.batch_writer() as batch:
            for func in eod_screener_functions:
                screener_id, results = func(all_tokens)
                if results is not None:
                    print(f"  -> {screener_id} found {len(results)} matching stocks.")
                    batch.put_item(Item={ 'screener_name': screener_id, 'results': results, 'last_updated': datetime.now().isoformat() })
        print("\n✅ All EOD screeners finished and results saved.")
        return True
    except Exception as e:
        print(f"❌ An error occurred during the main EOD screener run: {e}")
        return False

# ==============================================================================
# --- 5. INTRADAY SCREENER ANALYSIS LOGIC ---
# ==============================================================================
def _format_result_intraday(stock, candle_5min):
    # This function is already correct
    return {
        "symbol": stock['symbol'],
        "token": int(stock['token']),
        "price": Decimal(str(candle_5min[4])),
        "volume": int(candle_5min[5]),
        "changePct": ((Decimal(str(candle_5min[4])) - Decimal(str(candle_5min[1]))) / Decimal(str(candle_5min[1]))) * 100
    }

def run_intraday_screeners():
    # ... (No changes needed in this function's logic) ...
    print("\nStarting INTRADAY screener analysis...")
    if not intraday_task_lock.acquire(blocking=False):
        print("An intraday task is already running. Skipping.")
        return
    try:
        # ... (no changes to intraday logic) ...
        print("Connecting to Angel One SmartAPI for intraday data...")
        try:
            totp = pyotp.TOTP(TOTP_SECRET).now()
            session_data = smartApi.generateSession(CLIENT_CODE, MPIN, totp)
            if not session_data['status']:
                print(f"❌ Angel One Login Failed: {session_data['message']}")
                return
            print("✅ Angel One session created successfully.")
        except Exception as e:
            print(f"❌ Error during Angel One login: {e}")
            return

        all_stocks = []
        if TEST_MODE:
            # ... (no change to token gathering logic)
            print(f"⚠️ TEST MODE IS ON. Scanning until {TEST_STOCK_LIMIT} unique stocks are found.")
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
        else:
            # ... (no change to token gathering logic)
            print("Scanning for all unique stock tokens...")
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

        print(f"Found {len(all_stocks)} unique stocks to analyze for intraday screeners.")
        open_low_results, open_high_results, orh_breakout_results = [], [], []
        today_str = datetime.now().strftime('%Y-%m-%d')
        from_date, to_date = f"{today_str} 09:15", f"{today_str} 09:20"

        for index, stock in enumerate(all_stocks):
            print(f"  -> [{index + 1}/{len(all_stocks)}] Analyzing {stock['symbol']}...")
            try:
                hist_params = {"exchange": "NSE", "symboltoken": str(stock['token']), "interval": "FIVE_MINUTE", "fromdate": from_date, "todate": to_date}
                api_response = smartApi.getCandleData(hist_params)
                if not (api_response and api_response.get('status') and api_response.get('data')):
                    time.sleep(0.4)
                    continue
                first_candle = api_response['data'][0]
                candle_open, candle_high, candle_low, candle_close = Decimal(str(first_candle[1])), Decimal(str(first_candle[2])), Decimal(str(first_candle[3])), Decimal(str(first_candle[4]))
                
                formatted_result = _format_result_intraday(stock, first_candle)
                if formatted_result:
                    if candle_open == candle_low: open_low_results.append(formatted_result)
                    if candle_open == candle_high: open_high_results.append(formatted_result)

                prev_days_data = data_table.query(KeyConditionExpression=Key('instrument_token').eq(stock['token']), ScanIndexForward=False, Limit=3)
                if prev_days_data.get('Count', 0) == 3:
                    resistance_level = max([item['high'] for item in prev_days_data['Items']])
                    if candle_close > resistance_level and formatted_result:
                        orh_breakout_results.append(formatted_result)
                time.sleep(0.4)
            except Exception as e:
                print(f"     -> ERROR for {stock['symbol']}: {e}")
                continue
        
        print("Saving intraday screener results...")
        with results_table.batch_writer() as batch:
            batch.put_item(Item={'screener_name': 'intraday_open_low', 'results': open_low_results, 'last_updated': datetime.now().isoformat()})
            batch.put_item(Item={'screener_name': 'intraday_open_high', 'results': open_high_results, 'last_updated': datetime.now().isoformat()})
            batch.put_item(Item={'screener_name': 'intraday_orh_breakout', 'results': orh_breakout_results, 'last_updated': datetime.now().isoformat()})
        print("✅ Intraday screeners finished and results saved.")
    finally:
        intraday_task_lock.release()

# ==============================================================================
# --- 6. API SERVER LOGIC ---
# ==============================================================================
@app.route('/api/screeners/<screener_id>', methods=['GET'])
def get_screener_results(screener_id):
    # ... (No changes needed) ...
    if not screener_id: return jsonify({"error": "Screener ID is required"}), 400
    print(f"API request received for screener: {screener_id}")
    try:
        response = results_table.get_item(Key={'screener_name': screener_id})
        item = response.get('Item')
        if item:
            return app.response_class(response=json.dumps(item, cls=DecimalEncoder), status=200, mimetype='application/json')
        else:
            return jsonify({'screener_name': screener_id, 'results': [], 'last_updated': datetime.now().isoformat()}), 200
    except Exception as e:
        print(f"API Error for {screener_id}: {e}")
        return jsonify({"error": "An internal server error occurred"}), 500

@app.route('/api/history/<int:instrument_token>', methods=['GET'])
def get_stock_history(instrument_token):
    # ... (No changes needed) ...
    print(f"API request received for history: {instrument_token}")
    try:
        response = data_table.query(
            KeyConditionExpression=Key('instrument_token').eq(instrument_token),
            ScanIndexForward=False, Limit=200 )
        items = response.get('Items', [])
        items.sort(key=lambda x: x['date'])
        chart_data = [{"time": item['date'], "open": float(item['open']), "high": float(item['high']), "low": float(item['low']), "close": float(item['close']),} for item in items]
        return jsonify(chart_data)
    except Exception as e:
        print(f"History API Error for token {instrument_token}: {e}")
        return jsonify({"error": "An internal server error occurred"}), 500

# ==============================================================================
# --- 7. SCHEDULING & EXECUTION ---
# ==============================================================================
# ... (No changes needed in the scheduler) ...
def run_eod_tasks():
    if not eod_task_lock.acquire(blocking=False): print("An EOD task is already running. Skipping this trigger."); return
    try:
        print("="*50); print(f"Starting scheduled EOD tasks at {datetime.now()}"); print("="*50)
        update_success = run_daily_data_update()
        if update_success: run_all_eod_screeners()
        else: print("Skipping EOD screener analysis due to data update failure.")
        print("\nAll scheduled EOD tasks finished.")
    finally: eod_task_lock.release()

def get_ist_time():
    return datetime.now(ZoneInfo("Asia/Kolkata"))

def run_task_scheduler():
    print("✅ Internal Task Scheduler started. Waiting for scheduled times...")
    last_intraday_run_date = None
    last_eod_run_date = None
    while True:
        try:
            now_ist = get_ist_time()
            today_ist = now_ist.date()
            if now_ist.weekday() < 5 and now_ist.hour == 9 and now_ist.minute == 21:
                if today_ist != last_intraday_run_date:
                    print(f"\n[SCHEDULER] Triggering INTRADAY tasks for {today_ist.strftime('%Y-%m-%d')}...")
                    task_thread = threading.Thread(target=run_intraday_screeners); task_thread.start()
                    last_intraday_run_date = today_ist
            if now_ist.weekday() < 5 and now_ist.hour == 16 and now_ist.minute == 0:
                if today_ist != last_eod_run_date:
                    print(f"\n[SCHEDULER] Triggering END-OF-DAY tasks for {today_ist.strftime('%Y-%m-%d')}...")
                    task_thread = threading.Thread(target=run_eod_tasks); task_thread.start()
                    last_eod_run_date = today_ist
            time.sleep(30)
        except Exception as e:
            print(f"❌ An error occurred in the task scheduler: {e}")
            time.sleep(60)

if __name__ == '__main__':
    print("Starting the main application...")
    scheduler_thread = threading.Thread(target=run_task_scheduler, daemon=True)
    scheduler_thread.start()
    print("Triggering initial EOD data update and screener analysis in the background...")
    initial_eod_thread = threading.Thread(target=run_eod_tasks)
    initial_eod_thread.start()
    print("Starting Flask API server to serve screener results...")
    app.run(host='0.0.0.0', port=5000)

