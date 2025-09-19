import os
import sys
import json
import re
import time
from datetime import datetime, time as dt_time, timedelta, date
from dateutil.relativedelta import relativedelta, TH
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import requests
import pandas as pd
import socket

# --- CHANGE: Upgraded Google Authentication Library ---
# The old 'oauth2client' is deprecated. This is the modern, official library.
from google.oauth2.service_account import Credentials
import gspread

# --- NEW: Angel One SmartAPI Imports ---
from SmartApi import SmartConnect
import pyotp

# --- NEW: Yahoo Finance Import ---
import yfinance as yf

# --- START: IMPORTS FOR 24/7 SCHEDULER ---
from flask import Flask
import threading
import schedule
from zoneinfo import ZoneInfo
# --- END: IMPORTS FOR 24/7 SCHEDULER ---

print("="*60)
print("--- [RENDER-DEBUG] Script execution started (Top Level) ---")
print(f"--- Timestamp: {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')} ---")
print("="*60)


# ==============================================================================
# --- GLOBAL CONFIGURATION AND CREDENTIALS ---
# ==============================================================================

# --- Google Sheets Configuration ---
GOOGLE_SHEET_ID = "1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0"
ATH_CACHE_SHEET_NAME = "ATH Cache"

# --- Angel One SmartAPI Credentials ---
# IMPORTANT: These are placeholders from your example. Ensure they are correct.
API_KEY = "oNNHQHKU"
CLIENT_CODE = "D355432"
MPIN = "1234"
TOTP_SECRET = "QHO5IWOISV56Z2BFTPFSRSQVRQ"

# --- Angel One Configuration ---
INSTRUMENT_LIST_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
API_REQUEST_DELAY = 0.4 # Delay between successful API calls to respect rate limits

# --- Global Objects (Initialized at runtime) ---
smart_api_obj = None
instrument_master_list = []
instrument_token_map = {} # Cache for symbol -> token mapping

# --- List of symbols to exclude from all scans ---
SYMBOLS_TO_EXCLUDE = {
    # This extensive list is preserved from your original script.
    "ABSLNN50ET", "AONENIFTY", "AXISCETF", "AXISNIFTY", "BBNPNBETF", "BSE500IETF", "BSLNIFTY", "EQUAL200",
    "GROWWNIFTY", "GROWWN200", "HDFCBSE500", "HDFCGROWTH", "HDFCMID150", "HDFCNIF100", "HDFCNIFTY",
    "HDFCNEXT50", "HDFCSML250", "ICICIB22", "IDFNIFTYET", "IVZINNIFTY", "JUNIORBEES", "LICNETFN50",
    "LICNMID100", "MASPTOP50", "MID150", "MID150BEES", "MID150CASE", "MIDCAP", "MIDCAPETF", "MIDSMALL",
    "MON100", "MONEXT50", "MONIFTY500", "MOSMALL250", "NEXT50", "NEXT50IETF", "NIF100BEES", "NIF100IETF",
    "NIFTY1", "NIFTY100EW", "NIFTY50ADD", "NIFTYBEES", "NIFTYBETF", "NIFTYETF", "NIFTYIETF", "QNIFTY",
    "SETFNIF50", "SETFNN50", "SMALLCAP", "SNXT30BEES", "TOP100CASE", "TOP15IETF", "UTINIFTETF", "UTINEXT50",
    "UTISXN50", "ABSLPSE", "AUTOBEES", "AUTOIETF", "AXISBNKETF", "AXISHCETF", "AXISTECETF", "SBINEQWETF",
    "BANKBEES", "BANKBETF", "NIFMID150", "BANKETF", "BANKETFADD", "BANKIETF", "BFSI", "CONSUMBEES",
    "CONSUMER", "CONSUMIETF", "CPSEETF", "ESG", "FINIETF", "FMCGIETF", "GROWWDEFNC", "HDFCPSUBK",
    "HDFCPVTBAN", "HEALTHADD", "HEALTHIETF", "BANKPSU", "INFRAIETF", "INFRABEES", "IT", "ITBEES", "ITETF",
    "ITETFADD", "ITIETF", "MAKEINDIA", "METAL", "METALIETF", "MIDSELIETF", "MNC", "MOINFRA", "MOPSE",
    "NIFITETF", "OILIETF", "PHARMABEES", "PSUBANK", "PSUBANKADD", "PSUBANKIETF", "PSUBNKBEES", "PSUBNKIETF",
    "PVTBANIETF", "PVTBANKADD", "SBIETFIT", "SENSEXADD", "SETFNIFBK", "SHARIABEES", "TECH", "UTIBANKETF",
    "AXISGOLD", "AXISILVER", "BBNPPGOLD", "BSLGOLDETF", "EGOLD", "ESILVER", "GOLD1", "GOLDBEES", "GOLDCASE",
    "GOLDETF", "GOLDETFADD", "GOLDIETF", "GOLDSHARE", "GOLDTECH", "GROWWGOLD", "GROWWSLVR", "HDFCGOLD",
    "HDFCSILVER", "IVZINGOLD", "LICMFGOLD", "MOGOLD", "QGOLDHALF", "SBISILVER", "SETFGOLD", "SILVER",
    "SILVER1", "SILVERADD", "SILVERBEES", "SILVERCASE", "SILVERETF", "SILVERIETF", "SILVRETF", "TATSILV",
    "UNIONGOLD", "AXISBPSETF", "EBBETF0430", "EBBETF0431", "EBBETF0432", "EBBETF0433", "GILT5YBEES",
    "GSEC10ABSL", "GSEC10IETF", "GSEC10YEAR", "GSEC5IETF", "LICNETFGSC", "LTGILTBEES", "NIF10GETF",
    "PNBGILTS", "SDL26BEES", "SETF10GILT", "ALPHA", "ALPHAETF", "ALPL30IETF", "DIVOPPBEES", "EQUAL50",
    "EQUAL50ADD", "HDFCLOWVOL", "HDFCQUAL", "HDFCVALUE", "LOWVOL", "LOWVOL1", "LOWVOLIETF", "MOM100",
    "MOM30IETF", "MOM50", "MOMENTUM", "MOMENTUM50", "MOMIDMTM", "MON50EQUAL", "MOALPHA50", "MOLOWVOL",
    "MOQUALITY", "MOVALUE", "MULTICAP", "NIFTYQLITY", "NV20", "NV20BEES", "NV20IETF", "QUAL30IETF",
    "QUALITY30", "SBIETFQLTY", "SBIETFEQW", "VAL30IETF", "ABSLLIQUID", "AONELIQUID", "CASHIETF",
    "GROWWLIQID", "HDFCLIQUID", "LIQGRWBEES", "LIQUID", "LIQUID1", "LIQUIDADD", "LIQUIDBEES", "LIQUIDBETF",
    "LIQUIDCASE", "LIQUIDETF", "LIQUIDIETF", "LIQUIDPLUS", "LIQUIDSBI", "LIQUIDSHRI", "HDFCSENSEX",
    "SENSEXETF", "UTISENSETF", "BSLSENETFG", "SENSEXIETF", "AXSENSEX", "NIFTY GROWSECT 15", "NIFTY50 PR 2X LEV",
    "NIFTY 500", "NIFTY IT", "NIFTY BANK", "NIFTY MIDCAP 100", "NIFTY 100", "NIFTY NEXT 50", "NIFTY MIDCAP 50",
    "HANGSENG BEES-NAV", "INDIA VIX", "NIFTY REALTY", "NIFTY INFRA", "NIFTY ENERGY", "NIFTY FMCG", "NIFTY MNC",
    "NIFTY PHARMA", "NIFTY PSE", "NIFTY PSU BANK", "NIFTY SERV SECTOR", "NIFTY AUTO", "NIFTY METAL",
    "NIFTY MEDIA", "NIFTY SMLCAP 100", "NIFTY 200", "NIFTY DIV OPPS 50", "NIFTY COMMODITIES", "NIFTY CONSUMPTION",
    "NIFTY FIN SERVICE", "NIFTY50 DIV POINT", "NIFTY100 LIQ 15", "NIFTY CPSE", "NIFTY50 PR 1X INV",
    "NIFTY50 TR 2X LEV", "NIFTY50 TR 1X INV", "NIFTY50 VALUE 20", "NIFTY MID LIQ 15", "NIFTY PVT BANK",
    "NIFTY100 QUALTY30", "NIFTY GS 8 13YR", "NIFTY GS 10YR", "NIFTY GS 10YR CLN", "NIFTY GS 4 8YR",
    "NIFTY GS 11 15YR", "NIFTY GS 15YRPLUS", "NIFTY GS COMPSITE", "NIFTY50 EQL WGT", "NIFTY100 EQL WGT",
    "NIFTY100 LOWVOL30", "NIFTY ALPHA 50", "NIFTY MIDCAP 150", "NIFTY SMLCAP 50", "NIFTY SMLCAP 250",
    "NIFTY MIDSML 400", "NIFTY200 QUALTY30", "NIFTY MID SELECT", "SENSEX", "BSEPSU", "BSE100", "BSE200",
    "BSE500", "BSE IT", "BSEFMC", "BSE CG", "BSE CD", "BSE HC", "TECK", "BANKEX", "AUTO", "CPSE", "SMLCAP",
    "DOL30", "DOL100", "DOL200", "LRGCAP", "MIDSEL", "SMLSEL", "OILGAS", "POWER", "REALTY", "BSEIPO", "CARBON",
    "SMEIPO", "INFRA", "GREENX", "SNSX50", "SNXT50", "ENERGY", "FINSER", "INDSTR", "TELCOM", "LMI250",
    "MSL400", "MCXCRUDEX", "MCXCOPRDEX", "MCXSILVDEX", "MCXGOLDEX", "MCXMETLDEX", "MCXBULLDEX", "MCXCOMPDEX",
    "MFGLOBALCI", "MCXSAGRI", "MCXSENERGY", "MCXSMETAL", "MCXSCOMDEX", "MCXCOMPOSITE", "MCXCOMDEX", "MCXAGRI",
    "MCXENERGY", "MCXMETAL", "AGRIDEX", "NKRISHI", "FREIGHTEX", "NCDEXRAIN", "NCDEXAGRI", "FUTEXAGRI",
    "081NSETEST", "151NSETEST", "111NSETEST", "061NSETEST", "181NSETEST", "051NSETEST", "121NSETEST",
    "11NSETEST", "041NSETEST", "G1NSETEST", "131NSETEST", "171NSETEST", "021NSETEST", "031NSETEST",
    "011NSETEST", "071NSETEST", "101NSETEST", "091NSETEST", "141NSETEST", "V1NSETEST", "161NSETEST",
    "N1NSETEST", "HDFCNIFIT", "LICNETFSEN", "EBANKNIFTY", "MOMGF", "MOMSEC", "NIF5GETF", "NEXT30ADD", "NETF", "NPBET",
    "MIDCAPIETF", "SELECTIPO", "ABGSEC", "MIDQ50ADD", "AONETOTAL", "MOHEALTH", "HEALTHY", "SBIETFCON",
    "COMMOIETF", "GROWWMOM50", "LICNFNHGP", "TNIDETF", "MOGSEC", "HNGSNGBEES", "TOP10ADD", "MONQ50"
}

# Determine the path for the service account JSON key file
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
    print("[RENDER-DEBUG] Using production credential path: /etc/secrets/creds.json")
else:
    # Fallback for local development. Assumes the key file is in the same directory.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    JSON_KEY_FILE_PATH = os.path.join(current_dir, "the-money-method-ad6d7-6d23c192b74e.json")
    print(f"[RENDER-DEBUG] Using local development credential path: {JSON_KEY_FILE_PATH}")


# Define the necessary OAuth2 scopes for Google Sheets and Drive access
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --- START: FLASK WEB SERVICE SETUP ---
app = Flask(__name__)
print("[RENDER-DEBUG] Flask app object created.")

@app.route('/ping')
def ping():
    """A health-check endpoint for UptimeRobot."""
    return "The scanner service is alive.", 200
# --- END: FLASK WEB SERVICE SETUP ---


# ==============================================================================
# --- ANGEL ONE API AND UTILITY FUNCTIONS ---
# ==============================================================================

def initialize_services():
    """Connects to Google Sheets and Angel One SmartAPI."""
    global gsheet, cache_sheet, stock_sheet, smart_api_obj
    print("[DEBUG] Initializing services...")

    # --- SET GLOBAL TIMEOUT FOR ALL NETWORK OPERATIONS ---
    # This is critical for Render's environment to prevent hangs.
    print("[RENDER-DEBUG] Setting global socket timeout to 30 seconds...")
    socket.setdefaulttimeout(30)
    print("[RENDER-DEBUG] Global socket timeout set.")

    # --- Connect to Google Sheets ---
    try:
        print("[RENDER-DEBUG] 1. Preparing to create Google credentials object using the MODERN library...")
        creds = Credentials.from_service_account_file(JSON_KEY_FILE_PATH, scopes=SCOPE)
        
        print("[RENDER-DEBUG] 2. Credentials object created. Preparing to authorize...")
        client = gspread.authorize(creds)
        
        print("[RENDER-DEBUG] 3. Authorization successful. Preparing to open sheet...")
        
        # --- MODIFICATION: Added granular logging to pinpoint failure ---
        print(f"[RENDER-DEBUG] 3a. Attempting to open sheet with ID: {GOOGLE_SHEET_ID}...", flush=True)
        gsheet = client.open_by_key(GOOGLE_SHEET_ID)
        print("[RENDER-DEBUG] 3b. Successfully opened sheet object by key.", flush=True)
        
        stock_sheet = gsheet.worksheet(ATH_CACHE_SHEET_NAME)
        cache_sheet = gsheet.worksheet(ATH_CACHE_SHEET_NAME)
        print("[RENDER-DEBUG] 3c. Successfully accessed worksheet.", flush=True)
        # --- END MODIFICATION ---

        print("✅ Successfully connected to Google Sheets.")
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}", flush=True)
        import traceback
        print("--- Full Traceback ---", flush=True)
        traceback.print_exc()
        print("----------------------", flush=True)
        sys.exit()

    # --- Authenticate with Angel One ---
    try:
        print("🔐 Authenticating with Angel One SmartAPI...")
        smart_api_obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api_obj.generateSession(CLIENT_CODE, MPIN, totp)
        if not (data and data.get('status') and data.get('data', {}).get('jwtToken')):
            raise Exception(f"Authentication failed. Response: {data.get('message', 'No error message')}")
        print("✅ Angel One session generated successfully!")
    except Exception as e:
        print(f"❌ Error during Angel One session generation: {e}")
        sys.exit()

    # --- Download Instrument Master List ---
    try:
        global instrument_master_list, instrument_token_map
        print("📦 Downloading Angel One instrument master list...")
        response = requests.get(INSTRUMENT_LIST_URL)
        response.raise_for_status()
        instrument_master_list = response.json()
        # Create a quick lookup map: (symbol, exchange_segment) -> token
        for item in instrument_master_list:
            key = (item.get('symbol', '').upper(), item.get('exch_seg', '').upper())
            instrument_token_map[key] = item.get('token')
        print(f"✅ Instrument list downloaded and processed. Found {len(instrument_master_list)} instruments.")
    except Exception as e:
        print(f"❌ FATAL: Could not download or process the master instrument list: {e}. Exiting.")
        sys.exit()
    print("[DEBUG] Services initialized successfully.")

def get_token_for_symbol(symbol, exchange):
    """
    Finds the Angel One instrument token for a given symbol and exchange.
    Uses the pre-loaded instrument_token_map for fast lookups.
    """
    # Normalize inputs
    symbol_clean = symbol.strip().upper().replace("-EQ", "").replace("-BE", "")
    exchange_clean = exchange.strip().upper()

    # Map our 'NSE'/'BSE' to Angel One's 'exch_seg'
    if exchange_clean == 'NSE':
        key_eq = (f"{symbol_clean}-EQ", "NSE")
        key_be = (f"{symbol_clean}-BE", "NSE")
        token = instrument_token_map.get(key_eq, instrument_token_map.get(key_be))
        if token:
            return token, "NSE"

    elif exchange_clean == 'BSE':
        key = (symbol_clean, "BSE")
        token = instrument_token_map.get(key)
        if token:
            return token, "BSE"

    # Fallback: Search for the base symbol in both exchanges if direct match fails
    if instrument_token_map.get((f"{symbol_clean}-EQ", "NSE")): return instrument_token_map[(f"{symbol_clean}-EQ", "NSE")], "NSE"
    if instrument_token_map.get((f"{symbol_clean}-BE", "NSE")): return instrument_token_map[(f"{symbol_clean}-BE", "NSE")], "NSE"
    if instrument_token_map.get((symbol_clean, "BSE")): return instrument_token_map[(symbol_clean, "BSE")], "BSE"

    return None, None


def fetch_history_angelone(token, exchange, interval, from_date, to_date):
    """
    A robust wrapper for Angel One's getCandleData API with retry logic and enhanced debugging.
    Returns a pandas DataFrame for compatibility with existing logic.
    """
    historic_param = {
        "exchange": exchange,
        "symboltoken": token,
        "interval": interval,
        "fromdate": from_date.strftime('%Y-%m-%d %H:%M'),
        "todate": to_date.strftime('%Y-%m-%d %H:%M')
    }
    
    max_retries = 1
    retry_delay = 1.0 

    for attempt in range(max_retries + 1): 
        try:
            time.sleep(API_REQUEST_DELAY)

            if attempt > 0:
                print(f"   [RETRY] Waiting {retry_delay}s before retrying token {token} (Attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(retry_delay)

            # print(f"[DEBUG] Requesting {interval} history for token {token}. Params: {historic_param}")
            response = smart_api_obj.getCandleData(historic_param)
            
            if response and response.get("status") and response.get("data"):
                data = response["data"]
                if not data:
                    return pd.DataFrame()

                df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                df['Timestamp'] = df['Timestamp'].dt.tz_localize(None)
                df.set_index('Timestamp', inplace=True)
                return df
            else:
                # print(f"   [WARN] Invalid or empty response for token {token}.")
                # print(f"   [DIAGNOSTIC] Raw server response: {response}")
                if attempt < max_retries:
                    continue 
                else:
                    break 

        except Exception as e:
            # print(f"   [ERROR] API call failed for token {token}: {e}")
            if attempt < max_retries:
                continue 
            else:
                break 

    # print(f"❌ All retries failed for token {token}. Skipping.")
    return pd.DataFrame()

def fetch_history_for_ath_simple(token, exchange, interval, from_date, to_date):
    """
    A basic, non-retrying wrapper for the getCandleData API, for use by the ATH logic.
    """
    try:
        historic_param = {
            "exchange": exchange,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date.strftime('%Y-%m-%d %H:%M'),
            "todate": to_date.strftime('%Y-%m-%d %H:%M')
        }
        # print(f"[DEBUG] (ATH) Requesting {interval} history for token {token}...")
        response = smart_api_obj.getCandleData(historic_param)
        
        if response and response.get("status") and response.get("data"):
            data = response["data"]
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df['Timestamp'] = df['Timestamp'].dt.tz_localize(None)
            df.set_index('Timestamp', inplace=True)
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ (ATH) API call failed for token {token}: {e}")
        return pd.DataFrame()


def fetch_full_history_for_ath(token, exchange):
    """
    Dynamically fetches the complete historical data for a stock by making chunked 
    API calls until no more data is returned.
    """
    all_data = []
    end_date = datetime.now()
    for i in range(6): 
        start_date = end_date - relativedelta(years=5)
        
        if i == 0:
            initial_check_df = fetch_history_for_ath_simple(token, exchange, "ONE_DAY", end_date - relativedelta(months=1), end_date)
            if initial_check_df.empty:
                # print(f"[DEBUG] No recent data for token {token}. Skipping ATH fetch.")
                return None

        df_chunk = fetch_history_for_ath_simple(token, exchange, "ONE_DAY", start_date, end_date)
        time.sleep(API_REQUEST_DELAY)

        if not df_chunk.empty:
            all_data.append(df_chunk)
        else:
            # print(f"[DEBUG]   Empty chunk for token {token}. Assuming end of history.")
            break 
        end_date = start_date - timedelta(days=1)

    if not all_data:
        # print(f"[DEBUG] No historical data found for token {token}.")
        return None

    full_df = pd.concat(all_data)
    ath = full_df['High'].max()
    # print(f"[DEBUG] Calculated ATH for token {token} is {ath} from {len(full_df)} candles.")
    return ath


def get_fno_symbols():
    """
    Downloads the official list of F&O securities from the Angel Broking JSON file
    and returns a set of symbols for quick lookup.
    """
    print("📦 Downloading F&O securities list from Angel Broking...")
    fno_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    try:
        response = requests.get(fno_url, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # =================================================================
        # --- CHANGE IMPLEMENTED AS PER YOUR REQUEST ---
        # Switched from parsing the 'symbol' field to directly using the clean 'name' field.
        # This correctly identifies all symbols, including those with numbers or hyphens.
        fno_symbols = {
            item.get('name')
            for item in data
            if item.get('exch_seg') == 'NFO' and item.get('instrumenttype') == 'FUTSTK' and item.get('name')
        }
        # =================================================================
        
        print(f"✅ F&O list processed successfully. Found {len(fno_symbols)} unique stock futures symbols.")
        
        if fno_symbols:
            sorted_symbols = sorted(list(fno_symbols))
            print("\n--- F&O Symbol List ---")
            for i in range(0, len(sorted_symbols), 10):
                chunk = sorted_symbols[i:i+10]
                print(", ".join(chunk))
            print("-----------------------\n")
            
        return fno_symbols
    except Exception as e:
        print(f"❌ Error processing the F&O list: {e}")
        return set()

# ==============================================================================
# --- NEW: HELPER FUNCTION FOR FUNDAMENTAL DATA LOGGING ---
# (NO CHANGES MADE IN THIS SECTION)
# ==============================================================================

def format_large_number(num):
    """Formats a large number into a human-readable string (e.g., 1.2B, 345.6M, 78.9K)."""
    if num is None or not pd.notna(num) or not math.isfinite(num):
        return "N/A"
    num = abs(num)
    if num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f}B"
    if num >= 1_000_000:
        return f"{num / 1_000_000:.2f}M"
    if num >= 1_000:
        return f"{num / 1_000:.2f}K"
    return f"{num:.2f}"

# ==============================================================================
# --- MODIFIED: YAHOO FINANCE FUNCTION FOR QOQ GROWTH ---
# (NO CHANGES MADE IN THIS SECTION)
# ==============================================================================

def get_quarterly_growth(symbol, exchange):
    """
    Fetches the latest two quarters of financial data, calculates the
    Quarter-over-Quarter (QoQ) growth, and prints a detailed log.
    """
    ticker_symbol = f"{symbol.upper()}.NS" if exchange.upper() == 'NSE' else f"{symbol.upper()}.BO"
    
    try:
        stock_ticker = yf.Ticker(ticker_symbol)
        quarterly_financials = stock_ticker.quarterly_financials
        
        # Check if we have at least two quarters of data
        if quarterly_financials.empty or len(quarterly_financials.columns) < 2:
            # print(f"   -> [INFO] Not enough quarterly data for {ticker_symbol} to calculate growth.")
            return None, None

        # Get the latest quarter (Q1) and the previous quarter (Q2)
        latest_q = quarterly_financials.columns[0]
        previous_q = quarterly_financials.columns[1]

        # --- Revenue Growth Calculation ---
        revenue_q1 = quarterly_financials.loc['Total Revenue', latest_q] if 'Total Revenue' in quarterly_financials.index else None
        revenue_q2 = quarterly_financials.loc['Total Revenue', previous_q] if 'Total Revenue' in quarterly_financials.index else None
        
        revenue_growth = None
        if revenue_q1 is not None and revenue_q2 is not None and pd.notna(revenue_q1) and pd.notna(revenue_q2):
            if revenue_q2 > 0:
                revenue_growth = ((revenue_q1 / revenue_q2) - 1) * 100
            elif revenue_q1 > 0:
                revenue_growth = float('inf') # Positive growth from zero or negative
        
        # --- Profit Growth Calculation ---
        profit_q1 = quarterly_financials.loc['Net Income', latest_q] if 'Net Income' in quarterly_financials.index else None
        profit_q2 = quarterly_financials.loc['Net Income', previous_q] if 'Net Income' in quarterly_financials.index else None

        profit_growth = None
        if profit_q1 is not None and profit_q2 is not None and pd.notna(profit_q1) and pd.notna(profit_q2):
            if profit_q2 > 0: # Previous quarter was profitable
                profit_growth = ((profit_q1 / profit_q2) - 1) * 100
            elif profit_q2 < 0: # Previous quarter was a loss
                if profit_q1 > 0: # Turned profitable
                    profit_growth = float('inf') 
                else: # Loss reduced or increased
                    profit_growth = ((abs(profit_q2) - abs(profit_q1)) / abs(profit_q2)) * 100
            elif profit_q2 == 0: # Previous quarter was break-even
                if profit_q1 > 0:
                    profit_growth = float('inf')
                elif profit_q1 < 0:
                    profit_growth = float('-inf')

        # --- NEW: Detailed Console Output ---
        profit_growth_display = "N/A"
        if profit_growth is not None:
            if math.isinf(profit_growth):
                profit_growth_display = "Turned Profitable" if profit_growth > 0 else "Increased Loss"
            elif math.isfinite(profit_growth):
                profit_growth_display = f"{profit_growth:+.2f}%"

        revenue_growth_display = "N/A"
        if revenue_growth is not None:
            if math.isinf(revenue_growth):
                revenue_growth_display = "Positive from Zero"
            elif math.isfinite(revenue_growth):
                revenue_growth_display = f"{revenue_growth:+.2f}%"

        # --- MODIFICATION ---
        # Added flush=True to ensure output appears immediately in server logs.
        print(f"   {symbol} -> Profit : {profit_growth_display} (Q1: {format_large_number(profit_q1)}, Q2: {format_large_number(profit_q2)})", flush=True)
        print(f"   {symbol} -> Revenue: {revenue_growth_display} (Q1: {format_large_number(revenue_q1)}, Q2: {format_large_number(revenue_q2)})", flush=True)
        # --- END MODIFICATION ---

        return profit_growth, revenue_growth

    except Exception as e:
        # print(f"   [WARN] Could not fetch or calculate growth for {ticker_symbol}: {e}")
        return None, None
        
# ==============================================================================
# --- REFACTORED ANALYSIS FUNCTIONS (NO API CALLS) ---
# (NO CHANGES MADE IN THIS SECTION)
# ==============================================================================

def calculate_sma(daily_df):
    """Calculates SMA from a pre-fetched daily DataFrame."""
    if daily_df.empty: return None
    sma_period = 200 if len(daily_df) >= 200 else 50 if len(daily_df) >= 50 else 0
    if sma_period == 0: return None
    sma = round(daily_df["Close"][-sma_period:].mean(), 2)
    is_above = (daily_df["Close"][-10:] > sma).any()
    return (sma, sma_period, is_above)

def calculate_pm_logic(daily_df):
    """Calculates previous month breakout from a pre-fetched daily DataFrame."""
    if daily_df.empty: return ""
    
    df_full = daily_df.copy()
    
    today = date.today()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    last_day_prev2_month = first_day_prev_month - timedelta(days=1)
    first_day_prev2_month = last_day_prev2_month.replace(day=1)
    
    df_pm = df_full.loc[first_day_prev_month:last_day_prev_month]
    df_pp = df_full.loc[first_day_prev2_month:last_day_prev2_month]
    df_current = df_full[df_full.index.date >= first_day_this_month]
    
    pm_high = df_pm["High"].max() if not df_pm.empty else None
    pp_high = df_pp["High"].max() if not df_pp.empty else None
    
    # --- CHANGE IMPLEMENTED HERE: Using "High" price instead of "Close" for breakout check ---
    if pm_high and not df_current.empty and (df_current["High"] > pm_high).any():
        breakout_date = df_current[df_current["High"] > pm_high].index[0]
        return f"{last_day_prev_month.strftime('%B')} ({breakout_date.strftime('%d %B')})"
    if pp_high and not df_pm.empty and (df_pm["High"] > pp_high).any():
        breakout_date = df_pm[df_pm["High"] > pp_high].index[0]
        return f"{last_day_prev2_month.strftime('%B')} ({breakout_date.strftime('%d %B')})"
    return ""

def calculate_inside_bar(daily_df, interval):
    """Calculates inside bar count from a pre-fetched daily DataFrame."""
    if daily_df.empty: return 0

    # --- CHANGE IMPLEMENTED HERE: Set tolerance based on interval ---
    if interval == 'daily':
        tolerance_multiplier = 1.01  # 1% tolerance for Daily Inside Bars
    else:
        tolerance_multiplier = 1.02  # 2% tolerance for Weekly and Monthly

    resample_map = {'monthly': 'ME', 'weekly': 'W', 'daily': 'D'}
    resample_period = resample_map[interval]

    # Define aggregation rules for resampling
    agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    df = daily_df.resample(resample_period).agg(agg_rules).dropna()

    if not df.empty:
        last_timestamp = df.index[-1]
        now_timestamp = pd.Timestamp.now()
        if interval == 'daily' and last_timestamp.date() == now_timestamp.date(): df = df.iloc[:-1]
        if interval == 'weekly' and last_timestamp.week == now_timestamp.week: df = df.iloc[:-1]
        if interval == 'monthly' and last_timestamp.month == now_timestamp.month: df = df.iloc[:-1]

    if len(df) < 2: return 0
    count = 0
    for i in range(len(df) - 1, 0, -1):
        if (df.iloc[i]['High'] <= df.iloc[i-1]['High'] * tolerance_multiplier) and (df.iloc[i]['Close'] >= df.iloc[i-1]['Low']):
            count += 1
            if count >= 5: break
        else:
            break
    return count

def calculate_monthly_hh_count(daily_df):
    """Calculates the number of consecutive monthly higher highs."""
    if daily_df.empty:
        return 0

    agg_rules = {'High': 'max'}
    df_monthly = daily_df.resample('ME').agg(agg_rules).dropna()

    # Remove current incomplete month if it exists
    if not df_monthly.empty and df_monthly.index[-1].month == datetime.now().month:
        df_monthly = df_monthly.iloc[:-1]

    # Need at least 2 months to make a comparison
    if len(df_monthly) < 2:
        return 0

    count = 0
    # Iterate backwards from the last completed month up to a max of 5 comparisons
    for i in range(1, min(6, len(df_monthly))):
        current_month_high = df_monthly.iloc[-i]['High']
        previous_month_high = df_monthly.iloc[-(i + 1)]['High']

        if current_month_high > previous_month_high:
            count += 1
        else:
            # The chain of higher highs is broken, so stop counting
            break
    return count

# --- NEW: Combined Intraday Scan Function ---
def calculate_intraday_momentum_scan(intraday_data, is_fno):
    """
    Calculates the most recent significant up and down moves from pre-fetched intraday data.
    """
    up_result = ""
    down_result = ""

    if not intraday_data:
        return up_result, down_result

    combined_df = pd.concat(intraday_data)
    
    # --- UP SCAN LOGIC ---
    up_threshold = 1.5 if is_fno else 3.0
    combined_df['gain'] = ((combined_df['Close'] / combined_df['Low']) - 1) * 100
    significant_gains = combined_df[combined_df['gain'] >= up_threshold].copy()
    
    if not significant_gains.empty:
        # Sort by timestamp (latest first) to find the most recent signal
        sorted_gains = significant_gains.sort_index(ascending=False)
        latest_candle = sorted_gains.iloc[0]
        latest_gain = latest_candle['gain']
        timestamp_of_latest_gain = latest_candle.name
        formatted_time = timestamp_of_latest_gain.strftime("%d %B, %I:%M %p")
        up_result = f"'+{latest_gain:.2f}% ({formatted_time})"

    # --- DOWN SCAN LOGIC ---
    down_threshold = 1.5 if is_fno else 3.0
    combined_df['drop'] = (1 - (combined_df['Close'] / combined_df['High'])) * 100
    significant_drops = combined_df[combined_df['drop'] >= down_threshold].copy()

    if not significant_drops.empty:
        # Sort by timestamp (latest first) to find the most recent signal
        sorted_drops = significant_drops.sort_index(ascending=False)
        latest_candle = sorted_drops.iloc[0]
        max_drop = latest_candle['drop']
        timestamp_of_max_drop = latest_candle.name
        formatted_time = timestamp_of_max_drop.strftime("%d %B, %I:%M %p")
        down_result = f"'-{max_drop:.2f}% ({formatted_time})"

    return up_result, down_result

# ==============================================================================
# --- MAIN SCRIPT LOGIC ---
# (NO CHANGES MADE IN THIS SECTION)
# ==============================================================================

def run_ath_and_price_scan(fno_symbols_set, symbols_for_processing, symbol_to_row_map):
    """Main function to run the entire scanning process."""
    print("\n--- Starting ATH + Price Scan Logic ---")

    # === Check if ATH update is allowed ===
    now = datetime.now()
    ath_allowed = (
        (now.weekday() == 4 and now.time() >= dt_time(16, 0)) or
        (now.weekday() > 4) or
        (now.weekday() == 0 and now.time() <= dt_time(8, 0))
    )
    # print(f"[DEBUG] Is ATH update allowed? {ath_allowed}")

    # === ATH Cache Update (Columns AJ and AK) ===
    if ath_allowed:
        print("📈 Running ATH Cache Update using Angel One API...")
        today_str = now.strftime("%d-%b-%Y")
        updates = 0
        updates_to_sheet = []

        try:
            all_cache_data = cache_sheet.get_all_values()
        except Exception as e:
            print(f"❌ Error reading from ATH Cache sheet: {e}")
            all_cache_data = []

        cache_data_map = {
            row[34].strip().upper().replace("-EQ", "").replace("-BE", ""): 
            (row[35] if len(row) > 35 else None, row[36] if len(row) > 36 else None) 
            for row in all_cache_data[2:] if len(row) > 34 and row[34].strip()
        }
        
        # =================================================================
        # --- CHANGE IMPLEMENTED HERE ---
        # Added a progress tracker for the ATH update process.
        symbols_to_update_ath = [s for s in symbols_for_processing if cache_data_map.get(s['base_symbol'], (None, None))[1] != today_str]
        total_symbols_to_update = len(symbols_to_update_ath)
        
        for i, symbol_info in enumerate(symbols_to_update_ath):
            base_symbol = symbol_info['base_symbol']
            print(f"   [{i+1}/{total_symbols_to_update}] Processing ATH for {base_symbol}...")
        # =================================================================
            
            token = symbol_info['token']
            exchange = symbol_info['exchange']
            
            gs_row_num = symbol_to_row_map.get(base_symbol)
            if not gs_row_num:
                continue 

            new_ath = fetch_full_history_for_ath(token, exchange)

            if new_ath is not None and math.isfinite(new_ath):
                new_ath = round(new_ath, 2)
                prev_ath_str, _ = cache_data_map.get(base_symbol, (None, None))
                prev_ath = None
                try:
                    if prev_ath_str: prev_ath = float(prev_ath_str)
                except (ValueError, TypeError): pass

                if new_ath != prev_ath:
                    updates_to_sheet.append({'range': f'AJ{gs_row_num}:AK{gs_row_num}', 'values': [[new_ath, today_str]]})
                    updates += 1
            time.sleep(API_REQUEST_DELAY)

        if updates_to_sheet:
            try:
                cache_sheet.batch_update(updates_to_sheet, value_input_option='USER_ENTERED')
                print(f"✅ ATH Cache update complete: {updates} record(s) updated.")
            except Exception as e:
                print(f"❌ Error performing batch update for ATH Cache: {e}")
        else:
            print("ℹ️ No ATH records needed updating.")
    else:
        print(f"🕒 Skipping ATH Cache update (not in allowed time range).")


    # === Price Scan - Fetching LTPs ===
    print("⚡ Fetching LTPs for price scan using Angel One API...")
    try:
        ath_data_raw = cache_sheet.get_all_values()
        ath_data = [row for row in ath_data_raw[2:] if len(row) > 35 and row[34].strip()]
    except Exception as e:
        print(f"❌ Error reading ATH data for LTP calculation: {e}")
        ath_data = []

    ath_map = {row[34].strip().upper().replace("-EQ", "").replace("-BE", ""): float(row[35]) for row in ath_data if row[35]}


    ltp_map = {}
    tokens_to_fetch_ltp = [s['token'] for s in symbols_for_processing if s['token']]
    if tokens_to_fetch_ltp:
        tokens_by_exchange = {}
        for s_info in symbols_for_processing:
            if s_info['token'] not in tokens_by_exchange.setdefault(s_info['exchange'], []):
                tokens_by_exchange[s_info['exchange']].append(s_info['token'])

        for exchange, tokens in tokens_by_exchange.items():
            for i in range(0, len(tokens), 50):
                batch_tokens = tokens[i:i+50]
                payload = {"mode": "LTP", "exchangeTokens": {exchange: batch_tokens}}
                try:
                    response = smart_api_obj.getMarketData(**payload)
                    if response and response.get("status") and response.get("data"):
                        for item in response["data"]["fetched"]:
                            ltp_map[item['symbolToken']] = item.get('ltp')
                    time.sleep(API_REQUEST_DELAY)
                except Exception as e:
                    print(f"❌ Error fetching LTP batch for {exchange}: {e}")

    print(f"✅ LTP data fetched for {len(ltp_map)} records.")

    # === NEW EFFICIENT WORKFLOW =================================================
    print("🚚 Starting efficient detailed analysis...")

    # --- STEP 1: Pre-filter symbols that have basic data ---
    pre_filtered_symbols = []
    for s_info in symbols_for_processing:
        ltp = ltp_map.get(s_info['token'])
        ath = ath_map.get(s_info['base_symbol'])
        if ltp and ath:
            s_info.update({'ltp': ltp, 'ath': ath})
            pre_filtered_symbols.append(s_info)

    # --- STEP 2: Fetch 1 year of daily data ONCE for pre-filtered stocks ---
    print(f"   [1/4] Fetching 1 year of daily data for {len(pre_filtered_symbols)} stocks...")
    historical_data_map = {}
    end_date = datetime.now()
    start_date = end_date - relativedelta(years=1)
    for s_info in pre_filtered_symbols:
        token = s_info['token']
        exchange = s_info['exchange']
        df_daily = fetch_history_angelone(token, exchange, "ONE_DAY", start_date, end_date)
        historical_data_map[token] = df_daily
    print(f"   ✅ Fetched daily data for {len(historical_data_map)} stocks.")

    # --- STEP 3: Perform final filtering and all calculations from stored data ---
    print("   [2/4] Performing final filtering and all calculations...")
    scanned_symbols_info = []
    final_output_rows = []
    
    print("       - Applying filter: F&O stocks are included automatically, Cash stocks filtered by -40% from ATH...")
    for s_info in pre_filtered_symbols:
        token = s_info['token']
        daily_df = historical_data_map.get(token)

        if daily_df is None or daily_df.empty:
            continue
        
        three_months_ago = datetime.now() - relativedelta(months=3)
        lowest_low = daily_df[daily_df.index >= three_months_ago]['Low'].min()
        
        if not math.isfinite(lowest_low):
             continue

        drop_pct = round(((lowest_low / s_info['ath']) * 100) - 100, 2)
        
        # =================================================================
        # --- CHANGE IMPLEMENTED AS PER YOUR REQUEST ---
        # F&O stocks now bypass the -40% filter. The filter only applies to Cash (non-F&O) stocks.
        is_fno_check = s_info['base_symbol'] in fno_symbols_set
        if is_fno_check or drop_pct >= -40:
        # =================================================================
            s_info['drop_pct'] = drop_pct
            scanned_symbols_info.append(s_info)

    print(f"       - Found {len(scanned_symbols_info)} stocks passing the filter.")
    print("       - Calculating all technical indicators for filtered stocks...")

    for s_info in scanned_symbols_info:
        token = s_info['token']
        daily_df = historical_data_map.get(token)
            
        sma_data = calculate_sma(daily_df)
        pm_flag = calculate_pm_logic(daily_df)
        # =================================================================
        # --- CHANGE IMPLEMENTED AS PER YOUR REQUEST ---
        # Removed the function call for the previous month low breakdown.
        # =================================================================
        mib_flag = calculate_inside_bar(daily_df, 'monthly')
        wib_flag = calculate_inside_bar(daily_df, 'weekly')
        dib_flag = calculate_inside_bar(daily_df, 'daily')
        monthly_hh_count = calculate_monthly_hh_count(daily_df)
        
        # --- Fetch intraday data for equity momentum scan ---
        start_date_scan = datetime.now() - relativedelta(weeks=2)
        end_date_scan = datetime.now()
        timeframe_map_equity = {"ONE_HOUR": "60 min", "THIRTY_MINUTE": "30 min", "FIFTEEN_MINUTE": "15 min"}
        intraday_data_list = []
        for tf_code, tf_display in timeframe_map_equity.items():
            df = fetch_history_angelone(token, s_info['exchange'], tf_code, start_date_scan, end_date_scan)
            if not df.empty:
                df['timeframe'] = tf_display
                intraday_data_list.append(df)
        
        is_fno = s_info['base_symbol'] in fno_symbols_set
        intraday_up_result, intraday_down_result = calculate_intraday_momentum_scan(intraday_data_list, is_fno)

        # --- Fetch Fundamental Growth ---
        profit_growth, revenue_growth = get_quarterly_growth(s_info['base_symbol'], s_info['exchange'])
        time.sleep(0.2) 

        sanitized_profit_growth = profit_growth if profit_growth is not None and math.isfinite(profit_growth) else ""
        sanitized_revenue_growth = revenue_growth if revenue_growth is not None and math.isfinite(revenue_growth) else ""

        # =================================================================
        # --- CHANGE IMPLEMENTED AS PER YOUR REQUEST ---
        # Restored the original logic for Column J.
        sma_display, ltp_gt_sma_flag = "", ""
        if sma_data:
            sma_value, sma_period, is_above = sma_data
            sma_display = f"{sma_value}({sma_period})"
            if is_above:
                ltp_gt_sma_flag = f">{sma_period}"
        # =================================================================

        # =================================================================
        # --- CHANGE IMPLEMENTED AS PER YOUR REQUEST ---
        # Final row data structure updated to restore ltp_gt_sma_flag in Column J.
        row_data = [
            "FNO" if is_fno else "Cash", # C
            s_info['ltp'],               # D
            s_info['ath'],               # E
            s_info['drop_pct'],          # F
            mib_flag,                    # G
            monthly_hh_count,            # H
            sma_display,                 # I
            ltp_gt_sma_flag,             # J (ORIGINAL LOGIC RESTORED)
            pm_flag,                     # K
            wib_flag,                    # L
            dib_flag,                    # M
            intraday_up_result,          # N
            intraday_down_result,        # O
            sanitized_profit_growth,     # P
            sanitized_revenue_growth,    # Q
        ]
        # =================================================================
        final_output_rows.append(row_data)

    print(f"✅ Final analysis complete. Found {len(scanned_symbols_info)} stocks passing all filters.")

    # --- STEP 4: Write filtered symbols and results to Google Sheet ---
    print("   [3/4] Writing final results to the sheet...")
    if scanned_symbols_info:
        try:
            # Clear the range up to column Q only
            cache_sheet.batch_clear(['B3:Q2500'])
            
            # Write symbols to column B
            column_b_data = [[f"{s['exchange']}:{s['base_symbol']},"] for s in scanned_symbols_info]
            range_to_update_b = f"B3:B{len(column_b_data) + 2}"
            cache_sheet.update(values=column_b_data, range_name=range_to_update_b, value_input_option='USER_ENTERED')
            print(f"   ✅ Wrote {len(column_b_data)} filtered symbols to Column B.")

            if final_output_rows:
                # Write details up to column Q
                range_to_update_details = f"C3:Q{len(final_output_rows) + 2}"
                cache_sheet.update(values=final_output_rows, range_name=range_to_update_details, value_input_option='USER_ENTERED')
                
                # Update formatting ranges
                cache_sheet.format(f"D3:F{len(final_output_rows) + 2}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}})
                cache_sheet.format(f"P3:Q{len(final_output_rows) + 2}", {'numberFormat': {'type': 'PERCENT', 'pattern': '0.00"%"'}})
                print(f"   ✅ All detailed scan results written to ATH Cache sheet.")

        except Exception as e:
            print(f"❌ Error writing data to ATH Cache sheet: {e}")
    else:
        print("ℹ️ No symbols passed the final filter. Clearing old results.")
        try:
            cache_sheet.batch_clear(['B3:Q2500'])
        except Exception as e:
            print(f"❌ Error clearing old results: {e}")

    print("\n--- Technical scan process finished. ---")


# --- START: NEW WRAPPER AND SCHEDULER LOGIC ---

def run_daily_scan():
    """This function contains the entire 4-hour analysis logic."""
    print(f"[{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}] Starting the daily 4-hour scan...")
    
    initialize_services()
    
    try:
        # --- NEW WORKFLOW ---
        # 1. First, get all symbols from the sheet and find their tokens
        print("[DEBUG] Pre-loading all symbols from Google Sheet...")
        all_sheet_data = stock_sheet.get_all_values()[2:]
        symbols_raw = [row[34] for row in all_sheet_data if len(row) > 34 and row[34]]
        symbol_to_row_map = {
            row[34].strip().upper().replace("-EQ", "").replace("-BE", ""): index + 3 
            for index, row in enumerate(all_sheet_data) if len(row) > 34 and row[34].strip()
        }
        
        symbols_for_processing = []
        for s_raw in symbols_raw:
            if not isinstance(s_raw, str) or not s_raw.strip(): continue
            clean_s = s_raw.strip().upper().replace("-EQ", "").replace("-BE", "")
            if clean_s in SYMBOLS_TO_EXCLUDE: continue
            
            token_nse, exch_nse = get_token_for_symbol(clean_s, "NSE")
            if token_nse:
                symbols_for_processing.append({'base_symbol': clean_s, 'token': token_nse, 'exchange': exch_nse})
                continue
            token_bse, exch_bse = get_token_for_symbol(clean_s, "BSE")
            if token_bse:
                symbols_for_processing.append({'base_symbol': clean_s, 'token': token_bse, 'exchange': exch_bse})

        print(f"✅ Found tokens for {len(symbols_for_processing)} total symbols.")
    except Exception as e:
        print(f"❌ Critical error during symbol pre-loading: {e}")
        symbols_for_processing = []
        symbol_to_row_map = {}

    # 2. Get the list of F&O symbols
    fno_symbols_set = get_fno_symbols()
    
    # 3. Run the main analysis scan
    run_ath_and_price_scan(fno_symbols_set, symbols_for_processing, symbol_to_row_map)
        
    print(f"[{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}] Daily scan has finished.")


def run_scheduler():
    """
    Sets up and runs the job schedule in a loop.
    This function will run in a separate thread.
    """
    # NOTE: Render servers run on UTC time. 3:00 PM IST is 09:30 UTC.
    schedule.every().day.at("09:30").do(run_daily_scan)
    print(f"--- [RENDER-DEBUG] Scheduler initialized. Waiting for the scheduled time (09:30 UTC / 15:00 IST). Current UTC time: {datetime.utcnow().strftime('%H:%M:%S')} ---")

    while True:
        schedule.run_pending()
        # Sleep for a minute before checking the schedule again.
        time.sleep(60)

# --- NEW RENDER-COMPATIBLE STARTUP LOGIC ---
def start_background_tasks():
    """
    This function runs the initial scan and then starts the scheduler.
    It's designed to be run in a background thread.
    """
    print("\n" + "="*50)
    print("--- [RENDER-DEBUG] Background task thread started ---")
    print("--- [RENDER-DEBUG] Performing initial manual scan on startup... ---")
    print("="*50 + "\n")
    try:
        run_daily_scan()
        print("\n" + "="*50)
        print("--- [RENDER-DEBUG] Initial manual scan complete. ---")
        print("--- [RENDER-DEBUG] The service will now start the scheduler for 3 PM runs. ---")
        print("="*50 + "\n")
    except Exception as e:
        print(f"\n❌❌ [RENDER-DEBUG] An error occurred during the initial manual scan: {e}\n")
    
    # After the initial scan, start the scheduler for subsequent daily runs
    run_scheduler()

# This block ensures the background tasks are started only once, 
# even in environments that might spawn multiple worker processes.
# This is the correct way to start background jobs with Gunicorn.
# --- MODIFICATION: Corrected typo from TASS to TASKS ---
if 'BACKGROUND_TASKS_STARTED' not in os.environ:
    print("[RENDER-DEBUG] First worker process detected. Starting background tasks.")
    background_thread = threading.Thread(target=start_background_tasks, daemon=True)
    background_thread.start()
    os.environ['BACKGROUND_TASKS_STARTED'] = 'true'
    print("[RENDER-DEBUG] Background thread has been successfully started.")
else:
    print("[RENDER-DEBUG] Background tasks were already started by another worker. Skipping.")

# The `if __name__ == "__main__":` block is for local execution only.
# Gunicorn does not run this part.
if __name__ == "__main__":
    print("[LOCAL-DEBUG] Running locally. Starting Flask app directly.")
    # The background tasks are already started above, so we just run the app.
    # Note: On Windows, you might see the startup logs twice due to how Flask's reloader works.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

