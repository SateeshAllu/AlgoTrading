import requests
import webbrowser
import json
import pandas as pd
from urllib.parse import quote
from datetime import datetime, timedelta
import os
import logging
import csv
import time
from tabulate import tabulate
from pandas.tseries.offsets import Week


# Configuration Constants
API_KEY = '193ad638-6837-45f2-8558-a17a6476bf73'
SECRET_KEY = 't21vuv7mi8'
RURL = 'https://api.upstox.com/v2/login'
#TOKEN_FILE_PATH = r'C:\Users\satee\Desktop\accessToken.json'
TOKEN_FILE_PATH = "accessToken.json"
LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
MAX_RETRIES = 3  # Maximum number of retries for fetching option contracts
MIN=5

NIFTY_SYMBOL = "NSE_INDEX|Nifty 50"
BANKNIFTY_SYMBOL = "NSE_INDEX|Nifty Bank"
SENSEX_SYMBOL = "BSE_INDEX|SENSEX"

# Define indices and their corresponding expiry rules
EXPIRY_RULES = {
    'NSE_INDEX|Nifty 50': 'Thursday',
    'NSE_INDEX|Nifty Bank': 'Wednesday',
    'BSE_INDEX|SENSEX': 'Tuesday',
    'NSE_INDEX|Nifty Fin Service': 'Tuesday',
    'BSE_INDEX|BANKEX': 'Monday',
    #'NSE_INDEX|Nifty Midcap': 'Monday'
}

# Configure logging
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

class UpstoxAPI:
    def __init__(self, api_key, secret_key, redirect_url):
        self.api_key = api_key
        self.secret_key = secret_key
        self.redirect_url = redirect_url
        #self.token = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2TEEzTjgiLCJqdGkiOiI2Nzk4NDkyNzk4YTJjMjMyMmQ5YTYwMTciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzM4MDMzNDQ3LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3MzgxMDE2MDB9.V8ywACVNIZxAXrGoasW1r1-Y9UZ-49F2CNzJivfgLc0'
        self.token=self.load_access_token()
        self.base_mkt_url = 'https://api.upstox.com/v2/market-quote/quotes'
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.token}',
        }

    def build_auth_url(self):
        """Build the authorization URL."""
        return f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={self.api_key}&redirect_uri={quote(self.redirect_url)}'

    def open_auth_url(self):
        """Open the authorization URL in a web browser."""
        webbrowser.open(self.build_auth_url())
        logging.info("Authorization URL opened in the browser.")

    def fetch_access_token(self, code):
        """Fetch the access token using the authorization code."""
        url = 'https://api-v2.upstox.com/login/authorization/token'
        data = {
            'code': code,
            'client_id': self.api_key,
            'client_secret': self.secret_key,
            'redirect_uri': self.redirect_url,
            'grant_type': 'authorization_code'
        }
        response = requests.post(url, headers=self.get_headers(), data=data)
        response.raise_for_status()
        access_token = response.json()['access_token']
        self.save_access_token(access_token)
        return access_token

    def save_access_token(self, token):
        """Save the access token to a file."""
        with open(TOKEN_FILE_PATH, 'w') as file:
            json.dump({'access_token': token}, file)
        logging.info("Access token saved to file.")

    def load_access_token(self):
        if os.path.exists(TOKEN_FILE_PATH):
            with open(TOKEN_FILE_PATH, 'r') as file:
                access_token = file.read().strip()
            logging.info("Access token loaded from file.")
            return access_token
        else:
            logging.info("No token file avialable")

    def get_headers(self):
        """Return headers for requests."""
        return {
            'Accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {self.token}' if self.token else ''
        }

    def make_request(self, method, url, params=None, data=None):
        """Generic function for making HTTP requests."""
        response = requests.request(method, url, headers=self.get_headers(), params=params, json=data)
        response.raise_for_status()
        return response.json()

    def get_user_profile(self):
        """Fetch the user profile using the stored access token."""
        url = 'https://api-v2.upstox.com/user/get-funds-and-margin'
        return self.make_request('GET', url)

    def get_expiry_date(self, index, expiry_day):
        """
        Calculate the expiry date for a given index based on updated rules:
        - Nifty 50 expiry: Always Thursday
        - BankNifty expiry: Always Wednesday
        - Sensex expiry: Always Friday
        - Finnifty expiry: Always Tuesday
        - Bankex and Midcap expiry: Always Monday
        """

        today = datetime.today()
        current_weekday = today.weekday()

        # Expiry weekdays mapping
        expiry_days = {
            'NSE_INDEX|Nifty 50': 3,        # Thursday
            'NSE_INDEX|Bank Nifty': 2,      # Wednesday
            'BSE_INDEX|SENSEX': 1,          # Friday
            'NSE_INDEX|Finnifty': 1,        # Tuesday
            'NSE_INDEX|Bankex': 0,          # Monday
            'NSE_INDEX|Midcap': 0           # Monday
        }

        # Check if index is in expiry_days mapping
        if index not in expiry_days:
            logging.warning(f"Index {index} is not handled by the new rules.")
            return None
        
        # Calculate the target expiry date based on weekday
        target_weekday = expiry_days[index]
        days_to_expiry = (target_weekday - current_weekday) % 7  # Calculate the days to the next expiry

        # Special handling based on passed expiry_weekday argument
        if expiry_day is not None:
            if expiry_day == current_weekday:
                logging.info(f"Expiry day is today for {index}.")
                return today.strftime('%Y-%m-%d')
            elif expiry_day < current_weekday:
                # If the expiry_weekday is earlier in the week, move to next week's expiry
                days_to_expiry = (target_weekday - current_weekday) + 7
            else:
                days_to_expiry = (expiry_day - current_weekday) % 7
        
        # Determine expiry date by adding days to today's date
        expiry_date = today + timedelta(days=days_to_expiry)

        # Adjust for holidays
        expiry_date = self.adjust_for_holiday(expiry_date.strftime('%Y-%m-%d'))
        
        logging.info(f"Calculated expiry date for {index}: {expiry_date}")
        return expiry_date
    
    def get_daily_data(self, index):
        """Fetch daily data for the given index."""
        url = f"https://api.upstox.com/v2/market-quote/instruments/{quote(index)}"
        try:
            response = self.make_request('GET', url)
            return response.get('data', {})
        except Exception as e:
            logging.error(f"Error fetching daily data for {index}: {e}")
            return {}




    def adjust_for_holiday(self, expiry_date):
        """Check if expiry date is a holiday and adjust to the previous trading day."""
        # Implement a logic to check for holidays
        expiry_date = datetime.strptime(expiry_date, '%Y-%m-%d')
        if expiry_date.weekday() >= 5:  # Saturday or Sunday
            expiry_date -= timedelta(days=(expiry_date.weekday() - 4))  # Move to Friday
        return expiry_date.strftime('%Y-%m-%d')

    def fetch_option_contracts(self, index,expiry_date):
        """Fetch option contracts for the given index, retrying if no data is found."""
        #expiry_date = self.get_expiry_date(index)
        #expiry_date = self.adjust_for_holiday(expiry_date)
        
        for attempt in range(MAX_RETRIES):
            url = f"https://api.upstox.com/v2/option/contract?instrument_key={index}&expiry_date={expiry_date}"
            option_data = self.make_request('GET', url).get('data', [])
            
            if option_data:
                return option_data
            
            logging.warning(f"No option contracts found for {index} on {expiry_date}")
            expiry_date = (datetime.strptime(expiry_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')  # Reduce expiry date by 1
            logging.warning(f"Retrying for {index} with {expiry_date}. Retrying...")
            
        logging.error(f"Failed to retrieve option contracts for {index} after {MAX_RETRIES} attempts.")
        return []  # Return an empty list if all attempts fail

    def get_market_data(self, instrument_keys):
        """Fetch market data for the given instrument key."""
        instrument_key_param = ",".join(instrument_keys)
        url = f"{self.base_mkt_url}?instrument_key={instrument_key_param}"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.json()  # Return the response JSON data
        else:
            logging.error(f"Error fetching market data: {response.status_code}, {response.text}")
            return None
    def fetch_daily_contracts(self, index):
        """
        Fetch option contracts for the given index for the current date.
        This function bypasses expiry-specific logic and retrieves daily contracts.
        """
        today = datetime.today().strftime('%Y-%m-%d')
        url = f"https://api.upstox.com/v2/option/contract?instrument_key={index}&date={today}"
        logging.info(f"Fetching daily contracts for {index} on {today}.")

        for attempt in range(MAX_RETRIES):
            try:
                contracts_data = self.make_request('GET', url).get('data', [])
                logging.info(f"Testing ...{contracts_data}")
                if contracts_data:
                    return contracts_data
                logging.warning(f"No contracts found for {index} on {today}. Retrying...")
                time.sleep(1)  # Add delay before retrying
            except Exception as e:
                logging.error(f"Error fetching contracts: {e}")
        return []  # Return empty if no contracts found
            
    

    def process_index(self, index,expiry_day):
        """Fetch option contracts and market data for a given index."""
        logging.info(f"Fetching contracts for {index}")
        expiry_date = self.get_expiry_date(index,expiry_day)
        MAX=100
        
        if not expiry_date:
            logging.info(f"Skipping {index} as today is not its expiry day.")
            return  # Skip processing if today is not the expiry day
        option_contracts = self.fetch_option_contracts(index,expiry_date)
        contracts_data = {}

        # Store trading symbols and instrument keys
        for contract in option_contracts:
            trading_symbol = contract.get('trading_symbol')
            instrument_key = contract.get('instrument_key')
            if trading_symbol and instrument_key:
                contracts_data[trading_symbol] = instrument_key

        if contracts_data:
            # Fetch market data for the contracts
            instrument_keys = list(contracts_data.values())
            market_data = self.get_market_data(instrument_keys)

            # Process the matching instruments
            matching_instruments = self.get_matching_instruments(market_data, contracts_data,MAX)
            if not matching_instruments:
                MAX=100
                logging.info('Increasing the max logic to include more strikes')
                matching_instruments = self.get_matching_instruments(market_data, contracts_data,MAX)

            export_and_log_matching_instruments(matching_instruments)

        else:
            logging.warning(f"No contracts found for {index}.")

    def get_matching_instruments(self, market_data, contracts_data,MAX):
        """Get instruments with matching open and close prices based on instrument_token."""
        matching_instrument_keys = []
        if not market_data or 'data' not in market_data:
            logging.error("Market data could not be retrieved. Exiting function.")
            return []

        data = market_data.get('data', {})

        for trading_symbol, instrument_key in contracts_data.items():
            for instrument_token, instrument_info in data.items():
                if instrument_info.get('instrument_token') == instrument_key:
                    ohlc_data = instrument_info.get('ohlc')
                    if ohlc_data is None:
                        continue

                    open_price = ohlc_data.get('open')
                    high_price = ohlc_data.get('high')
                    last_price = instrument_info.get('last_price')

                    if open_price == high_price and (last_price > MIN and last_price < MAX):
                        matching_instrument_keys.append({
                            'trading_symbol': trading_symbol,
                            'instrument_key': instrument_key,
                            'open_price': open_price,
                            'high_price': high_price,
                            'Premium': last_price
                        })
        logging.info(f'Prining MIn and MAX Values : {MIN} : {MAX}')               
        return matching_instrument_keys
    
def sendTelegramUpdate(message):
    bot_token = "7553390752:AAFoDRWwBWzVOBbDZXQ_TQC2Sw5454lx9VE"
    chat_id = "-4543026190"
    message = message
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("Message sent successfully!")
    else:
        print(f"Failed to send message. Error: {response.text}")
    

def export_and_log_matching_instruments(matching_instruments):
    """Export matching instruments data to a CSV file and display it in tabular format in logs."""
    message_list=[]
    if not matching_instruments:
        logging.warning("No matching instruments found to export.")
        message='No OPEN HIGH Found for today Expiry'
        sendTelegramUpdate(message)
        return

    csv_file_path = 'matching_instruments.csv'
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header
        writer.writerow(['Trading Symbol', 'Instrument Key', 'Open Price', 'High Price', 'Premium'])
        for instrument in matching_instruments:
            writer.writerow([
                instrument['trading_symbol'],
                instrument['instrument_key'],
                instrument['open_price'],
                instrument['high_price'],
                instrument['Premium']
            ])
            message_list.append(f"{instrument['trading_symbol']}")
                                #, "
            #                     f"Instrument Key: {instrument['instrument_key']}, "
            #                     f"Open Price: {instrument['open_price']}, "
            #                     f"High Price: {instrument['high_price']}, "
            #                     f"Premium: {instrument['Premium']}")
           

    logging.info(f"Matching instruments exported to {csv_file_path}.")
    # Display the data in tabular format
    logging.info(tabulate(matching_instruments, headers="keys", tablefmt="grid"))
    
    # if not message_list:
    #     message = "There are no OPEN HIGH for today's expiry."
    # else:
    #     message = "\n".join(message_list)
    message = "\n".join(message_list)
    #print(f"Telegram Message : {message}")
    #message='test'
    sendTelegramUpdate(message)

def main():
    # Create an UpstoxAPI instance
    upstox_api = UpstoxAPI(API_KEY, SECRET_KEY, RURL)
    
    # Uncomment the following line to open authorization URL
    # upstox_api.open_auth_url()

    # Add your logic to retrieve the authorization code after logging in
    # authorization_code = input("Enter the authorization code: ")
    # upstox_api.fetch_access_token(authorization_code)

    #user_profile = upstox_api.get_user_profile()
    #logging.info(f"User Profile: {user_profile}")

    
    #upstox_api.process_index_daily(NIFTY_SYMBOL,expiry_day=3)
    upstox_api.process_index(NIFTY_SYMBOL, expiry_day=3,)
    upstox_api.process_index(SENSEX_SYMBOL, expiry_day=1,)
    upstox_api.process_index(BANKNIFTY_SYMBOL, expiry_day=3)


if __name__ == "__main__":
    main()
