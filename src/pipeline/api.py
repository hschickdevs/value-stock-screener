import warnings; warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import requests
from functools import cached_property
import random
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from yahoo_fin.stock_info import tickers_nasdaq, get_data


class StockDataAPI:
    def __init__(self) -> None:
        pass
    
    def get_exchange_tickers(self, exchanges: list[str], randomized: bool = True) -> set:
        """
        Needs to fetch all tickers in the exchange,
        and then map them to the ticker symbol and name from the macrotrends API.
        
        Returns (pd.DataFrame):
            ticker,name
        """
        # Generate the set of valid tickers based on the exchange
        tickers = set()
        for exchange in exchanges:
            if exchange.lower() == "nasdaq":
                tickers.update(tickers_nasdaq())
            elif exchange.lower() == "nyse":
                headers = {
                    'authority': 'api.nasdaq.com',
                    'accept': 'application/json, text/plain, */*',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                    'origin': 'https://www.nasdaq.com',
                    'sec-fetch-site': 'same-site',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-dest': 'empty',
                    'referer': 'https://www.nasdaq.com/',
                    'accept-language': 'en-US,en;q=0.9',
                }
                r = requests.get("https://api.nasdaq.com/api/screener/stocks?exchange=nyse&download=true", headers=headers).json()
                tickers.update([row['symbol'] for row in r['data']['rows']])
            else:
                raise NotImplementedError("Exchange must be either 'nasdaq' or 'nyse'")

        # Exclude conditions
        exclude = ['W', 'R', 'P', 'Q']
        is_valid = lambda symbol: not (len(symbol) > 4 and symbol[-1] in exclude)

        # Apply the filter to clean the list of symbols
        valid_tickers = {symbol for symbol in tickers if is_valid(symbol)}
        
        # Macrotrends tickers
        df = self.all_macrotrends_tickers
        
        # Filter the DataFrame to include only rows with valid tickers
        filtered_df = df[df['symbol'].isin(valid_tickers)]
        
        # Randomize the DataFrame if randomized is True
        if randomized:
            filtered_df = filtered_df.sample(frac=1).reset_index(drop=True)
            
        return filtered_df
    
    def get_pe_ratio_history(self, symbol: str, company_name: str, macrotrends_url: str) -> tuple[pd.DataFrame, float]:
        """
        Fetch the P/E ratio history for a given stock symbol.
        
        :returns: (DataFrame, float)
        - DataFrame: The P/E ratio history for the stock.
        - float: The current P/E ratio for the stock.
        """
        r = requests.get(f"https://www.macrotrends.net/stocks/charts/{macrotrends_url}/pe-ratio",
                        headers=self._request_headers)
        
        # Parsing the response content with BeautifulSoup
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        
        # Extract the latest P/E ratio element
        pe_ratio_element = soup.select_one('#main_content > div:nth-of-type(2) > span > p:nth-of-type(1) > strong')
        current_pe_ratio = float(pe_ratio_element.text)
        
        # Read the table and skip the first row
        df = pd.read_html(str(table), header=1)[0]
        
        # Rename all columns by doing lowercase and replacing spaces with underscores
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        df['ttm_net_eps'] = df['ttm_net_eps'].replace('[\$,]', '', regex=True).astype(float)
        
        df.drop(['stock_price'], inplace=True, axis=1)
                
        return df, current_pe_ratio
        
    def get_pb_ratio_history(self, symbol: str, company_name: str, macrotrends_url: str) -> tuple[pd.DataFrame, float]:
        """
        Fetch the P/B ratio history for a given stock symbol.
        
        :returns: (DataFrame, float)
        - DataFrame: The P/B ratio history for the stock.
        - float: The current P/B ratio for the stock.
        """
        r = requests.get(f"https://www.macrotrends.net/stocks/charts/{macrotrends_url}/price-book",
                        headers=self._request_headers)
        
        # Parsing the response content with BeautifulSoup
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        
        # Get the latest P/B ratio
        pb_ratio_element = soup.select_one('#main_content > div:nth-child(2) > span > p:nth-child(1) > strong')
        current_pb_ratio = float(pb_ratio_element.text)
        
        # Read the table and skip the first row
        df = pd.read_html(str(table), header=1)[0]
        
        # Rename columns
        new_columns = {
            'Date': 'date',
            'Stock Price': 'stock_price',
            'Book Value per Share': 'book_value_per_share',
            'Price to Book Ratio': 'price_to_book_ratio'
        }
        df.rename(columns=new_columns, inplace=True)
        
        # Clean the data
        df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Convert to datetime, coerce errors to NaT
        df = df.dropna(subset=['date'])  # Drop rows where date conversion failed
        
        # Ensure other columns are cleaned
        df['stock_price'] = df['stock_price'].replace('[\$,]', '', regex=True).astype(float)
        df['book_value_per_share'] = df['book_value_per_share'].replace('[\$,]', '', regex=True).astype(float)
        df['price_to_book_ratio'] = df['price_to_book_ratio'].astype(float)
        
        # Append metadata
        df['symbol'] = symbol
        df['name'] = company_name
        columns_order = ['symbol', 'name'] + [col for col in df.columns if col not in ['symbol', 'name']]
        df = df[columns_order]
                
        return df, current_pb_ratio
    
    @cached_property
    def all_macrotrends_tickers(self) -> pd.DataFrame:
        r = requests.get("https://www.macrotrends.net/assets/php/ticker_search_list.php", 
                         headers=self._request_headers).json()
        
        return pd.DataFrame.from_records(
            (
                {
                    'symbol': company['n'].split(' - ')[0],
                    'name': company['s'].split('/')[1],
                    'full_name': company['n'].split(' - ')[1],
                    'url': company['s']
                }
                for company in r
            )
        )
        
    @property
    def _request_headers(self):
        return {'User-Agent': UserAgent().random}