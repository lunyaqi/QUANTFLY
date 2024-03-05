import akshare as ak
import numpy as np
import pandas as pd
from tqdm import tqdm
from Utils.utils import convert_to_datetime

def extract_stock_eod_price_history(start_date, end_date):
	stock_pool = get_stock_pool()
	all_stock_price = pd.DataFrame()
	for ticker in tqdm(stock_pool):
		try:
			price_df = get_basic_price(ticker, format_date(self.start_date), format_date(self.end_date))
			price_df['Ticker'] = ticker
			all_stock_price = pd.concat([all_stock_price, price_df], axis=0)
		except Exception as err:
			print(ticker, err)
			continue

	all_stock_price.columns = ['Date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude',
							   'pct_change', 'change', 'turnover', 'Ticker']
	# all_stock_price.set_index(['Ticker','Date'],inplace=True)
	all_stock_price['Date'] = convert_to_datetime(all_stock_price['Date'])

if __name__ == '__main__':
    stock_bid_ask_em_df = ak.stock_bid_ask_em(symbol="000001")
    print(stock_bid_ask_em_df)