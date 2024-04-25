
import datetime
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from Utils.utils import is_trading_day
from Config.conf import today_str, today_format
from Utils.logger import logger_datacube
from daily_update.ashare_1min import extract_ashare_1min_daily as ashare_1min_update
from daily_update.ashare_balance import extract_ashare_balance_daily as ashare_balance_update
from daily_update.ashare_capital import extract_ashare_capital_daily as ashare_capital_update
from daily_update.ashare_cashflow import extract_ashare_cashflow_daily as ashare_cashflow_update
from daily_update.ashare_eod_prices import extract_stock_eod_price_daily as ashare_eod_prices_update
from daily_update.ashare_income import extract_ashare_income_daily as ashare_income_update
from daily_update.ashare_new_issue import extract_ashare_new_issue_daily as ashare_new_issue_update
from daily_update.ashare_st import extract_ashare_st_daily as ashare_st_update
from daily_update.index_eod_prices import exstract_index_eod_prices_daily as index_eod_prices_update
from daily_update.index_weight import extract_index_weight_daily as index_weight_update
from daily_update.sector_cons import extract_sector_cons_daily as sectors_cons_update
from daily_update.sector_eod_prices import extract_sector_eod_prices_daily as sectors_eod_prices_update
from daily_update.sw_indu_cons import exstract_sw_indu_cons_daily as sw_indu_cons_update
from daily_update.sw_indu_eod_prices import extract_sw_indu_eod_prices_daily as sw_indu_eod_prices_update
from download_data.download_data_from_xtdata import download_daily_data


def daily_update():
    # update daily data
    try:
        if not is_trading_day(today_str):
            logger_datacube.warning(f'{today_str} is not a trading day!')
            return
        start_time = datetime.datetime.now()
        logger_datacube.info(f'{today_format} start update')

        download_daily_data(today_str, today_str)

        ashare_1min_update(today_str)
        ashare_eod_prices_update(today_str)
        ashare_balance_update(today_str, today_str)
        ashare_cashflow_update(today_str, today_str)
        ashare_income_update(today_str, today_str)
        ashare_capital_update(today_str, today_str)
        index_eod_prices_update(today_format, today_format)
        index_weight_update(today_str, today_str)
        sectors_cons_update(today_format, today_format)
        sectors_eod_prices_update(today_format, today_format)
        sw_indu_cons_update(today_format, today_format)
        sw_indu_eod_prices_update(today_format, today_format)
        ashare_st_update(today_str)
        ashare_new_issue_update(today_format, today_format)
        end_time = datetime.datetime.now()

        logger_datacube.info(f'{today_format} update done!,cost time:{end_time - start_time}')
    except Exception as e:
        logger_datacube.error(f'[ETL-DAILY] {e}')


if __name__ == '__main__':
    daily_update()
