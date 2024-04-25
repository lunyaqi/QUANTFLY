import datetime

from tqdm import tqdm
from xtquant import xtdata
from multiprocessing import Pool

from Config.conf import today_str
from Utils.logger import logger_datacube

sector_list = ['上期所',
               '上证A股',
               '上证B股',
               '上证期权',
               '上证转债',
               '中金所',
               '创业板',
               '大商所',
               '板块加权指数',
               '板块指数',
               '概念指数',
               '沪市ETF',
               '沪市债券',
               '沪市基金',
               '沪市指数',
               '沪深A股',
               '沪深B股',
               '沪深ETF',
               '沪深债券',
               '沪深基金',
               '沪深指数',
               '沪深转债',
               '沪港通',
               '深市ETF',
               '深市债券',
               '深市基金',
               '深市指数',
               '深港通',
               '深证A股',
               '深证B股',
               '深证期权',
               '深证转债',
               '科创板',
               '科创板CD']


def on_progress(data):
    print(data)


def download_stock_price(args):
    stock_code, period, start_date, end_date = args
    print(stock_code)
    try:
        xtdata.download_history_data(stock_code, period, start_time=start_date, end_time=end_date, incrementally=True)
    except Exception as e:
        logger_datacube.error(f"Error download history Price Data for {stock_code}!,{e}")


def download_period_price_data(sector_name='沪深A股', period='1d', start_date='', end_date=None):
    """
    下载历史K线数据，period支持"tick, 1m, 5m, 1d"
    """
    if sector_name in sector_list:
        stock_list = xtdata.get_stock_list_in_sector(sector_name)
    else:
        logger_datacube.error(f'sector name 输入错误!参考: {sector_list}')
        return

    if len(stock_list) == 0:
        print("stock_list is empty, pass")
        return
    start_time = datetime.datetime.now()
    logger_datacube.info(f"{sector_name}| {period} | Price Data | {start_date}-{end_date} | Download Begin")

    try:
        with Pool(8) as p:
            p.map(download_stock_price, [(stock_code, period, start_date, end_date) for stock_code in stock_list])
        # xtdata.download_history_data(stock_list, period, start_time=start_date, end_time=end_date,
        #                               callback=on_progress)

        end_time = datetime.datetime.now()
        logger_datacube.info(
            f"{sector_name}| {period} | Price Data | {start_date}-{end_date} | Download End, "
            f"Cost Time ={end_time - start_time} ")

    except Exception as e:
        logger_datacube.error(f"Error download {start_date}-{end_date} Price Data!,{e}")


def download_period_financial_data(sector_name='沪深A股', start_date='', end_date=None):
    """
    下载历史财务数据
    :param sector_name: 板块名称
    :param start_date:
    :param end_date:
    :return:
    """
    if sector_name in sector_list:
        stock_list = xtdata.get_stock_list_in_sector(sector_name)
    else:
        logger_datacube.error(f'sector name 输入错误!参考: {sector_list}')
        return

    if len(stock_list) == 0:
        print("stock_list is empty, pass")
        return
    start_time = datetime.datetime.now()
    logger_datacube.info(f"{sector_name}| Financial Data | {start_date}-{end_date} | Download Begin")

    try:
        xtdata.download_financial_data2(stock_list,
                                        table_list=['Balance', 'Income', 'CashFlow', 'Capital', 'PershareIndex'],
                                        start_time=start_date, end_time=end_date, callback=on_progress)
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f"{sector_name} | Financial Data | {start_date}-{end_date} | Download End: "
            f"Cost Time ={end_time - start_time} ")

    except Exception as e:
        logger_datacube.error(f"Error download history Financial Data!,{e}")


def download_history_data(end_date=''):
    """
    :param end_date: YYYYMMDD
    :return:
    """
    # download_period_price_data(sector_name='沪深A股', period='1d', start_date='20100101', end_date=end_date)
    # download_period_price_data(sector_name='沪深A股', period='1m', start_date='20150101', end_date=end_date)
    # download_period_price_data(sector_name='沪深A股', period='5m', start_date='20150101', end_date=end_date)
    # download_period_price_data(sector_name='沪深A股', period='tick', start_date='20150101', end_date=end_date)
    download_period_financial_data(sector_name='沪深A股', start_date='20100101', end_date='')


def download_daily_data(start_date: str, end_date: str):
    """
    :param start_date: YYYYmmdd
    :param end_date: YYYYmmdd
    :return:
    """
    download_period_price_data(sector_name='沪深A股', period='1d', start_date=start_date, end_date=end_date)
    download_period_price_data(sector_name='沪深A股', period='1m', start_date=start_date, end_date=end_date)
    # download_period_price_data(sector_name='沪深A股', period='5m', start_date=start_date, end_date=end_date)
    # download_period_price_data(sector_name='沪深A股', period='tick', start_date=start_date, end_date=end_date)
    download_period_financial_data(sector_name='沪深A股', start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    download_daily_data(today_str, today_str)
