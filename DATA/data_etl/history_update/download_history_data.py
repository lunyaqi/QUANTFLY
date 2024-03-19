import datetime
from Utils.logger import logger_datacube
from tqdm import tqdm
from xtquant import xtdata
from config.conf import today_str

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

def download_history_price_data(sector_name='沪深A股', period='1d', start_date='', end_date=None):
    '''
    下载历史K线数据，period支持"tick, 1m, 5m, 1d"
    '''
    if sector_name in sector_list:
        stock_list = xtdata.get_stock_list_in_sector(sector_name)
    else:
        logger_datacube.error(f'sector name 输入错误!参考: {sector_list}')
        return

    if len(stock_list) == 0:
        print("stock_list is empty, pass")
        return
    start_time = datetime.datetime.now()
    logger_datacube.info(f"{sector_name}| {period} | Download Begin: {start_time.strftime('%Y%m%d-%H:%M:%S')}")

    try:
        for stock in tqdm(stock_list):
            xtdata.download_history_data(stock, period=period, start_time=start_date, end_time=end_date,
                                          incrementally=True)
    except Exception as e:
        logger_datacube.error (f"Error download history!,{e}")
    end_time = datetime.datetime.now()
    logger_datacube.info(f"{sector_name}| {period} | Download End: {end_time.strftime('%Y%m%d-%H:%M:%S')} Cost Time ={end_time - start_time} ")


if __name__ == '__main__':
    # download_history_price_data(sector_name='沪深A股', period='1d', start_date='20100101', end_date='')
    download_history_price_data(sector_name='沪深A股', period='1m', start_date='20150101', end_date='')
    download_history_price_data(sector_name='沪深A股', period='5m', start_date='20150101', end_date='')
    download_history_price_data(sector_name='沪深A股', period='tick', start_date='20150101', end_date='')
