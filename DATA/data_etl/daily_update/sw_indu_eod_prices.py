import datetime
import json
import math

import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_to_datetime

# def crawler_sw_indu_prices(
#         index_type: str = "市场表征",
#         start_date: str = "20221103",
#         end_date: str = "20221103",
# ) -> pd.DataFrame:
#     """
#     申万宏源研究-指数分析
#     https://www.swhyresearch.com/institute_sw/allIndex/analysisIndex
#     :param symbol: choice of {"市场表征", "一级行业", "二级行业", "风格指数"}
#     :type symbol: str
#     :param start_date: 开始日期
#     :type start_date: str
#     :param end_date: 结束日期
#     :type end_date: str
#     :return: 指数分析
#     :rtype: pandas.DataFrame
#     """
#     url = "https://www.swhyresearch.com/institute-sw/api/index_analysis/index_analysis_report/"
#     params = {
#         "page": "1",
#         "page_size": "200",
#         "index_type": index_type,
#         "start_date": "-".join([start_date[:4], start_date[4:6], start_date[6:]]),
#         "end_date": "-".join([end_date[:4], end_date[4:6], end_date[6:]]),
#         "type": "DAY",
#         "swindexcode": "all",
#     }
#     headers = {
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
#     }
#     r = requests.get(url, params=params, headers=headers)
#     data_json = r.json()
#     df = pd.DataFrame(data_json["data"]["results"])
#
#     df.rename(
#         columns={
#             "swindexcode": "indu_code",
#             "swindexname": "indu_name",
#             "bargaindate": "datetime",
#             "closeindex": "close",
#             "bargainamount": "volume",
#             "markup": "pct_change",
#             "turnoverrate": "turnover_rate",
#             "meanprice": "avg_price",
#             "bargainsumrate": "amount_rate",
#             "negotiablessharesum1": "float_market_value",
#             "negotiablessharesum2": "avg_float_market_value",
#         },
#         inplace=True,
#     )
#
#     return df


def crawler_index_hist_sw(index_type, start_date, end_date) -> pd.DataFrame:
    """
    申万宏源研究-指数发布-指数详情-指数历史数据
    http://www.swhyresearch.com/api/index_publish/history/?end_date=2024-04-17&index_code=all&index_type=%E4%B8%80%E7%BA%A7%E8%A1%8C%E4%B8%9A&page=2&page_size=10&rule=&sortField=&start_date=2024-04-17,
    :param symbol: 指数代码
    :type symbol: str
    :param period: choice of {"day", "week", "month"}
    :type period: str
    :return: 指数历史数据
    :rtype: pandas.DataFrame
    """
    url = 'https://www.swhyresearch.com/institute-sw/api/index_publish/history/?page=1&page_size=200'
    type_dict = {'一级行业': '%E4%B8%80%E7%BA%A7%E8%A1%8C%E4%B8%9A',
                 '二级行业': '%E4%BA%8C%E7%BA%A7%E8%A1%8C%E4%B8%9A'}
    where_cond = "&index_type=%s&start_date=%s&end_date=%s&index_code=all&sortField=&rule=" % (
        type_dict[index_type], start_date, end_date)
    url = url + where_cond
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    r = requests.get(url, headers=headers)
    data_json = r.json()
    df = pd.DataFrame(data_json["data"]["results"])
    # driver = webdriver.Edge()  # 替换为你的 IEDriverServer 的路径
    # driver.get(url)
    # html_content = driver.page_source
    # driver.quit()
    # # 使用BeautifulSoup解析HTML
    # soup = BeautifulSoup(html_content, 'html.parser')
    # pre = soup.find_all('pre')
    # text = pre[1].text
    # # 从文本内容中找到JSON字符串的开始和结束位置
    # start = text.find('{')
    # end = text.rfind('}') + 1
    # # 提取JSON字符串并解析为Python字典
    # data_json = json.loads(text[start:end])
    # # 从字典中获取'data'字段
    # data = data_json['data']['results']
    # df = pd.DataFrame(data)

    df.rename(
        columns={
            "swindexcode": "indu_code",
            "swindexname": "indu_name",
            "bargaindate": "datetime",
            "openindex": "open",
            "maxindex": "high",
            "minindex": "low",
            "closeindex": "close",
            "markup": "pct_change",
            "bargainamount": "volume",
            "bargainsum": "amount",
        },
        inplace=True,
    )

    return df


def extract_sw_indu_eod_prices_daily(start_date, end_date):
    start_time = datetime.datetime.now()
    try:
        sw1_df = crawler_index_hist_sw(index_type='一级行业', start_date=start_date, end_date=end_date)
        sw2_df = crawler_index_hist_sw(index_type='二级行业', start_date=start_date, end_date=end_date)
        sw_total = pd.concat([sw1_df, sw2_df], axis=0, ignore_index=True)
        sw_total['volume'] = (sw_total['volume'].astype(float) * 10000.0).astype('int64')
        sw_total['amount'] = (sw_total['amount'].astype(float) * 10000.0).astype('int64')
        sw_total['datetime'] = pd.to_datetime(sw_total['datetime'])
        sw_total['datetime'] = sw_total['datetime'].dt.tz_localize(None)
        if start_date==end_date:
            sw_total['datetime'] = convert_to_datetime(start_date)
        # 插入数据库
        insert_df_to_postgres(sw_total, table_name='sw_indu_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert sw_indu_eod_prices,{start_date}-{end_date},cost time: {end_time - start_time},lens={len(sw_total)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] sw_indu_eod_prices:{err}')


if __name__ == '__main__':

    # get_index_daily(index_code='801030', start_date='2024-04-17', end_date='2024-04-17')
    extract_sw_indu_eod_prices_daily(start_date='2024-04-18', end_date='2024-04-18')
