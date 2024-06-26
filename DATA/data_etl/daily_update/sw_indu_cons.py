import datetime
import json
import time

import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_to_datetime

'''
申万行业成分股
数据来源：akshare
'''

def crawler_sw_indu_cons(index_code):
    'https://www.swsresearch.com/institute-sw/api/index_publish/details/component_stocks/?swindexcode=801010&page=1&page_size=1000'
    url = f'https://www.swsresearch.com/institute-sw/api/index_publish/details/component_stocks/?swindexcode={index_code}&page=1&page_size=1000'
    # headers = {
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    # }
    # r = requests.get(url, headers=headers)
    # data_json = r.json()
    # df = pd.DataFrame(data_json["data"]["results"])
    driver = webdriver.Edge()  # 替换为你的 IEDriverServer 的路径
    driver.get(url)
    html_content = driver.page_source
    driver.quit()
    # 使用BeautifulSoup解析HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    pre = soup.find_all('pre')
    text = pre[1].text
    # 从文本内容中找到JSON字符串的开始和结束位置
    start = text.find('{')
    end = text.rfind('}') + 1
    # 提取JSON字符串并解析为Python字典
    data_json = json.loads(text[start:end])
    # 从字典中获取'data'字段
    data = data_json['data']['results']
    df = pd.DataFrame(data)

    df = df.rename(columns={'stockcode': 'ticker', 'stockname': 'ticker_name', 'newweight': 'weight',
                            'beginningdate': 'begin_date'})
    df["begin_date"] = pd.to_datetime(df["begin_date"], errors="coerce").dt.date
    time.sleep(1)
    return df


def crawler_sw_indu_codes(index_type):
    """
    实时行情，用来取代码
    'https://www.swsresearch.com/institute-sw/api/index_publish/current/?page=1&page_size=10&indextype=%E4%B8%80%E7%BA%A7%E8%A1%8C%E4%B8%9A&sortField=&rule='

    :param index_type:
    :return:
    """
    url = 'https://www.swsresearch.com/institute-sw/api/index_publish/current/?page=1&page_size=1000'
    type_dict = {'一级行业': '%E4%B8%80%E7%BA%A7%E8%A1%8C%E4%B8%9A',
                 '二级行业': '%E4%BA%8C%E7%BA%A7%E8%A1%8C%E4%B8%9A'}
    where_cond = "&indextype=%s&sortField=&rule=" % (
        type_dict[index_type])
    url = url + where_cond
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    r = requests.get(url, headers=headers)
    data = r.json()
    df = pd.DataFrame(data['data']['results'])
    index_codes = df['swindexcode'].to_list()
    index_names = df['swindexname'].to_list()
    return index_codes, index_names


def exstract_sw_indu_cons_daily(start_date, end_date):
    start_time = datetime.datetime.now()
    start_date = convert_to_datetime(start_date).date()
    end_date = convert_to_datetime(end_date).date()

    try:
        sw1_codes, sw1_names = crawler_sw_indu_codes('一级行业')
        sw2_codes, sw2_names = crawler_sw_indu_codes('二级行业')
        codes_all = sw1_codes + sw2_codes
        names_all = sw1_names + sw2_names
        dfs = []
        for indu_code in tqdm(codes_all):
            print(indu_code)
            df = crawler_sw_indu_cons(indu_code)
            df['indu_name'] = names_all[codes_all.index(indu_code)]
            df['indu_type'] = 1 if indu_code in sw1_codes else 2
            dfs.append(df)
        sw_indu_cons = pd.concat(dfs, ignore_index=True)

        total_sw_indu_cons = sw_indu_cons[
            (sw_indu_cons['begin_date'] <= end_date) & (sw_indu_cons['begin_date'] >= start_date)]
        if total_sw_indu_cons.empty:
            logger_datacube.warning(
                f'[DAILY] sw_indu_cons:{start_date}-{end_date},no data!')

        insert_df_to_postgres(total_sw_indu_cons, table_name='sw_indu_cons')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert sw_indu_cons,{start_date}-{end_date},cost time: {end_time - start_time},lens={len(total_sw_indu_cons)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] sw_indu_cons:{err}')


if __name__ == '__main__':
    exstract_sw_indu_cons_daily(start_date='2024-01-01', end_date='2024-04-15')
