import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import  insert_df_to_postgres


def extract_stock_info_history():
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        info_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            ashare_info_ori = xtdata.get_instrument_detail(ticker, iscomplete=True)
            ashare_info_ori_df = pd.DataFrame(ashare_info_ori.values()).T
            ashare_info_ori_df.columns = list(ashare_info_ori.keys())
            ashare_info_ori_df['ticker'] = ticker
            info_df = pd.concat([info_df, ashare_info_ori_df], axis=0)
        info_df = info_df.rename(columns={'ExchangeID': 'exchange_id',
                                          'InstrumentID': 'instrument_id',
                                          'InstrumentName': 'instrument_name',
                                          'Abbreviation': 'abbreviation',
                                          'ProductID': 'product_id',
                                          'ProductName': 'product_name',
                                          'CreateDate': 'create_date',
                                          'OpenDate': 'open_date',
                                          'ExpireDate': 'expire_date',
                                          'PreClose': 'pre_close',
                                          'SettlementPrice': 'settlement_price',
                                          'UpStopPrice': 'up_stop_price',
                                          'DownStopPrice': 'down_stop_price',
                                          'FloatVolume': 'float_volume',
                                          'TotalVolume': 'total_volume',
                                          'AccumulatedInterest': 'accumulated_interest',
                                          'LongMarginRatio': 'long_margin_ratio',
                                          'ShortMarginRatio': 'short_margin_ratio',
                                          'PriceTick': 'price_tick',
                                          'VolumeMultiple': 'volume_multiple',
                                          'MainContract': 'main_contract',
                                          'MaxMarketOrderVolume': 'max_market_order_volume',
                                          'MinMarketOrderVolume': 'min_market_order_volume',
                                          'MaxLimitOrderVolume': 'max_limit_order_volume',
                                          'MinLimitOrderVolume': 'min_limit_order_volume',
                                          'MaxMarginSideAlgorithm': 'max_margin_side_algorithm',
                                          'DayCountFromIPO': 'day_count_from_ipo',
                                          'LastVolume': 'last_volume',
                                          'InstrumentStatus': 'instrument_status',
                                          'IsTrading': 'is_trading',
                                          'IsRecent': 'is_recent',
                                          'bNotProfitable': 'b_not_profitable',
                                          'bDualClass': 'b_dual_class',
                                          'secuCategory': 'secu_category',
                                          'secuAttri': 'secu_attri',
                                          'OptUnit': 'opt_unit',
                                          'MarginUnit': 'margin_unit',
                                          'OptUndlCode': 'opt_undl_code',
                                          'OptUndlMarket': 'opt_undl_market',
                                          'OptLotSize': 'opt_lot_size',
                                          'OptExercisePrice': 'opt_exercise_price',
                                          'NeeqExeType': 'neeq_exe_type',
                                          'OptExchFixedMargin': 'opt_exch_fixed_margin',
                                          'OptExchMiniMargin': 'opt_exch_mini_margin',
                                          'Ccy': 'ccy',
                                          'IbSecType': 'ib_sec_type',
                                          'OptUndlRiskFreeRate': 'opt_undl_risk_free_rate',
                                          'OptUndlHistoryRate': 'opt_undl_history_rate',
                                          'EndDelivDate': 'end_delivery_date',
                                          'RegisteredCapital': 'registered_capital',
                                          'MaxOrderPriceRange': 'max_order_price_range',
                                          'MinOrderPriceRange': 'min_order_price_range',
                                          'VoteRightRatio': 'vote_right_ratio',
                                          'MaxMarketSellOrderVolume': 'max_market_sell_order_volume',
                                          'MinMarketSellOrderVolume': 'min_market_sell_order_volume',
                                          'MaxLimitSellOrderVolume': 'max_limit_sell_order_volume',
                                          'MinLimitSellOrderVolume': 'min_limit_sell_order_volume',
                                          'MaxFixedBuyOrderVol': 'max_fixed_buy_order_vol',
                                          'MinFixedBuyOrderVol': 'min_fixed_buy_order_vol',
                                          'MaxFixedSellOrderVol': 'max_fixed_sell_order_vol',
                                          'MinFixedSellOrderVol': 'min_fixed_sell_order_vol',
                                          'ProductTradeQuota': 'product_trade_quota',
                                          'ContractTradeQuota': 'contract_trade_quota',
                                          'ProductOpenInterestQuota': 'product_open_interest_quota',
                                          'ContractOpenInterestQuota': 'contract_open_interest_quota'
                                          })
        # 插入数据库
        del info_df['FloatVolumn'],
        del info_df['TotalVolumn'],

        insert_df_to_postgres(info_df, table_name='ashare_info')
    except Exception as err:
        logger_datacube.error(f'[ERROR]:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'Successfully inserted financial info history data,cost: {end_time - start_time}')


if __name__ == '__main__':
    extract_stock_info_history()
