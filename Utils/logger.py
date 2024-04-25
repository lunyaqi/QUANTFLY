# SQL DIR PATH
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from os.path import dirname, abspath
from Config.conf import DATA_LOG_PATH, FACTOR_LOG_PATH

# 创建一个格式器，设置日志格式
formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

parent_path = dirname(dirname(abspath(__file__)))

logger_datacube = logging.getLogger('datacube')
logger_datacube.setLevel(logging.INFO)  # 设置日志级别为INFO
filehandler_datacube = TimedRotatingFileHandler(DATA_LOG_PATH, 'D', 10, 100)
filehandler_datacube.setFormatter(formatter)  # 为处理器设置格式器
logger_datacube.addHandler(filehandler_datacube)
logger_datacube.suffix = "%Y%m%d.log"

# 添加一个StreamHandler，使得日志信息可以输出到控制台
stream_handler_datacube = logging.StreamHandler()
stream_handler_datacube.setFormatter(formatter)
logger_datacube.addHandler(stream_handler_datacube)

logger_factor = logging.getLogger('factor')
logger_factor.setLevel(logging.INFO)  # 设置日志级别为INFO
filehandler_factor = TimedRotatingFileHandler(FACTOR_LOG_PATH, 'D', 10, 100)
filehandler_factor.setFormatter(formatter)  # 为处理器设置格式器
logger_factor.addHandler(filehandler_factor)
logger_factor.suffix = "%Y%m%d.log"

# 添加一个StreamHandler，使得日志信息可以输出到控制台
stream_handler_factor = logging.StreamHandler()
stream_handler_factor.setFormatter(formatter)
logger_factor.addHandler(stream_handler_factor)