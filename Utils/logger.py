# SQL DIR PATH
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from os.path import dirname, abspath
from Config.conf import DATA_LOG_PATH, FACTOR_LOG_PATH

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [%(module)s] [%(funcName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO,
)
parent_path = dirname(dirname(abspath(__file__)))


logger_datacube = logging.getLogger('datacube')
filehandler_datacube = TimedRotatingFileHandler(DATA_LOG_PATH, 'D', 10, 100)
# %Y%m%d_%H%M%S
logger_datacube.addHandler(filehandler_datacube)
logger_datacube.suffix = "%Y%m%d.log"


logger_factor = logging.getLogger('factor')
filehandler_factor = TimedRotatingFileHandler(FACTOR_LOG_PATH, 'D', 10, 100)
logger_factor.suffix = "%Y%m%d.log"
# %Y%m%d_%H%M%S
logger_factor.addHandler(filehandler_factor)