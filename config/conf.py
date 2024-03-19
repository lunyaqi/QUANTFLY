import os
from datetime import datetime,timedelta
today = datetime.today()
today_str = today.strftime("%Y%m%d")
today_int = int(today.strftime('%Y%m%d'))
today_format = today.strftime('%Y-%m-%d')
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y%m%d")
yesterday_int = int(yesterday.strftime('%Y%m%d'))
yesterday_format = yesterday.strftime('%Y-%m-%d')

POSTGRES_CONFIG = {
    'host': '127.0.0.1',
    'port': 5432,
    'user': 'postgres',
    'password': 'lunyaqi',
    'database': 'postgres'
}
PARENT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BASE_LOG_PATH = os.path.join(PARENT_PATH,f'logs')

if not os.path.exists(BASE_LOG_PATH):
    os.makedirs(BASE_LOG_PATH)


# 定义文件路径
DATA_LOG_PATH = os.path.join(BASE_LOG_PATH, 'data.log')

# 如果文件不存在，创建文件并写入初始内容
if not os.path.exists(DATA_LOG_PATH):
    with open(DATA_LOG_PATH, 'w') as file:
        file.write('Initial content for data.log\n')

# 定义文件路径
FACTOR_LOG_PATH = os.path.join(BASE_LOG_PATH, 'factor.log')

# 如果文件不存在，创建文件并写入初始内容
if not os.path.exists(FACTOR_LOG_PATH):
    with open(FACTOR_LOG_PATH, 'w') as file:
        file.write('Initial content for factor.log\n')


