import os

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres'
}

BASE_LOG_PATH = f'/logs'
if not os.path.exists(BASE_LOG_PATH):
    os.makedirs(BASE_LOG_PATH)

DATA_LOG_PATH = os.path.join(BASE_LOG_PATH,'data.log')
if not os.path.exists(DATA_LOG_PATH):
    os.makedirs(DATA_LOG_PATH)

FACTOR_LOG_PATH = os.path.join(BASE_LOG_PATH,'factor.log')
if not os.path.exists(FACTOR_LOG_PATH):
    os.makedirs(FACTOR_LOG_PATH)



