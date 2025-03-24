import psycopg2
# import urllib.parse

# Database connection parameters
DB_PARAMS = {
    'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
    'port': '5434',
    'dbname': 'KIESQUAREDE',
    'user': 'KIESQUAREDE',
    'password': 'KIESQUARE123'
}

# DB_USER = 'KIESQUAREDE'
# DB_PASS = urllib.parse.quote_plus('KIESQUARE123')  # URL encode the password
# DB_HOST = 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com'
# DB_PORT = '5434'
# DB_NAME = 'KIESQUAREDE'
# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

conn = psycopg2.connect(**DB_PARAMS)