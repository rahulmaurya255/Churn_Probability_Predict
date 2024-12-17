

import os
from clickhouse_driver import Client
import pandas as pd

ckh_host = os.getenv('CKH_HOST')
ckh_user = os.getenv('CKH_USER')
ckh_password = os.getenv('CKH_PASSWORD')
ckh_port = os.getenv('CKH_PORT')
ckh_database = os.getenv('CKH_DB')
timezone=os.getenv("TIMEZONE")
time_period=os.getenv("TIMEPERIOD")
backfill_type=os.getenv('BACKFILL_TYPE')
start_date_str = os.getenv('START_DATE')
end_date_str = os.getenv("END_DATE")
kms_region=os.getenv("AWS_REGION")
kms_google_key = os.environ.get("KMS_GOOGLE_KEY")


aws_ce_region = os.getenv('AWS_CE_REGION')



#Clickhouse connection function
def connect_clickhouse():
    CKH_HOST = ckh_host
    CKH_PASSWORD = ckh_password
    CKH_USER = ckh_user
    client = Client(host=CKH_HOST , port=9000 , user=CKH_USER , password=CKH_PASSWORD )
    return client

def clickhouse_connection(query):
    client = connect_clickhouse()
    try:
        result = client.execute(query,with_column_types=True)
        columns = [col[0] for col in result[1]]  # Extracting column names
        data = result[0]  # Extracting data
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print("ClickHouse connection not made")
        print(e)
        return pd.DataFrame()
