from datetime import date
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()

PG_PASSWORD = os.getenv('PG_PASSWORD')

df = pd.read_csv(r"C:\Users\PeterCampbell\environments\bank\campbell-plaid\UWCU_Historical.csv")
df = df.rename(columns={'AccountNumber': 'account_number', 'AccountType': 'account_type', 'Posted Date': 'posted_date',
                        'Amount': 'amount', 'Description': 'description', 'Check Number': 'check_number',
                        'Category': 'category', 'Balance': 'balance', 'Note': 'note'})

conn_string = 'postgresql://postgres:%s@10.0.0.26/campbell_bank' % PG_PASSWORD

db_engine = create_engine(conn_string)
db_engine = db_engine.execution_options(autocommit=True)
conn = db_engine.connect()
conn.autocommit = True

df.to_sql('uwcu_historical',con=conn,index=False, if_exists='append')
conn.commit()
conn.close()
print('done')