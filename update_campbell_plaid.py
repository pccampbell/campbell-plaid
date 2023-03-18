# from plaid.model.recipient_bacs_nullable import RecipientBACSNullable
# from plaid.model.payment_initiation_address import PaymentInitiationAddress
# from plaid.model.payment_initiation_recipient_create_request import PaymentInitiationRecipientCreateRequest
# from plaid.model.payment_initiation_payment_create_request import PaymentInitiationPaymentCreateRequest
# from plaid.model.payment_initiation_payment_get_request import PaymentInitiationPaymentGetRequest
# from plaid.model.link_token_create_request_payment_initiation import LinkTokenCreateRequestPaymentInitiation
# from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
# from plaid.model.asset_report_create_request import AssetReportCreateRequest
# from plaid.model.asset_report_create_request_options import AssetReportCreateRequestOptions
# from plaid.model.asset_report_user import AssetReportUser
# from plaid.model.asset_report_get_request import AssetReportGetRequest
# from plaid.model.asset_report_pdf_get_request import AssetReportPDFGetRequest
# from plaid.model.auth_get_request import AuthGetRequest
# from plaid.model.identity_get_request import IdentityGetRequest
# from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
# from plaid.model.item_get_request import ItemGetRequest
# from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
# from plaid.model.transfer_authorization_create_request import TransferAuthorizationCreateRequest
# from plaid.model.transfer_create_request import TransferCreateRequest
# from plaid.model.transfer_get_request import TransferGetRequest
# from plaid.model.transfer_network import TransferNetwork
# from plaid.model.transfer_type import TransferType
# from plaid.model.transfer_user_in_request import TransferUserInRequest
# from plaid.model.ach_class import ACHClass
# from plaid.model.transfer_create_idempotency_key import TransferCreateIdempotencyKey
# from plaid.model.transfer_user_address_in_request import TransferUserAddressInRequest
import plaid
from plaid.exceptions import ApiException
from plaid.model.payment_amount import PaymentAmount
from plaid.model.payment_amount_currency import PaymentAmountCurrency
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.api import plaid_api
from datetime import datetime
from datetime import timedelta
import base64
import os
import datetime
import json
import time
import os
from datetime import date,timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import types
import requests
from dotenv import load_dotenv
load_dotenv()


# Fill in your Plaid API keys - https://dashboard.plaid.com/account/keys
PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'development')
PLAID_PRODUCTS = os.getenv('PLAID_PRODUCTS', 'transactions').split(',')
PLAID_COUNTRY_CODES = os.getenv('PLAID_COUNTRY_CODES', 'US').split(',')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
PG_PASSWORD = os.getenv('PG_PASSWORD')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
CLIENT_NAME = "CampbellTechInc"

TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
YESTERDAY_DS = YESTERDAY.strftime("%m/%d/%Y")

def empty_to_none(field):
    value = os.getenv(field)
    if value is None or len(value) == 0:
        return None
    return value

def pg_conn():
    conn = None
    try:
        conn_string = 'postgresql://postgres:%s@10.0.0.26/campbell_bank' % PG_PASSWORD
        db_engine = create_engine(conn_string)
        conn = db_engine.connect()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        send_slack_message('Oh No something went wrong!',str(error))
        exit()

    return conn

def get_last_run(conn):
    sql = "SELECT MAX(date) FROM campbell_bank.public.plaid_raw"

    try:
        result = conn.execute(text(sql))
        last_run_date = result.first()[0]
    except Exception as error:
        send_slack_message('Oh No something went wrong!', str(error))
        exit()
    return last_run_date


def get_current_balance(conn):
    sql = "select balance from campbell_bank.public.checking_account_core where transaction_date = " \
          "(select MAX(transaction_date) from campbell_bank.public.checking_account_core)"

    try:
        result = conn.execute(text(sql))
        curr_balance = result.first()[0]
    except Exception as error:
        send_slack_message('Oh No something went wrong!', str(error))
        exit()
    return curr_balance

def pull_campbell_plaid(last_date=None):
    if last_date == None:
        start_date = date.fromisoformat('2017-01-01')
    else:
        start_date = last_date + timedelta(days=1)

    PLAID_REDIRECT_URI = empty_to_none('PLAID_REDIRECT_URI')
    if PLAID_ENV == 'development':
        host = plaid.Environment.Development

    configuration = plaid.Configuration(
        host=host,
        api_key={
            'clientId': PLAID_CLIENT_ID,
            'secret': PLAID_SECRET,
            'plaidVersion': '2020-09-14'
        }
    )

    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    # products = []
    # for product in PLAID_PRODUCTS:
    #     products.append(Products(product))

    request = TransactionsGetRequest(
        access_token=ACCESS_TOKEN,
        start_date= start_date,  # date.fromisoformat(start_date),
        end_date=YESTERDAY, # date.fromisoformat(YESTERDAY),
        options=TransactionsGetRequestOptions(
            include_original_description=True,
            include_personal_finance_category=True
        )
    )
    response = client.transactions_get(request)
    transactions = response['transactions']

    while len(transactions) < response['total_transactions']:
        request = TransactionsGetRequest(
            access_token=ACCESS_TOKEN,
            start_date=start_date,
            end_date=YESTERDAY,
            options=TransactionsGetRequestOptions(
                include_original_description=True,
                include_personal_finance_category=True,
                offset=len(transactions)
            )
        )
        response = client.transactions_get(request)
        transactions.extend(response['transactions'])

    return transactions

def pg_load_plaid(transactions, conn):
    dict_list = []
    for i in range(len(transactions)):
        dict = transactions[i].to_dict()
        dict_list.append(dict)

    pretty_json = json.dumps(dict_list, indent=4, sort_keys=True, default=str)
    # json_string = json.dumps(transactions, indent=4, sort_keys=True, default=str)
    df = pd.read_json(pretty_json)
    # datats = df.dtypes
    # df.to_csv('test_ETL.csv', index=False)
    #### LOAD df into table(s)
    num_rows = df.to_sql('plaid_raw', con=conn, index=False, if_exists='append',
                         dtype={'location': types.JSON,
                                'payment_meta': types.JSON,
                                'personal_finance_category': types.JSON})
    conn.commit()
    # conn.close()
    print('ingested %s rows' % num_rows)
    return num_rows


def combine_tables(conn):
    drop_sql = """drop table if exists campbell_bank.public.checking_account_core"""
    try:
        conn.execute(text(drop_sql))
        conn.commit()
    except Exception as e:
        print(str(r))
        pass

    create_sql = """
                    create table campbell_bank.public.checking_account_core as
                    (
                    with historical as 
                    (
                    SELECT 'bNPrxrEOYdCOJn4akbZwhKak700gMDuDw64V7' as account_id 
                    , 'Checking' as account_type 
                    , amount 
                    , null::date as authorized_date
                    , balance
                    , category
                    , null::int as category_id
                    , posted_date as transaction_date
                    , null as transaction_location
                    , null as merchant_name
                    , description as transaction_name
                    , null as payment_channel
                    , null as payment_meta
                    , null::boolean as pending 
                    , null as personal_finance_category
                    , null as transaction_id
                    , null as transaction_type
                    from campbell_bank.public.uwcu_historical
                    where posted_date  <= '2021-02-07'
                    )
                    , plaid as 
                    (
                    select account_id
                    , 'Checking' as account_type 
                    , -1 * amount
                    , authorized_date
                    , null::numeric(8, 2) as balance
                    , category
                    , category_id
                    , date as transaction_date
                    , location as transaction_location
                    , merchant_name
                    , name as transaction_name
                    , payment_channel
                    , payment_meta
                    , pending 
                    , personal_finance_category
                    , transaction_id
                    , transaction_type 
                    from 
                    campbell_bank.public.plaid_raw
                    where account_id = 'bNPrxrEOYdCOJn4akbZwhKak700gMDuDw64V7' 
                    )
                    , combined as 
                    (
                    select * from historical
                    union all 
                    select * from plaid
                    order by transaction_date desc
                    )
                    select 	account_id 
                    , account_type
                    , amount 
                    , authorized_date
                    , case when balance is not null then balance 
                           else SUM(amount) over (order by transaction_date asc) END as balance
                    , category 
                    , category_id
                    , transaction_date 
                    , transaction_location 
                    , merchant_name 
                    , transaction_name 
                    , payment_channel 
                    , payment_meta 
                    , pending
                    , personal_finance_category 
                    , transaction_id 
                    , transaction_type 
                    from combined
                    order by transaction_date desc
                    )
                    """
    try:
        conn.execute(text(create_sql))
        print('Combined')
    except:
        return 'Oh no the table join of plaid and UWCU failed'
    conn.commit()
    return "Successfully combined UWCU and plaid"


def send_slack_message(header, text_1=' ', text_2=' ', text_3=' '):
    slack_blob = {
        "username": 'Update Bot',
        "channel": "#campbell-bank-testing",
        "text": "Plaid data has been pulled and tables updated",
        "icon_emoji": ":bank:",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": text_1
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": text_2
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": text_3
                }
            },
            {
                "type": "divider"
            }
        ]
    }

    i = 0
    while i < 5:
        response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(slack_blob))
        if response.status_code == 200:
            return
        elif response.status_code != 200 and i < 4:
            i += 1
            time.sleep(30)
            print('waiting')
        elif i == 4:
            raise Exception(response.status_code, response.text)

if __name__ == '__main__':
    error_message = 'Oh no there was an error updating campble plaid'
    header = 'Successful Campbell Plaid Pull'
    try:
        conn = pg_conn()
        last_run_date = get_last_run(conn)
        if last_run_date == YESTERDAY:
            print('Data already pulled up to yesterday, no new data to pull exiting...')
            exit()
        else:
            try:
                transactions = pull_campbell_plaid(last_run_date)
                load_result = pg_load_plaid(transactions, conn)
                if int(load_result) == 0:
                    plaid_message = 'No new data available from campbell-plaid'
                    comb_message = 'No merge needed'
                    balance = '{:,}'.format(get_current_balance(conn))
                    balance_message = f'Current Balance: :heavy_dollar_sign:{balance}'
                    send_slack_message(header, plaid_message, comb_message,balance_message)
                    exit()

                else:
                    plaid_message = f':tada: Data was pulled from campbell-plaid and {load_result} ' \
                                    f'rows were added to the plaid table'

            except Exception as error:
                send_slack_message(error_message, str(error))
                exit()

            comb_message = combine_tables(conn)
            balance = '{:,}'.format(get_current_balance(conn))
            balance_message = f'Current Balance: :heavy_dollar_sign:{balance}'
            send_slack_message(header, plaid_message, comb_message,balance_message)
            conn.close()
            print('done')
    except Exception as e:
        send_slack_message(error_message,str(e))
        exit()




