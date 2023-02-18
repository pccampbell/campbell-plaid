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
from datetime import date
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


# Fill in your Plaid API keys - https://dashboard.plaid.com/account/keys
PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'development')
PLAID_PRODUCTS = os.getenv('PLAID_PRODUCTS', 'transactions').split(',')
PLAID_COUNTRY_CODES = os.getenv('PLAID_COUNTRY_CODES', 'US').split(',')

CLIENT_NAME = "CampbellTechInc"

def empty_to_none(field):
    value = os.getenv(field)
    if value is None or len(value) == 0:
        return None
    return value

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

products = []
for product in PLAID_PRODUCTS:
    products.append(Products(product))




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=date.fromisoformat('2017-09-14'),
        end_date=date.fromisoformat('2023-02-07'),
        options=TransactionsGetRequestOptions()
    )
    response = client.transactions_get(request)
    transactions = response['transactions']
    trans1 = transactions[0]
    while len(transactions) < response['total_transactions']:
        request = TransactionsGetRequest(
            access_token=access_token,
            start_date=date.fromisoformat('2017-09-14'),
            end_date=date.fromisoformat('2023-02-07'),
            options=TransactionsGetRequestOptions(
                offset=len(transactions)
            )
        )
        response = client.transactions_get(request)
        transactions.extend(response['transactions'])
    all_json = []
    dict_list = []
    for i in range(len(transactions)):
        dict = transactions[i].to_dict()
        dict_list.append(dict)
        # js = json.dumps(dict, indent=4, sort_keys=True, default=str)
        # js_load = json.loads(js)
        # all_json.append(js_load)
        # test = json.dumps(dict_list, indent=4, sort_keys=True, default=str)
        print('test')

    test = json.dumps(dict_list, indent=4, sort_keys=True, default=str)
    json_string = json.dumps(transactions, indent=4, sort_keys=True, default=str)
    df = pd.read_json(test)
    df.to_csv('test_NEW.csv', index=False)
    print('You win')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
