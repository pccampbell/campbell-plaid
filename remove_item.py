from plaid.api import plaid_api
from plaid.model.item_remove_request import ItemRemoveRequest
import plaid
import os

from dotenv import load_dotenv
load_dotenv()

# Fill in your Plaid API keys - https://dashboard.plaid.com/account/keys
PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'production')
PLAID_PRODUCTS = os.getenv('PLAID_PRODUCTS', 'transactions').split(',')
PLAID_COUNTRY_CODES = os.getenv('PLAID_COUNTRY_CODES', 'US').split(',')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
PG_PASSWORD = os.getenv('PG_PASSWORD')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
CLIENT_NAME = "CampbellTechInc"

# Assuming you have a Plaid client instance
# client = plaid_api.PlaidApi(...)  # Replace with your actual client initialization

def empty_to_none(field):
    value = os.getenv(field)
    if value is None or len(value) == 0:
        return None
    return value


PLAID_REDIRECT_URI = empty_to_none('PLAID_REDIRECT_URI')
if PLAID_ENV == 'sandbox':
    host = plaid.Environment.Sandbox
elif PLAID_ENV == 'production':
    host = plaid.Environment.Production

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

# Replace with the access token of the item you want to remove
access_token = "access-production-379a82bd-32bc-45bb-8252-408da949581b"

try:
    request = ItemRemoveRequest(access_token=access_token)
    response = client.item_remove(request)
    print(response) # Check the response for confirmation
except Exception as e:
    print(f"Error removing item: {e}")