from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
import datetime
from google.oauth2 import service_account

# Set your Facebook Ads credentials
app_id = 'INSERT META APP ID HERE'
app_secret = 'INSERT META APP SECRET TOKEN HERE'
access_token = 'INSERT META APP ACCESS TOKEN HERE'
ad_account_id = 'INSERT YOUR META ADS ACCOUNT ID HERE'

# Set your Google BigQuery information
project_id = 'INSERT YOUR GOOGLE BIGQUERY PROJECT ID HERE'
dataset_id = 'INSERT YOUR GOOGLE BIGQUERY DATASET ID HERE'
table_id = 'INSERT YOUR GOOGLE BIGQUERY TABLE ID HERE'

# Initialize the Facebook Ads API
FacebookAdsApi.init(app_id, app_secret, access_token)

# Create an empty DataFrame
data = []

# Create an AdAccount object
ad_account = AdAccount(ad_account_id)

# Define the fields you want to retrieve for campaigns
campaign_fields = [
    'name',
    'id'
]

# Define the parameters for your campaign request
campaign_params = {
    'limit': 10,
}

# Make the API request to get campaigns
campaigns = ad_account.get_campaigns(fields=campaign_fields, params=campaign_params)

# Get today's date in the format 'YYYY-MM-DD'
today_date = datetime.date.today().strftime('%Y-%m-%d')

# Loop through the campaigns and retrieve insights for each campaign
for campaign in campaigns:
    campaign_id = campaign['id']

    # Define the fields you want to retrieve for insights
    insight_fields = [
        'campaign_name',
        'spend',
        'cpc',
        'actions',
        'action_values',
        'date_start',
        'date_stop'
    ]

    # Define the parameters for your insights request
    insight_params = {
        'time_range': {'since': today_date, 'until': today_date},
        'level': 'campaign',
        'filtering': [{'field': 'campaign.id', 'operator': 'EQUAL', 'value': campaign_id}],
        'limit': 10,
    }

    # Make the API request to get insights for the current campaign
    insights = ad_account.get_insights(fields=insight_fields, params=insight_params)

    # Check if the campaign falls within the specified date range
    for insight in insights:
        if insight['date_start'] == today_date and insight['date_stop'] == today_date:
            row = {
                'Campaign Name': insight['campaign_name'],
                'Date Range': f"{insight['date_start']} - {insight['date_stop']}",
                'Spend (USD)': insight['spend'],
                'CPC (USD)': insight.get('cpc', 'N/A'),
                'Conversions': next((action['value'] for action in insight.get('actions', []) if action['action_type'] == 'omni_purchase'), 'N/A'),
                'Purchase Value (USD)': next((action['value'] for action in insight.get('action_values', []) if action['action_type'] == 'purchase'), 'N/A')
            }
            data.append(row)

# Create a DataFrame from the collected data
df = pd.DataFrame(data)

# Group the DataFrame by 'Date Range' and 'Campaign Name'
grouped_df = df.groupby(['Date Range', 'Campaign Name']).sum().reset_index()

# Print the grouped DataFrame
print(grouped_df)

# Rename the columns to have valid names
grouped_df.columns = grouped_df.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

# Upload data to Google BigQuery
credentials = service_account.Credentials.from_service_account_file('creds.json')
project_id = 'page-speed-reporting-ace-link'
destination_table_id = f'{project_id}.{dataset_id}.{table_id}'

grouped_df.to_gbq(destination_table=destination_table_id, if_exists='append', credentials=credentials)

print("Data uploaded to BigQuery successfully.")
