from google.cloud import bigquery
from google.oauth2 import service_account

credentials_path = "/mnt/c/Temp/desafiolh-445818-3cb0f62cb9ef.json"
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project="desafioadventureworks-446600", location="us-central1")

query = "SELECT * FROM `raw_data.humanresources_employee` LIMIT 10"
results = client.query(query).to_dataframe()
print(results)
