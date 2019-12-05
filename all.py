#!/usr/local/bin/python2
import sys
import requests
import json
import argparse
import csv
from google.cloud import bigquery


ANOMALIES_QUERY = '''
{
  organization(id: 16770) {
  devices {
    id
    anomalies: objectStat(statTypeEnum: osDAnomalyEvent, duration: 15552000000) {
      changedAtMs
      protoValue {
        anomalyEvent {
          description
          bootCount
        }
      }
    }
  }
}
}'''

ANOMALIES_TABLE = 'total.anomalies'

ANOMALIES_OUTPUT =  '/tmp/' + 'ANOMALIES_output.csv'

ANOMALIES_OUTPUT = 'ANOMALIES_output.csv'


def parse_and_write_ag_anomalies(result,output):
    devices = result['organization']['devices']

    for d in devices:
      anomalies = d['anomalies']
   
      for c in anomalies:
        print(c)
        deviceId = d['id']
        changedAtMs = c['changedAtMs']
        description = c['protoValue']['anomalyEvent']['description']
        bootCount = c['protoValue']['anomalyEvent']['bootCount']

        output.writerow([deviceId,
        changedAtMs,
        description,
        bootCount
      ])





def do_bq(upload_file,t): 
    client = bigquery.Client()

    table = t
    table = client.get_table(table)  
    print('Updating Table: ',table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.skip_leading_rows = 0

    csv_file = open(upload_file,'rb')
    job = client.load_table_from_file(
        csv_file, table, job_config=job_config)  # API request
    job.result()  # Waits for table load to complete.




def do_gql_query(query):
  url = "https://api.samsara.com" 
  r = requests.post(
    url + "/v1/admin/graphql",
    headers={
      'X-Access-Token': 'cGj73vBjZUD4XqjTSeSZxk3pu25kxR'
    }, data=json.dumps({"query": query, "variables": {}})
  )
  return json.dumps(r.json(), indent=4)





def get_data(query,table, f,output):
  c = csv.writer(open(output, "w+"))

  print("Running GQL query\n ", query)
  curResult = do_gql_query(query)
  f(json.loads(curResult),c)



  print("finished with devices, writing to bigquery")
  do_bq(output,table)


def lambda_handler(event,context):
  get_data(ANOMALIES_QUERY, ANOMALIES_TABLE, parse_and_write_ag_anomalies,ANOMALIES_OUTPUT)


lambda_handler(None,None)
