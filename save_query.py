#!/usr/bin/env python

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import argparse
import json
import logging

def parse_arguments():
    parser = argparse.ArgumentParser(description="Stream bigquery results to file")
    parser.add_argument('--query', nargs='+', action='store', type=str, dest='query_string', metavar='query_string', help='select blah where blah"')
    parser.add_argument('--output-file', action='store', type=str, dest='output_file', metavar='/path/to/save/json', help='Path to save rollup list into')
    parser.add_argument('--max_rows', action='store', type=int, dest='max_rows', metavar='max_rows', help='number of rows to accept')
    parser.add_argument('--project', required=True, action='store', type=str, dest='table_project', metavar='project', help='project table resides in')
    parser.add_argument('--service-account-json', type=str, action='store', dest='service_account_json', required=False, help="service account json")

    format_group = parser.add_mutually_exclusive_group(required=True)
    format_group.add_argument('--json', action='store_true', help='json format output')
    format_group.add_argument('--csv', action='store_true', help='csv format output')

    return parser.parse_args()

def initiate_bigquery_client(project, credentials=None):
    # print("Initializing google storage client...")
    try:
        if credentials:
            bq_client = bigquery.Client(project=project, credentials=credentials)
        else:
            bq_client = bigquery.Client(project=project)
        return bq
    except Exception as e:
        print("ERROR: Could not connect to bigquery API!: {}".format(e))

def main():
    args = parse_arguments()

    query_string = args.query_string
    output_file = args.output_file
    max_rows = args.max_rows
    project = args.table_project
    service_account_json = args.service_account_json
    # PROJECT = project
    
    if args.json:
        output_format = 'json'
    if args.csv:
        output_format = 'csv'

    if service_account_json:
        SERVICE_ACCOUNT = service_account_json
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        # Initialize the bq client
        bq = initiate_bigquery_client(project, credentials=credentials)
    else:
        # Initialize the bq client
        bq = initiate_bigquery_client(project)

    try:
        query_job = bq.query(query_string)
    except Exception:
        logging.error('BQ query failed!: {}'.format(Exception))

    # query_job.

    with open(output_file, 'w') as outfile:
        json.dump(query_job.result())


if __name__ == "__main__":
    main()
