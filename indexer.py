import requests
import codecs
import json
import sys
import gzip
import bz2

import pandas as pd

try:
    elasticUrl = sys.argv[1]
    indexName = sys.argv[2]
    inFilePath = sys.argv[3]
except:
    print("Usage: indexer.py <http://localhost:PORT> <index_name> <firewall-log.csv>")
    sys.exit(-1)

# What type of object is this?
dataType = "_doc"

inFileDf = pd.read_csv(inFilePath)
inFileDf.columns = [x.replace(" ", "_").replace("/", "_").lower() for x in inFileDf.columns]

batch_size = 1000
bulk_post_data = []

def push_bulk_request(bulk_post_data):
    # Create the bulk request
    local_json_data = "\n".join(["\n".join(pair) for pair in bulk_post_data]) + "\n"

    # make bulk request 
    targetUrl = "{0}/_bulk".format(elasticUrl)
    res = requests.post(targetUrl, data=local_json_data, headers={"Content-Type": "application/x-ndjson"})
    print("Result:", res.status_code, res.text)

# For each row in our dataframe, convert it to a dictionary
#. for that specific row
for record in inFileDf.to_dict('records'):

    for k,v in record.items():
        if ( v == "(empty)" ):
            record[k] = None

    # Build the index URL
    targetUrl = "{0}/{1}/{2}".\
        format(elasticUrl, indexName, dataType)

    op_json = json.dumps({ "index" : { "_index" : indexName, "_type" : dataType } })
    bulk_post_data.append((op_json, json.dumps(record)))

    if ( len(bulk_post_data) == batch_size ):
        print("Posting %d records..." % batch_size )
        
        push_bulk_request(bulk_post_data)

        # Reset bulk data
        bulk_post_data = []


if ( len(bulk_post_data) > 0 ):
    push_bulk_request(bulk_post_data)
