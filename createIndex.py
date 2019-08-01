#!/usr/bin/python

import json
import sys
import requests

index_info = {
  "mappings": {
    "properties": {
      "source_ip": {
        "type": "ip"
      },
      "destination_ip": {
        "type": "ip"
      },
      "source_port": {
        "type": "integer"
      },
      "destination_port": {
        "type": "integer"
      },
      "date_time": {
        "type": "date",
        "format": "dd/MMM/yyyy HH:mm:ss"
      }
    }
  }
}

es_url = sys.argv[1]
index_name = sys.argv[2]

res = requests.put(es_url + "/" + index_name, data=json.dumps(index_info), headers={"Content-Type": "application/json"})
print("Result:", res.status_code, res.text)
