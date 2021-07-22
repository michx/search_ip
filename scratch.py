import csv,sys, json
import requests
import pandas as pd
import urllib
from datetime import datetime, timedelta
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-d","--deployment",help="URL of Sightline deployment to query")
parser.add_argument("-t","--token",help="REST API token for authentication")
parser.add_argument("-i","--IP",help="IP address contained on searched flows as SRC and DST host")
parser.add_argument("-s","--start",help="Search starting date. If not specified this is the current date minus 3 hours. Format: YYYY-mm-ddTHH:mm:sss.000Z  Example: 2021-07-22T11:06:00.000Z ")
parser.add_argument("-e","--end",help="Search finish date. If not specified this is the current date.Format: YYYY-mm-ddTHH:mm:sss.000Z  Example: 2021-07-22T11:06:00.000Z ")
args = parser.parse_args()

if args.end:
    end_date=args.end
else:
    end_date=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

if args.start:
    start_date=args.start
else:
    start_date=(datetime.now()- timedelta(hours=3, minutes=0)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

ip_address=args.IP
heads={'X-Arbux-APIToken':args.token}
max_num_lines=1000
server=args.deployment

csv_filename=ip_address+'.csv'
url='{"start":"'+start_date+'","end":"'+end_date+'","dimensions":["timestamp","IP_Protocol","Source_IPv4_Address","Source_IPv6_Address","Source_Port","Destination_IPv4_Address","Destination_IPv6_Address","Destination_Port","Bytes_Sent","Packets_Sent"],"filters":{"type":"selector","facet":"IPv4_Address","value":"'+ip_address+'"},"limit":'+str(max_num_lines)+',"view":"Network","view_values":[]}'
try:
    req=requests.get('https://'+server+'/api/sp/insight/rawflows?json='+urllib.quote_plus(url), headers=heads)
except:
    print "Something went wrong connecting to the server"
    exit()

response_data=json.loads(req.text)

df = pd.read_json (json.dumps(response_data['response']))
df.to_csv (csv_filename, index = None)
print "Successfully written data to file : ",csv_filename
