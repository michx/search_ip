import csv,sys, json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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
parser.add_argument("-I","--input", help="Location of csv input file in which each rows contains ip address, start date, end date.")
args = parser.parse_args()

def execute_query(ip_address, start_date, end_date):
        heads={'X-Arbux-APIToken':args.token , 'Accept' : 'text/csv'}
        max_num_lines=10000
        server=args.deployment

        csv_filename=ip_address+'_'+start_date+'-'+end_date+'.csv'
        url='{"start":"'+start_date+'","end":"'+end_date+'","dimensions":["timestamp","IP_Protocol","Source_IPv4_Address","Source_IPv6_Address","Source_Port","Destination_IPv4_Address","Destination_IPv6_Address","Destination_Port","Bytes_Sent","Packets_Sent"],"filters":{"type":"selector","facet":"IPv4_Address","value":"'+ip_address+'"},"limit":'+str(max_num_lines)+',"view":"Network","view_values":[]}'
        try:
                req=requests.get('https://'+server+'/api/sp/insight/rawflows?json='+urllib.quote_plus(url), headers=heads, verify=False, timeout = 300)
        except requests.Timeout as t:
                #print "Timeout error: "+ip_address+","+start_date+","+end_date
                return "TIMEOUT"
                #exit()
        except Exception as e:
                #print e
                #print "Something went wrong connecting to the server"
                #exit()
                return "KO"

        destination_file=open(csv_filename,"w")
        destination_file.write(req.text)
        return "OK"
        #print "Successfully written data to file : ",csv_filename




ip_data = pd.read_csv(args.input, header=None)

for index, row in ip_data.iterrows():
    end_date=row[2]
    start_date=row[1]
    ip_address=row[0]
    #print ip_address, start_date, end_date
    date_pre = datetime.now()
    status = execute_query(row[0], row[1], row[2])
    date_post = datetime.now()
    print row[0]+','+row[1]+','+row[2]+','+str(date_pre)+','+str(date_post)+','+status
    
exit()

if args.end:
    end_date=args.end
else:
    end_date=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

if args.start:
    start_date=args.start
else:
    start_date=(datetime.now()- timedelta(hours=3, minutes=0)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

ip_address=args.IP
