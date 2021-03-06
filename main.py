'''
Python 3
'''
  
import datetime
import hashlib
import hmac
import http.client
import json
import pandas as pd
import numpy as np
import itertools
import keboola
from keboola import docker
 
def cx_api(path, obj, username, secret):
    date = datetime.datetime.utcnow().isoformat() + "Z"
    signature = hmac.new(secret.encode('utf-8'), date.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
    headers = {"X-cXense-Authentication": "username=%s date=%s hmac-sha256-hex=%s" % (username, date, signature)}
    headers["Content-Type"] = "application/json; charset=utf-8"
    connection = http.client.HTTPSConnection("api.cxense.com", 443)
    connection.request("POST", path, json.dumps(obj), headers)
    response = connection.getresponse()
    status = response.status
    responseObj = json.loads(response.read().decode('utf-8'))
    connection.close()
    return status, responseObj


cfg = docker.Config('/data/')
configuration = cfg.get_parameters()

# site table
site_table = configuration['site_table']
outSiteFullName = '/data/out/tables/' + 'site' + '.csv'
outDestinationSite = 'ex_cxense_site'

# traffic table
traffic_table = configuration['traffic_table']
trafficTableName = configuration['traffic_table_name']
outTrafficFullName = '/data/out/tables/' + trafficTableName + '.csv'
outDestinationTraffic = 'ex_cxense_traffic'

request_username = configuration['request_username']
request_secret = configuration['#request_secret']
trtaffic_request_stop = configuration['trtaffic_request_stop'] 
trtaffic_request_start = configuration['trtaffic_request_start']
traffic_request_historyResolution = configuration['traffic_request_history_resolution']
main_traffic_groups_list = configuration['traffic_filters']
traffic_request_groups = configuration['traffic_request_groups']
traffic_request_method = configuration['traffic_request_method']

cfg.write_table_manifest(outSiteFullName, destination=outDestinationSite, primary_key=['site_id'], incremental=True)
cfg.write_table_manifest(outTrafficFullName, destination=outDestinationTraffic, primary_key=['id'], incremental=True)

if __name__ == "__main__":
    username = request_username
    secret   = request_secret

#  --------------------------------------------------------------------------------------------------------------------------------
    # SITE API CALL
    site_request = (cx_api("/site", {
                                      }, username, secret))

    site_ids = [] #array of site_ids

    site_response_column_names = ['site_id', 'name', 'url', 'country']
    site_df_columns = []
    for site_col in site_response_column_names:
        site_df_columns.append(site_col)

    site_df_list = []
    for site in site_request[1]['sites']:
        site_id = site['id']
        site_name = site['name']
        site_url = site['url']
        site_country = site['country']
        site_values = [site_id, site_name, site_url, site_country]
        site_df_list.append(site_values)

        site_ids.append(site_id)

    site_df = pd.DataFrame(site_df_list, columns=site_df_columns)
    site_df = site_df.set_index('site_id')
    
    out_site_table = pd.DataFrame.to_string(site_df)

    if site_table == "True":
        file = open(outSiteFullName,'w')  
        file.write(out_site_table)
        file.close()

    if traffic_table == "True":
#  --------------------------------------------------------------------------------------------------------------------------------
# TRAFFIC EVENT API CALL (for name of groups items) 
        list_tables = []
        for siteId in site_ids:     
            #print("SITE ID", siteId)
            traffic_event_request = (cx_api("/traffic/event", {
                                        "siteId" : siteId,  
                                        "stop": trtaffic_request_stop,
                                        "start": trtaffic_request_start,
                                        "historyResolution": traffic_request_historyResolution, 
                                        "groups":["deviceType",
                                                "mobileBrand",
                                                "browser", 
                                                "connectionSpeed", 
                                                "resolution",
                                                "colorDepth", 
                                                "exitLinkHost", 
                                                "exitLinkUrl",   
                                                "postalCode", 
                                                "city",   
                                                "url",  
                                                "referrerUrl", 
                                                "referrerHost", 
                                                "referrerHostClass", 
                                                "referrerSocialNetwork", 
                                                "referrerSearchEngine",
                                                "host"
                                                ]
                                        },username, secret))

            traffic_event_group_item_dict = {}   # dict with all groups and items

            for event_group in traffic_event_request[1]['groups']:
                traffic_event_key = event_group['group']
                for event_item in event_group['items']:
                    traffic_event_value = event_item['item']
                    traffic_event_group_item_dict.setdefault(traffic_event_key, []).append(traffic_event_value)
#  --------------------------------------------------------------------------------------------------------------------------------
# TRAFFIC API CALLs

            # traffic event or traffic custom
            if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"):
                traffic_request_template = {"siteId" : siteId,  
                                            "stop": trtaffic_request_stop,
                                            "start": trtaffic_request_start,
                                            "historyResolution": traffic_request_historyResolution, 
                                            "groups": traffic_request_groups,
                                            "fields":["events",
                                                    "sessionStarts",
                                                    "sessionStops", 
                                                    "sessionBounces", 
                                                    "activeTime", 
                                                    "uniqueUsers", 
                                                    "urls"
                                                    ], 
                                            "historyFields": ["events",
                                                            "sessionStarts",
                                                            "sessionStops", 
                                                            "sessionBounces",                                            
                                                            "activeTime", 
                                                            "uniqueUsers", 
                                                            "urls"
                                                            ]}

                main_traffic_items_list = [] # list with items of chosen groups

                for main_traffic_group in main_traffic_groups_list:
                    for traffic_event_item_key in traffic_event_group_item_dict:
                        if traffic_event_item_key == main_traffic_group:
                            main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key]) 

                response_column_names = ['id', 'date', 'group', 'item', 'events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls', 'siteId']

                df_columns = []
                for col in main_traffic_groups_list:
                    df_columns.append(col)
                for col in response_column_names:
                    df_columns.append(col)

                df = pd.DataFrame(columns=df_columns)
                row_index = 1

                #print("PRODUCTS,", main_traffic_items_list)
                for combination in itertools.product(*main_traffic_items_list):
                    #print("combination ,", combination)
                    filters = []

                    for i in range(len(combination)):
                        column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                        filters.append(column_filter)

                    traffic_request_template["filters"] = filters
                    resp = cx_api(traffic_request_method, traffic_request_template, username, secret)

                    dates = resp[1]['history']
                    #print("date range count,", len(range(len(dates) - 1)))
                    for j in range(len(dates) - 1):
                        #print("groups count,", dates[j], len(resp[1]['groups']))
                        for group in resp[1]['groups']:
                            #print("group items count,", group['group'], len(group['items']))
                            for item in group['items']:
                                #print("item,", item['item'])
                                r_date = dates[j]
                                r_group = group['group']
                                r_item = item['item']
                                r_events = item['historyData']['events'][j]
                                r_urls = item['historyData']['urls'][j]
                                r_sessionStarts = item['historyData']['sessionStarts'][j]
                                r_sessionStops = item['historyData']['sessionStops'][j]
                                r_sessionBounces = item['historyData']['sessionBounces'][j]
                                r_activeTime = item['historyData']['activeTime'][j]
                                r_uniqueUsers = item['historyData']['uniqueUsers'][j]
                                r_site = siteId 
                                r_id = str(r_date) + str(r_group) + str(r_item) + str(r_site)

                                values = [r_id, r_date, r_group, r_item, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls, r_site]
                                #print('%s %s %s %s %s %s %s %s %s %s\n' % (r_date, r_group, r_item, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls))

                                arr = []
                                for val in combination:
                                    arr.append(val)
                                for val in values:
                                    arr.append(val)
                    
                                df.loc[row_index] = arr
                                row_index += 1

                #print(df)
                traffic_tab = df.set_index('id')
                #print(traffic_table)
                list_tables.append(traffic_tab)



            # traffic keyword
            if traffic_request_method == "/traffic/keyword":
                traffic_request_template = {"siteId" : siteId,  
                                            "stop": trtaffic_request_stop,
                                            "start": trtaffic_request_start,
                                            "historyResolution": traffic_request_historyResolution, 
                                            "groups": traffic_request_groups,
                                            "fields":["events",
                                                        "urls",
                                                        "weight"
                                                    ], 
                                            "historyFields": ["events",
                                                                "urls",
                                                                "weight"
                                                            ]}

                main_traffic_items_list = [] # list with items of chosen groups

                for main_traffic_group in main_traffic_groups_list:
                    for traffic_event_item_key in traffic_event_group_item_dict:
                        if traffic_event_item_key == main_traffic_group:
                            main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key]) 

                response_column_names = ['id', 'date', 'group', 'item', 'events', 'urls', 'weight', 'siteId']

                df_columns = []
                for col in main_traffic_groups_list:
                    df_columns.append(col)
                for col in response_column_names:
                    df_columns.append(col)

                df = pd.DataFrame(columns=df_columns)
                row_index = 1

                #print("PRODUCTS,", main_traffic_items_list)
                for combination in itertools.product(*main_traffic_items_list):
                    #print("combination ,", combination)
                    filters = []

                    for i in range(len(combination)):
                        column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                        filters.append(column_filter)

                    traffic_request_template["filters"] = filters
                    resp = cx_api(traffic_request_method, traffic_request_template, username, secret)

                    dates = resp[1]['history']
                    #print("date range count,", len(range(len(dates) - 1)))
                    for j in range(len(dates) - 1):
                        #print("groups count,", dates[j], len(resp[1]['groups']))
                        for group in resp[1]['groups']:
                            #print("group items count,", group['group'], len(group['items']))
                            for item in group['items']:
                                #print("item,", item['item'])
                                r_date = dates[j]
                                r_group = group['group']
                                r_item = item['item']
                                r_events = item['historyData']['events'][j]
                                r_urls = item['historyData']['urls'][j]
                                r_weight = item['historyData']['weight'][j]
                                r_site = siteId 
                                r_id = str(r_date) + str(r_group) + str(r_item) + str(r_site)

                                values = [r_id, r_date, r_group, r_item, r_events, r_urls, r_weight, r_site]

                                arr = []
                                for val in combination:
                                    arr.append(val)
                                for val in values:
                                    arr.append(val)
                    
                                df.loc[row_index] = arr
                                row_index += 1

                #print(df)
                traffic_tab = df.set_index('id')
                #print(traffic_table)
                list_tables.append(traffic_tab)
   
        # TRAFFIC OUT
        #print("count tables", len(list_tables))
        traffic_tables = pd.concat(list_tables)
        #print("count traffic tables", len(traffic_tables))
        out_traffic_table = pd.DataFrame.to_string(traffic_tables)
        file = open(outTrafficFullName,'w')  
        file.write(out_traffic_table)
        file.close()