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
import time, socket
from datetime import timedelta
from dateutil.relativedelta import *
 
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

def pauseAndContinue(exceptionType, tries, e):
    sleepTime = tries * tries * 10
    print("Error of type '%s': %s. Trying again in %s seconds" % (exceptionType, e, sleepTime))
    time.sleep(sleepTime)
     
def execute(path, requestObj, username, secret, errorMsg = "error", maxTries = 5):
    response = None
    tries = 0
    while (tries < maxTries):
        tries += 1
        try:
            status, response = cx_api(path, requestObj, username, secret)
        except socket.gaierror as e:
            pauseAndContinue('socket.gaierror', tries, e)
            continue
        except TimeoutError as e:
            pauseAndContinue('TimeoutError', tries, e)
            continue
        except ConnectionAbortedError as e:
            pauseAndContinue('ConnectionAbortedError', tries, e)
            continue
        except ConnectionResetError as e:
            pauseAndContinue('ConnectionResetError', tries, e)
            continue
        except ResourceWarning as e:
            pauseAndContinue('ResourceWarning', tries, e)
            continue
        except Exception as e:
            raise Exception('Unhandled connection error: "%s"' % str(e))
         
        try:
            #errorHandling(status, response, errorMsg)
            print(status)
            break
        except Exception as e:
            errorText = None
            if status == 401:
                errorText = 'Request expired'
            elif status == 500:
                errorText = 'Error while processing request'   
            elif status == 503:
                errorText = 'Service Unavailable'
            if errorText and errorText in str(e):
                pauseAndContinue(errorText, tries, e)
                continue
            raise Exception(str(e))
    if not response:
        raise Exception(errorMsg)
    return status, response

def new_date(string):
    new_string = str(string)
    numbers = new_string[1:-1]
    if new_string == "now":
        return(string)
    elif new_string[-1:] == "d":
        d = datetime.datetime.today() - timedelta(days=int(numbers))
    elif new_string[-1:] == "w":
        d = datetime.datetime.today() - timedelta(weeks=int(numbers))
    elif new_string[-1:] == "M":
        d = datetime.datetime.today() - relativedelta(months=+int(numbers))
    elif new_string[-1:] == "y":
        d = datetime.datetime.today() - relativedelta(years=+int(numbers))
    else:
        return(string)

    day = d.strftime('%Y-%m-%d')
    return(day)

cfg = docker.Config('/data/')
configuration = cfg.get_parameters()

try:
    # site table
    site_table = configuration['site_table']
    outSiteFullName = '/data/out/tables/' + 'site' + '.csv'
    outDestinationSite = 'site'

    # traffic table
    traffic_table = configuration['traffic_table']
    trafficTableName = configuration['traffic_table_name']
    outTrafficFullName = '/data/out/tables/' + trafficTableName + '.csv'
    outDestinationTraffic = trafficTableName

    request_username = configuration['request_username']
    request_secret = configuration['#request_secret']
    traffic_request_stop = configuration['traffic_request_stop'] 
    traffic_request_start = configuration['traffic_request_start']
    traffic_request_historyResolution = configuration['traffic_request_history_resolution']
    main_traffic_groups_list = configuration['traffic_filters']
    traffic_request_groups = configuration['traffic_request_groups']
    traffic_request_method = configuration['traffic_request_method']
    site_ids_filter = configuration['site_ids_filter']
    user_ids = configuration['user_ids']
except:
    print("Please complete the missing part of the configuration")
    exit(1)

if __name__ == "__main__":
    username = request_username
    secret   = request_secret

#  --------------------------------------------------------------------------------------------------------------------------------
# SITE API CALL
    site_request = (execute("/site", {
                                      }, username, secret))
    #print(site_request)
    site_ids = [] #array of site_ids

    site_response_column_names = ['site_id', 'name', 'url', 'country']
    site_df_columns = []
    try:
        for site_col in site_response_column_names:
            site_df_columns.append(site_col)
    except:
        print("Invalid credentials")
        exit(1)

    site_df_list = []

    try:
        site_request[1]['sites']
    except:
        print("Invalid request_username or request_secret")
        exit(1)

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

    if site_table == "True":
        cfg.write_table_manifest(outSiteFullName, destination=outDestinationSite, primary_key=['site_id'], incremental=True)
        site_df.to_csv(path_or_buf=outSiteFullName) 

    if traffic_table == "True":
#  --------------------------------------------------------------------------------------------------------------------------------
# TRAFFIC EVENT API CALL (for name of groups items) 
        list_tables = []

        if isinstance(site_ids_filter, list) == True:
            main_site_ids = site_ids_filter
        if site_ids_filter == "False":
            main_site_ids = site_ids

        try:
            main_site_ids
        except:
            print("Invalid credentials")
            exit(1)

        for siteId in main_site_ids:     
            print("SITE ID", siteId)

            # traffic_request_stop
            if traffic_request_stop == "today":
                traffic_request_stop = datetime.datetime.today().strftime('%Y-%m-%d')
            else:
                traffic_request_stop = new_date(traffic_request_stop)
            # traffic_request_start
            traffic_request_start = new_date(traffic_request_start)

            traffic_event_request_template =  {
                                        "siteId" : siteId,  
                                        "stop": traffic_request_stop,
                                        "start": traffic_request_start,
                                        "historyResolution": traffic_request_historyResolution
                                        }

            if traffic_request_stop == "now":
                del traffic_event_request_template['stop']


            traffic_event_request = (execute("/traffic/event", traffic_event_request_template ,username, secret))

            traffic_event_group_item_dict = {}   # dict with all groups and items

            try:
                for event_group in traffic_event_request[1]['groups']:
                    traffic_event_key = event_group['group']
                    for event_item in event_group['items']:
                        traffic_event_value = event_item['item']
                        traffic_event_group_item_dict.setdefault(traffic_event_key, []).append(traffic_event_value)
            except:
                print("Invalid credentials")
                exit(1)

            print("traffic_event_group_item_dict: ", traffic_event_group_item_dict)
#  --------------------------------------------------------------------------------------------------------------------------------
# TRAFFIC API CALLs
            if user_ids == "False":
                # traffic event or traffic custom
                if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"):    

                    traffic_request_template = {"siteId" : siteId,  
                                                "stop": traffic_request_stop,
                                                "start": traffic_request_start,
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

                    if traffic_request_stop == "now":
                        del traffic_request_template['stop']

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

                    print("PRODUCTS,", main_traffic_items_list)
                    for combination in itertools.product(*main_traffic_items_list):
                        print("combination ,", combination)
                        filters = []

                        for i in range(len(combination)):
                            column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                            filters.append(column_filter)

                        traffic_request_template["filters"] = filters

                        resp = execute(traffic_request_method, traffic_request_template, username, secret)
                        
                        print(resp)
                        try:
                            dates = resp[1]['history']
                        except:
                            print("Invalid credentials")
                            exit(1)
                        #dates.pop(0)

                        #print("date range count,", len(range(len(dates) - 1)))
                        for j in range(len(dates) - 1):
                            print("groups count,", dates[j], len(resp[1]['groups']))
                            for group in resp[1]['groups']:
                                print("group items count,", group['group'], len(group['items']))
                                for item in group['items']:
                                    print("item,", item['item'])
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
                                    if combination:
                                        r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) + '-' + str(combination) 
                                    else:
                                        r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) 

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

                # -----------------------------------------------
                # traffic keyword
                if (traffic_request_method == "/traffic/keyword"):

                    traffic_request_template = {"siteId" : siteId,  
                                                "stop": traffic_request_stop,
                                                "start": traffic_request_start,
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

                    if traffic_request_stop == "now":
                        del traffic_request_template['stop']

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

                    print("PRODUCTS,", main_traffic_items_list)
                    for combination in itertools.product(*main_traffic_items_list):
                        print("combination ,", combination)
                        filters = []

                        for i in range(len(combination)):
                            column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                            filters.append(column_filter)

                        traffic_request_template["filters"] = filters

                        resp = execute(traffic_request_method, traffic_request_template, username, secret)

                        #print(resp)
                        try:
                            dates = resp[1]['history']
                        except:
                            print("Invalid credentials")
                            exit(1)
                        #dates.pop(0)

                        #print("date range count,", len(range(len(dates) - 1)))
                        for j in range(len(dates) - 1):
                            print("groups count,", dates[j], len(resp[1]['groups']))
                            for group in resp[1]['groups']:
                                print("group items count,", group['group'], len(group['items']))     
                                for item in group['items']:
                                    print("item,", item['item'])
                                    r_date = dates[j]
                                    r_group = group['group']
                                    r_item = item['item']
                                    r_events = item['historyData']['events'][j]
                                    r_urls = item['historyData']['urls'][j]
                                    r_weight = item['historyData']['weight'][j]
                                    r_site = siteId 
                                    r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) + '-' + str(combination)

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


#  --------------------------------------------------------------------------------------------------------------------------------
# TRAFFIC USER API CALL
            if user_ids == "True":

                traffic_user_request_template = {"siteId" : siteId,  
                                        "stop": traffic_request_stop,
                                        "start": traffic_request_start,
                                        "historyResolution": traffic_request_historyResolution
                                        }

                if traffic_request_stop == "now":
                    del traffic_user_request_template['stop']

                traffic_user_request = (execute("/traffic/user", traffic_user_request_template ,username, secret))

                traffic_user_dict = {}

                try:
                    for user_group in traffic_user_request[1]['groups']:
                        traffic_user_key = user_group['group']
                        for user_item in user_group['items']:
                            traffic_user_value = user_item['item']
                            traffic_user_dict.setdefault(traffic_user_key, []).append(traffic_user_value)
                except:
                    print("Invalid credentials")
                    exit(1)

                print("traffic_user_dict: ", traffic_user_dict)

#  --------------------------------------------------------------------------------------------------------------------------------
# TRAFFIC API CALLs + USERS
                # traffic event or traffic custom
                if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"):    

                    traffic_request_template = {"siteId" : siteId,  
                                                "stop": traffic_request_stop,
                                                "start": traffic_request_start,
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

                    if traffic_request_stop == "now":
                        del traffic_request_template['stop']

                    main_traffic_items_list = [] # list with items of chosen groups

                    #print("main_traffic_groups_list: ", main_traffic_groups_list)
                
                    for main_traffic_group in main_traffic_groups_list:
                        for traffic_event_item_key in traffic_event_group_item_dict:
                            if traffic_event_item_key == main_traffic_group:
                                main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key])

                    #print("main_traffic_items_list: ", main_traffic_items_list)

                    response_column_names = ['id', 'date', 'group', 'item', 'user_group', 'user_id', 'events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls', 'siteId']

                    df_columns = []
                    for col in main_traffic_groups_list:
                        df_columns.append(col)
                    for col in response_column_names:
                        df_columns.append(col)

                    df = pd.DataFrame(columns=df_columns)
                    row_index = 1

                    user_filters = []
                    for key, value in traffic_user_dict.items():
                        for v in value:
                            user_filter = {"type":"user", "group":key, "item":v}
                            user_filters.append(user_filter)
                    
                    for u_filter in user_filters:
                        
                        #print("PRODUCTS,", main_traffic_items_list)
                        for combination in itertools.product(*main_traffic_items_list):
                            print("combination ,", combination)
                            filters = []
                            filters.append(u_filter)

                            for i in range(len(combination)):
                                column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                                filters.append(column_filter)

                            print("filters: ", filters)

                            traffic_request_template["filters"] = filters

                            resp = execute(traffic_request_method, traffic_request_template, username, secret)

                            print(resp)

                            try:
                                dates = resp[1]['history']
                            except:
                                print("Invalid credentials")
                                exit(1)
                            #dates.pop(0)

                            #print("date range count,", len(range(len(dates) - 1)))
                            for j in range(len(dates) - 1):
                                print("groups count,", dates[j], len(resp[1]['groups']))
                                for group in resp[1]['groups']:
                                    print("group items count,", group['group'], len(group['items']))
                                    for item in group['items']:
                                        print("item,", item['item'])
                                        r_date = dates[j]
                                        r_group = group['group']
                                        r_item = item['item']
                                        r_user_group = u_filter['group']
                                        r_user_id = u_filter['item']
                                        r_events = item['historyData']['events'][j]
                                        r_urls = item['historyData']['urls'][j]
                                        r_sessionStarts = item['historyData']['sessionStarts'][j]
                                        r_sessionStops = item['historyData']['sessionStops'][j]
                                        r_sessionBounces = item['historyData']['sessionBounces'][j]
                                        r_activeTime = item['historyData']['activeTime'][j]
                                        r_uniqueUsers = item['historyData']['uniqueUsers'][j]
                                        r_site = siteId 
                                        if combination:
                                            r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) + '-' + str(r_user_id) + '-' + str(combination) 
                                        else:
                                            r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) + '-' + str(r_user_id)

                                        values = [r_id, r_date, r_group, r_item, r_user_group, r_user_id, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls, r_site]
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
                
                # -----------------------------------------------
                # traffic keyword
                if (traffic_request_method == "/traffic/keyword"):

                    traffic_request_template = {"siteId" : siteId,  
                                                "stop": traffic_request_stop,
                                                "start": traffic_request_start,
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

                    if traffic_request_stop == "now":
                        del traffic_request_template['stop']

                    main_traffic_items_list = [] # list with items of chosen groups

                    for main_traffic_group in main_traffic_groups_list:
                        for traffic_event_item_key in traffic_event_group_item_dict:
                            if traffic_event_item_key == main_traffic_group:
                                main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key]) 

                    response_column_names = ['id', 'date', 'group', 'user_group', 'user_id', 'item', 'events', 'urls', 'weight', 'siteId']

                    df_columns = []
                    for col in main_traffic_groups_list:
                        df_columns.append(col)
                    for col in response_column_names:
                        df_columns.append(col)

                    df = pd.DataFrame(columns=df_columns)
                    row_index = 1

                    user_filters = []
                    for key, value in traffic_user_dict.items():
                        for v in value:
                            user_filter = {"type":"user", "group":key, "item":v}
                            user_filters.append(user_filter)

                    for u_filter in user_filters:

                        print("PRODUCTS,", main_traffic_items_list)
                        for combination in itertools.product(*main_traffic_items_list):
                            print("combination ,", combination)
                            filters = []

                            for i in range(len(combination)):
                                column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                                filters.append(column_filter)

                            print("filters: ", filters)

                            traffic_request_template["filters"] = filters

                            resp = execute(traffic_request_method, traffic_request_template, username, secret)
                            print(resp)

                            try:
                                dates = resp[1]['history']
                            except:
                                print("Invalid credentials")
                                exit(1)
                            #dates.pop(0)

                            #print("date range count,", len(range(len(dates) - 1)))
                            for j in range(len(dates) - 1):
                                print("groups count,", dates[j], len(resp[1]['groups']))
                                for group in resp[1]['groups']:
                                    print("group items count,", group['group'], len(group['items']))     
                                    for item in group['items']:
                                        print("item,", item['item'])
                                        r_date = dates[j]
                                        r_group = group['group']
                                        r_item = item['item']
                                        r_user_group = u_filter['group']
                                        r_user_id = u_filter['item']
                                        r_events = item['historyData']['events'][j]
                                        r_urls = item['historyData']['urls'][j]
                                        r_weight = item['historyData']['weight'][j]
                                        r_site = siteId 
                                        r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) + '-' + str(r_user_id) + '-' + str(combination)

                                        values = [r_id, r_date, r_group, r_user_group, r_user_id, r_item, r_events, r_urls, r_weight, r_site]

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

#  -------------------------------------------------------------------------------------------------------------------------------- 
# # TRAFFIC OUT
        #print(list_tables)
        print("count tables", len(list_tables))
        traffic_tables = pd.concat(list_tables)
        cfg.write_table_manifest(outTrafficFullName, destination=outDestinationTraffic, primary_key=['id'], incremental=True)
        traffic_tables.to_csv(path_or_buf=outTrafficFullName) 

 