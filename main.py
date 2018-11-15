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

# --------------------------------------
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

# --------------------------------------
def pauseAndContinue(exceptionType, tries, e):
    sleepTime = tries * tries * 10
    print("Error of type '%s': %s. Trying again in %s seconds" % (exceptionType, e, sleepTime))
    time.sleep(sleepTime)

# --------------------------------------     
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

# --------------------------------------
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

# --------------------------------------
# FULL REQUEST TEMPLATE

def full_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups):
    if isinstance(siteId, list) == False:
        siteId = [siteId]
    else:
        siteId = siteId

    request_template = {"siteIds" : siteId,  
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
    return(request_template)

# --------------------------------------
# SHORT REQUEST TEMPLATE

def short_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups):
    if isinstance(siteId, list) == False:
        siteId = [siteId]
    else:
        siteId = siteId

    request_template = {"siteIds" : siteId,  
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
    return(request_template)

# --------------------------------------
# DICTIONARY

def traffic_response_dict(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,execute,t_method):

    if isinstance(siteId, list) == False:
        siteId = [siteId]
    else:
        siteId = siteId

    traffic_template =  {
                        "siteIds" : siteId,  
                        "stop": traffic_request_stop,
                        "start": traffic_request_start,
                        "historyResolution": traffic_request_historyResolution
                        }

    if traffic_request_stop == "now":
        del traffic_template['stop']

    if t_method == "event":
        method = "/traffic/event"
    if t_method == "user":
        method = "/traffic/user"
    if t_method == "keyword":
        method = "/traffic/keyword"

    traffic_response = (execute(method, traffic_template ,username, secret))

    traffic_dict = {}   # dict with all groups and items

    try:
        for event_group in traffic_response[1]['groups']:
            traffic_event_key = event_group['group']
            for event_item in event_group['items']:
                traffic_event_value = event_item['item']
                traffic_dict.setdefault(traffic_event_key, []).append(traffic_event_value)
    except:
        print("Invalid credentials")
        exit(1)
    
    return(traffic_dict)

# --------------------------------------
# KEYWORD - EVENT - CUSTOM; WITHOUT USERS

def traffic_tab_without_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter):

    if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
        traffic_request_template = full_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups)
    if traffic_request_method == "/traffic/keyword":
        traffic_request_template = short_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups)

    if traffic_request_stop == "now":
        del traffic_request_template['stop']

    main_traffic_items_list = [] # list with items of chosen groups

    for main_traffic_group in main_traffic_groups_list:
        for traffic_event_item_key in traffic_event_group_item_dict:
            if traffic_event_item_key == main_traffic_group:
                main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key]) 

    if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
        response_column_names = ['id', 'date', 'group', 'item', 'events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls', 'siteId']
    if traffic_request_method == "/traffic/keyword":
        response_column_names = ['id', 'date', 'group', 'item', 'events', 'urls', 'weight']

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

        items_list = []
        for i_group in resp[1]['groups']:
            for i_item in i_group['items']:
                i_item = i_item['item']
                items_list.append(i_item)

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

                    if site_ids_filter == "All" and (request_for_set_of_sites == "True"):
                        r_site = "All-" + str(len(siteId))
                    else:
                        r_site = str(siteId) 

                    if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
                        r_sessionStarts = item['historyData']['sessionStarts'][j]
                        r_sessionStops = item['historyData']['sessionStops'][j]
                        r_sessionBounces = item['historyData']['sessionBounces'][j]
                        r_activeTime = item['historyData']['activeTime'][j]
                        r_uniqueUsers = item['historyData']['uniqueUsers'][j]
                    if traffic_request_method == "/traffic/keyword":
                        r_weight = item['historyData']['weight'][j]

                    if ("site" in traffic_request_groups) and (traffic_request_method == "/traffic/event") and (request_for_set_of_sites == "True"):
                        if combination:
                            r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_site) + '-' + str(combination) 
                        else:
                            r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_site) 
                    else:
                        if combination:
                            r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) + '-' + str(combination) 
                        else:
                            r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_site) 


                    if ("site" in traffic_request_groups) and (traffic_request_method == "/traffic/event") and (request_for_set_of_sites == "True"):
                        values = [r_id, r_date, r_group, r_site, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls, r_site]
                    if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
                        values = [r_id, r_date, r_group, r_item, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls, r_site]
                    if traffic_request_method == "/traffic/keyword":
                        values = [r_id, r_date, r_group, r_item, r_events, r_urls, r_weight]

                    arr = []
                    for val in combination:
                        arr.append(val)
                    for val in values:
                        arr.append(val)

                    df.loc[row_index] = arr
                    row_index += 1

    traffic_tab = df.set_index('id')
              
    if ("site" in traffic_request_groups) and (traffic_request_method == "/traffic/event") and (request_for_set_of_sites == "True"):
        df['item'] = str(r_site)
        df = df.set_index('id')
        df1 = df.groupby(df.index)['events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls'].sum().reset_index()
        df1 = df1.set_index('id')
        df2 = df.drop(['events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls'], axis=1)
        df2 = df2.drop_duplicates()

        traffic_tab = pd.concat([df2, df1], axis=1, join_axes=[df2.index])

    return(traffic_tab,resp)

# --------------------------------------
# KEYWORD + KEYWORD; WITHOUT USERS

def keyword_tab_without_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter,keyword_dict):

    traffic_request_groups = ["site"]

    traffic_request_template = full_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups)
    
    if traffic_request_stop == "now":
        del traffic_request_template['stop']

    main_traffic_items_list = [] # list with items of chosen groups

    for main_traffic_group in main_traffic_groups_list:
        for traffic_event_item_key in traffic_event_group_item_dict:
            if traffic_event_item_key == main_traffic_group:
                main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key]) 

    response_column_names = ['id', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'siteId']

    df_columns = []
    for col in main_traffic_groups_list:
        df_columns.append(col)
    for col in response_column_names:
        df_columns.append(col)

    df = pd.DataFrame(columns=df_columns)
    row_index = 1

    keyword_filters = []
    for key, value in keyword_dict.items():
        for v in value:
            keyword_filter = {"type":"keyword", "group":key, "item":v}
            keyword_filters.append(keyword_filter)
    print("keyword_filters: ", keyword_filters)

    for k_filter in keyword_filters:

        print("PRODUCTS,", main_traffic_items_list)
        for combination in itertools.product(*main_traffic_items_list):
            print("combination ,", combination)
            filters = []
            filters.append(k_filter)

            for i in range(len(combination)):
                column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                filters.append(column_filter)

            traffic_request_template["filters"] = filters

            print("filters: ", filters)
            print("keyord_filter: ", k_filter)

            traffic_request_method = "/traffic/event"
            resp = execute(traffic_request_method, traffic_request_template, username, secret)
            #print("resp: ",resp)
            print("traffic_request_template: ", traffic_request_template)

            try:
                dates = resp[1]['history']
            except:
                print("Invalid credentials")
                exit(1)

            for j in range(len(dates) - 1):
                print("groups count,", dates[j], len(resp[1]['groups']))
                for group in resp[1]['groups']:
                    print("group items count,", group['group'], len(group['items']))
                    for item in group['items']:
                        print("item,", item['item'])
                        r_date = dates[j]
                        r_keyword_group = k_filter['group']
                        r_keyword_item = k_filter['item']
                        if site_ids_filter == "All":
                            r_site = "All-" + str(len(siteId))
                        else:
                            r_site = siteId 

                        r_sessionStarts = item['historyData']['sessionStarts'][j]
                        r_sessionStops = item['historyData']['sessionStops'][j]
                        r_sessionBounces = item['historyData']['sessionBounces'][j]
                        r_activeTime = item['historyData']['activeTime'][j]
                        r_uniqueUsers = item['historyData']['uniqueUsers'][j]
                    
                        if combination:
                            r_id = str(r_date) + '-' + str(r_keyword_group) + '-' + str(r_keyword_item) + '-' + str(r_site) + '-' + str(combination) 
                        else:
                            r_id = str(r_date) + '-' + str(r_keyword_group) + '-' + str(r_keyword_item) + '-' + str(r_site) 

                        values = [r_id, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_site]
                            
                        arr = []
                        for val in combination:
                            arr.append(val)
                        for val in values:
                            arr.append(val)
                                   
                        df.loc[row_index] = arr
                        row_index += 1

    traffic_tab = df.set_index('id')

    return(traffic_tab)

# --------------------------------------
# WITH USERS 

def traffic_tab_with_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter):

    if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
        traffic_request_template = full_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups)
    if traffic_request_method == "/traffic/keyword":
        traffic_request_template = short_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups)

    if traffic_request_stop == "now":
        del traffic_request_template['stop']

    main_traffic_items_list = [] # list with items of chosen groups

    for main_traffic_group in main_traffic_groups_list:
        for traffic_event_item_key in traffic_event_group_item_dict:
            if traffic_event_item_key == main_traffic_group:
                main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key])

    if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
        response_column_names = ['id', 'date', 'group', 'item', 'userGroup', 'userId', 'events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls', 'siteId']
    if traffic_request_method == "/traffic/keyword":
        response_column_names = ['id', 'date', 'group','item', 'userGroup', 'userId', 'events', 'urls', 'weight']

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
    print("user_filters: ", user_filters)
    
    for u_filter in user_filters:
        
        print("PRODUCTS,", main_traffic_items_list)
        for combination in itertools.product(*main_traffic_items_list):
            print("combination ,", combination)
            filters = []
            filters.append(u_filter)

            for i in range(len(combination)):
                column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                filters.append(column_filter)

            print("filters: ", filters)
            print("user_filter: ", u_filter)

            traffic_request_template["filters"] = filters

            resp = execute(traffic_request_method, traffic_request_template, username, secret)
            #print(resp)

            try:
                dates = resp[1]['history']
            except:
                print("Invalid credentials")
                exit(1)

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

                        if (site_ids_filter == "All") and (request_for_set_of_sites == "True"):
                            r_site = "All-" + str(len(siteId))
                        else:
                            r_site = str(siteId) 
                        
                        if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"): 
                            r_sessionStarts = item['historyData']['sessionStarts'][j]
                            r_sessionStops = item['historyData']['sessionStops'][j]
                            r_sessionBounces = item['historyData']['sessionBounces'][j]
                            r_activeTime = item['historyData']['activeTime'][j]
                            r_uniqueUsers = item['historyData']['uniqueUsers'][j]
                        if traffic_request_method == "/traffic/keyword":
                            r_weight = item['historyData']['weight'][j]

                        if ("site" in traffic_request_groups) and (traffic_request_method == "/traffic/event") and (request_for_set_of_sites == "True"):
                            if combination:
                                r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_site) + '-' + str(r_user_group) + '-' + str(r_user_id) + '-' + str(combination) 
                            else:
                                r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_site) + '-' + str(r_user_group) + '-' + str(r_user_id) 
                        else:
                            if combination:
                                r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_user_group) + '-' + str(r_user_id) + '-' + str(r_site) + '-' + str(combination) 
                            else:
                                r_id = str(r_date) + '-' + str(r_group) + '-' + str(r_item) + '-' + str(r_user_group) + '-' + str(r_user_id) + '-' + str(r_site) 
                               
                        if ("site" in traffic_request_groups) and (traffic_request_method == "/traffic/event") and (request_for_set_of_sites == "True"):
                            values = [r_id, r_date, r_group, r_site, r_user_group, r_user_id, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls, r_site]
                        if (traffic_request_method == "/traffic/event") or (traffic_request_method == "/traffic/custom"):
                            values = [r_id, r_date, r_group, r_item, r_user_group, r_user_id, r_events, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_urls, r_site]
                        if traffic_request_method == "/traffic/keyword":
                            values = [r_id, r_date, r_group, r_item, r_user_group, r_user_id, r_events, r_urls, r_weight]

                        arr = []
                        for val in combination:
                            arr.append(val)
                        for val in values:
                            arr.append(val)
            
                        df.loc[row_index] = arr
                        row_index += 1

    traffic_tab = df.set_index('id')
    
    if ("site" in traffic_request_groups) and (traffic_request_method == "/traffic/event") and (request_for_set_of_sites == "True"):
        df['item'] = str(r_site)
        df = df.set_index('id')
        df1 = df.groupby(df.index)['events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls'].sum().reset_index()
        df1 = df1.set_index('id')
        df2 = df.drop(['events', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'urls'], axis=1)
        df2 = df2.drop_duplicates()
        
        traffic_tab = pd.concat([df2, df1], axis=1, join_axes=[df2.index])

    return(traffic_tab,resp)

# --------------------------------------
# KEYWORD + KEYWORD; WITH USERS

def keyword_tab_with_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter,keyword_dict):

    traffic_request_groups = ["site"]

    traffic_request_template = full_traffic_request_template(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,traffic_request_groups)
   
    if traffic_request_stop == "now":
        del traffic_request_template['stop']

    main_traffic_items_list = [] # list with items of chosen groups

    for main_traffic_group in main_traffic_groups_list:
        for traffic_event_item_key in traffic_event_group_item_dict:
            if traffic_event_item_key == main_traffic_group:
                main_traffic_items_list.append(traffic_event_group_item_dict[traffic_event_item_key])

    response_column_names = ['id', 'sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers', 'siteId']

    df_columns = []
    for col in main_traffic_groups_list:
        df_columns.append(col)
    for col in response_column_names:
        df_columns.append(col)

    df = pd.DataFrame(columns=df_columns)
    row_index = 1

    keyword_filters = []
    for key, value in keyword_dict.items():
        for v in value:
            keyword_filter = {"type":"keyword", "group":key, "item":v}
            keyword_filters.append(keyword_filter)
    print("keyword_filters: ", keyword_filters)

    for k_filter in keyword_filters:

        user_filters = []
        for key, value in traffic_user_dict.items():
            for v in value:
                user_filter = {"type":"user", "group":key, "item":v}
                user_filters.append(user_filter)
        print("user_filters: ", user_filters)
        
        for u_filter in user_filters:
            
            print("PRODUCTS,", main_traffic_items_list)
            for combination in itertools.product(*main_traffic_items_list):
                print("combination ,", combination)
                filters = []
                filters.append(u_filter)
                filters.append(k_filter)

                for i in range(len(combination)):
                    column_filter = {"type":"event", "group":main_traffic_groups_list[i], "item":combination[i]}
                    filters.append(column_filter)

                traffic_request_template["filters"] = filters

                print("filters: ", filters)
                print("user_filter: ", u_filter)
                print("keyword_filter: ", k_filter)

                traffic_request_method = "/traffic/event"
                resp = execute(traffic_request_method, traffic_request_template, username, secret)
                #print("resp: ",resp)
                #print("traffic_request_template: ", traffic_request_template)

                try:
                    dates = resp[1]['history']
                except:
                    print("Invalid credentials")
                    exit(1)

                for j in range(len(dates) - 1):
                    print("groups count,", dates[j], len(resp[1]['groups']))
                    for group in resp[1]['groups']:
                        print("group items count,", group['group'], len(group['items']))
                        for item in group['items']:
                            print("item,", item['item'])
                            r_date = dates[j]
                            r_keyword_group = k_filter['group']
                            r_keyword_item = k_filter['item']
                            r_user_group = u_filter['group']
                            r_user_id = u_filter['item']
                            if site_ids_filter == "All":
                                r_site = "All" + str(len(siteId))
                            else:
                                r_site = siteId 
                            
                            r_sessionStarts = item['historyData']['sessionStarts'][j]
                            r_sessionStops = item['historyData']['sessionStops'][j]
                            r_sessionBounces = item['historyData']['sessionBounces'][j]
                            r_activeTime = item['historyData']['activeTime'][j]
                            r_uniqueUsers = item['historyData']['uniqueUsers'][j]
                            
                            if combination:
                                r_id = str(r_date) + '-' + str(r_keyword_group) + '-' + str(r_keyword_item) + '-' + str(r_user_group) + '-' + str(r_user_id) + '-' + str(r_site) + '-' + str(combination) 
                            else:
                                r_id = str(r_date) + '-' + str(r_keyword_group) + '-' + str(r_keyword_item) + '-' + str(r_user_group) + '-' + str(r_user_id) + '-' + str(r_site)
                            
                            values = [r_id, r_sessionStarts, r_sessionStops, r_sessionBounces, r_activeTime, r_uniqueUsers, r_site]

                            arr = []
                            for val in combination:
                                arr.append(val)
                            for val in values:
                                arr.append(val)
                
                            df.loc[row_index] = arr
                            row_index += 1

    traffic_tab = df.set_index('id')

    return(traffic_tab)  

# ---------------------------------------------------------------------
# MAIN PART

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
    request_for_set_of_sites = configuration['request_for_set_of_sites']
except:
    print("Please complete the missing part of the configuration")
    exit(1)

if (site_ids_filter == "All") and (request_for_set_of_sites == "False"):
    print("Invalid credentials")
    exit(1)

if __name__ == "__main__":
    username = request_username
    secret   = request_secret

#  --------------------------------------------------------------------------------------------------------------------------------
# SITE API CALL
    site_request = (execute("/site", {
                                      }, username, secret))
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
# TRAFFIC EVENT API CALLs 

        if isinstance(site_ids_filter, list) == True:
            main_site_ids = site_ids_filter
        if (site_ids_filter == "False") or (site_ids_filter == "All"):
            main_site_ids = site_ids

        try:
            main_site_ids
        except:
            print("Invalid credentials")
            exit(1)

        # traffic_request_stop
        if traffic_request_stop == "today":
            traffic_request_stop = datetime.datetime.today().strftime('%Y-%m-%d')
        else:
            traffic_request_stop = new_date(traffic_request_stop)
        # traffic_request_start
        traffic_request_start = new_date(traffic_request_start)

        list_tables = []

        if request_for_set_of_sites == "False":
        
            for siteId in main_site_ids:     
                print("SITE ID", siteId)

                traffic_event_group_item_dict = traffic_response_dict(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,execute,t_method="event")

                if user_ids == "False":
                    
                    traffic_tab, resp = traffic_tab_without_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter)
                
                    if traffic_request_method == "/traffic/keyword":
                        keyword_dict = {}
                        for k_group in resp[1]['groups']:
                            k_key = k_group['group']
                            for k_item in k_group['items']:
                                k_value = k_item['item']
                                keyword_dict.setdefault(k_key, []).append(k_value)

                        traffic_tables_2 = keyword_tab_without_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter,keyword_dict)
                        keyword_tab = traffic_tables_2[['sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers']]

                        traffic_tab = pd.concat([traffic_tab, keyword_tab], axis=1, join_axes=[traffic_tab.index])

                    list_tables.append(traffic_tab)
                    traffic_tables = pd.concat(list_tables)

                if user_ids == "True":
                    
                    traffic_user_dict = traffic_response_dict(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,execute,t_method="user")
                    print("users: ", traffic_user_dict)

                    traffic_tab,resp = traffic_tab_with_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter)

                    if traffic_request_method == "/traffic/keyword":
                        keyword_dict = {}
                        for k_group in resp[1]['groups']:
                            k_key = k_group['group']
                            for k_item in k_group['items']:
                                k_value = k_item['item']
                                keyword_dict.setdefault(k_key, []).append(k_value)

                        traffic_tables_2 = keyword_tab_with_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter,keyword_dict)
                        keyword_tab = traffic_tables_2[['sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers']]

                        traffic_tab = pd.concat([traffic_tab, keyword_tab], axis=1, join_axes=[traffic_tab.index])

                    list_tables.append(traffic_tab)
                    traffic_tables = pd.concat(list_tables)

        if request_for_set_of_sites == "True":
            siteId = main_site_ids
            print("SITE IDs", siteId)

            traffic_event_group_item_dict = traffic_response_dict(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,execute,t_method="event")

            if user_ids == "False":
                    
                traffic_tab, resp = traffic_tab_without_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter)

                if traffic_request_method == "/traffic/keyword":
                    keyword_dict = {}
                    for k_group in resp[1]['groups']:
                        k_key = k_group['group']
                        for k_item in k_group['items']:
                            k_value = k_item['item']
                            keyword_dict.setdefault(k_key, []).append(k_value)

                    traffic_tables_2 = keyword_tab_without_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter,keyword_dict)
                    keyword_tab = traffic_tables_2[['sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers']]
                    keyword_tab = keyword_tab.groupby(keyword_tab.index)['sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers'].sum().reset_index()
                    keyword_tab = keyword_tab.set_index('id')

                    traffic_tab = pd.concat([traffic_tab, keyword_tab], axis=1, join_axes=[traffic_tab.index])

                list_tables.append(traffic_tab)
                traffic_tables = pd.concat(list_tables)

            if user_ids == "True":
                    
                traffic_user_dict = traffic_response_dict(siteId,traffic_request_stop,traffic_request_start,traffic_request_historyResolution,execute,t_method="user")
                print("users: ", traffic_user_dict)

                traffic_tab,resp = traffic_tab_with_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter)

                if traffic_request_method == "/traffic/keyword":
                    keyword_dict = {}
                    for k_group in resp[1]['groups']:
                        k_key = k_group['group']
                        for k_item in k_group['items']:
                            k_value = k_item['item']
                            keyword_dict.setdefault(k_key, []).append(k_value)

                    traffic_tables_2 = keyword_tab_with_users(traffic_request_method,main_traffic_groups_list,traffic_event_group_item_dict,site_ids_filter,keyword_dict)
                    keyword_tab = traffic_tables_2[['sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers']]
                    keyword_tab = keyword_tab.groupby(keyword_tab.index)['sessionStarts', 'sessionStops', 'sessionBounces', 'activeTime', 'uniqueUsers'].sum().reset_index()
                    keyword_tab = keyword_tab.set_index('id')

                    traffic_tab = pd.concat([traffic_tab, keyword_tab], axis=1, join_axes=[traffic_tab.index])    

                list_tables.append(traffic_tab)
                traffic_tables = pd.concat(list_tables)
#  -------------------------------------------------------------------------------------------------------------------------------- 
# # TRAFFIC OUT

        traffic_tables = traffic_tables.fillna("Null")
        cfg.write_table_manifest(outTrafficFullName, destination=outDestinationTraffic, primary_key=['id'], incremental=True)
        traffic_tables.to_csv(path_or_buf=outTrafficFullName) 