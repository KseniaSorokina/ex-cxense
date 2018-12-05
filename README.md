# ex-cxense
Cxense extractor (Keboola Connection)

API documentation: https://wiki.cxense.com/display/cust/Cxense+Insight+API

This extractor is not public, to find it just put /kds.ex-cxense to the end of url (for example: https://connection.eu-central-1.keboola.com/..../extractors/kds.ex-cxense)

* sometimes results may vary:
Our systems are designed for high availability and will sometime return numbers that can be slightly different when executed several times. Realtime requests for recent data for periods of seconds or minutes can sometimes see incomplete data, due to the nature of the internet and that events will enter our systems out-of-order. All data sent to our systems will eventually be counted, usually within 1 second, but can sometimes lag a little behind.We also keep several redundant clusters of servers in a shared-nothing setup, where historic data can differ slightly over time. The difference is usually less than 0.001% and is usually only seen on high traffic sites. This should be negligible error margin, and much smaller than other factors on the internet. This has usually to do with our de-duplication algorithm of events, which can miss when two duplicates arrive at different points in time. (https://wiki.cxense.com/pages/viewpage.action?pageId=21169346)

## Configuration:
{
- "request_username": "", 
- "#request_secret": "",
- "site_table": "",
- "traffic_table": "",
- "traffic_table_name": "",
- "request_for_set_of_sites": "",
- "site_ids_filter": [],
- "user_ids": "True",
- “traffic_request_start": "",
- “traffic_request_stop”: "",
- "traffic_request_history_resolution": "",
- "traffic_request_method": "",
- "traffic_filters": [ ],
- "traffic_request_groups": [ ]
}

## Configuration description:
* “request_username”, “#request_secret" - Cxense username and password;

* Using this extractor, it is possible to obtain two types of tables:
  1. “Site” table, which describes all sites (columns: site_id, name, url, country). To get it, just write "site_table": “True”, If it is not necessary to obtain - "site_table": “False”.

  2. Another type - “Traffic” table, just write "traffic_table": “True" to get it or "traffic_table": “False” - if it not needed (other fields can be empty). This type of table can be modified depending on which methods, groups and filters will be selected.

	  a) A table showing the values (“events”, "sessionStarts","sessionStops", "sessionBounces", "activeTime", 	"uniqueUsers", "urls") of groups separately. 
	  (* groups: 
	   * "deviceType", 
	   * “mobileBrand”, 
	   * “browser", 
	   * "connectionSpeed", 
	   * “resolution", 
	   * “colorDepth", 
	   * "site",  			
	   * "exitLinkHost", 
	   * "exitLinkUrl", 
	   * "postalCode", 
	   * "city", 
	   * “url", 
	   * "referrerUrl", 
	   * "referrerHost", 
	   * "referrerHostClass", 	
	   * "referrerSocialNetwork", 
	   * “referrerSearchEngine”). 

	groups description: https://wiki.cxense.com/display/cust/Event+groups (all of these groups is possible to use)
	values description: https://wiki.cxense.com/display/cust/Cxense+Insight+Metrics

 	- “traffic_request_method": "/traffic/event",
 	- "traffic_filters": [ ],   (field is empty)
 	- "traffic_request_groups": [ "deviceType", “mobileBrand”, ...]     ( names of the necessary groups are enough to 			insert here)


	 b) A table showing the values (the same as table a) of groups mix (the same groups as table a)

	 documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169348 
	 - “traffic_request_method": "/traffic/event",
  	 - "traffic_filters": ["deviceType", “mobileBrand”, ...],(names of the necessary groups are enough to insert here; at the same time it is better to take a small number of groups)
	 - "traffic_request_groups": ["site”] 


	 c) A table showing the values (the same as table a) of groups mix (the same groups as table a + “template” and/or selected custom dimensions, which it is possible to find in "Custom Parameters") 
	
	 documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169350

	 - “traffic_request_method": "/traffic/custom",
  	 - "traffic_filters": ["deviceType", “mobileBrand”, ...],   (names of the necessary groups are enough to insert here; at 		the same time it is better to take a small number of groups)
  	 - "traffic_request_groups": ["template”, ...]  (custom dimensions)


	 d) A table showing the values (the same as table a + “weight”) of groups mix (the same groups as table a + 	"category" and/or "taxonomy" and/or selected custom dimensions, which it is possible to find in "Site Content") 

	 documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169352

	 - “traffic_request_method": "/traffic/keyword",
  	 - "traffic_filters": ["deviceType", “mobileBrand”, ...],    ( names of the necessary groups are enough to insert here; at 		the same time it is better to take a small number of groups)
  	 - "traffic_request_groups": ["category", ...] (custom dimensions)
	 - WARRING: metrics as "uniqueUsers","sessionStarts", "sessionStops", "sessionBounces", "activeTime" sometimes is "Null". Тhis is due to the fact that to recover these metrics is used another method "/traffic/event" as a backend (which is less powerfull than "/traffic/keyword")

* "request_for_set_of_sites" - returns metrics for selected set of sites;
	2 options:
	+ "True" - table with set of sites
	+ "False" - table with separate sites 
	 
* "site_ids_filter" - filter for specific sitе ids;
	3 options:
	+ "site_ids_filter": [“”, “”, ….] - opportunity to specify the interesting site ids
	+ ”site_ids_filter": “False” - (without parentheses) which allows to download data for all site ids
	+ ”site_ids_filter": “All” - can use only with "request_for_set_of_sites": "True"
	
* "user_ids" - for writing of user ids;
	2 options:
	+ "True" - table with user ids
	+ "False" - table without user ids

* “traffic_request_start”, “traffic_request_stop” - start and stop period
Time specification: https://wiki.cxense.com/display/cust/Traffic+time+specification; 
	+ a) Terms of use:
	+ it is possible to use "today" and "now" in “traffic_request_stop”;
	+ "today": used when downloading data for days, weeks, months, and years (for example “-1d”, “-1w”, “-1M”, “-1y”) --> the data will be downloaded from the beginning of the day
	+ "now": used when downloading data for seconds, hours or minutes(for example “-1s”, “-1m”, “-1h”) --> the data will be downloaded from the current time
	+ everything else works the same as written in the documentation	
	+ b) WARRING:
	+ it is not possible to use the same date in “traffic_request_start”, “traffic_request_stop”
	+ c) Example of “traffic_request_start”, “traffic_request_stop” uses:
	+ "traffic_request_start": “-1d“
	+ "traffic_request_stop": “today”
	+ if today is 2018-10-31 --> the table will show the start date of the period, which is 2018-10-30 00:00
	+ timestamp for Prague '2018-10-31T00:00:00.000+01:00'

* “traffic_request_history_resolution” ("month", "week", "day", "hour" and “minute")
