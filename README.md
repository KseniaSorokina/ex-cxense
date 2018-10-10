# ex-cxense
Cxense extractor (Keboola Connection)

API documentation: https://wiki.cxense.com/display/cust/Cxense+Insight+API

## Configuration:
{
  "request_username": "", 
  "#request_secret": "",
- "site_table": "",
- "traffic_table": "",
⋅⋅*"traffic_table_name": "",
⋅⋅*  “traffic_request_start": "",
⋅⋅*  “traffic_request_stop”: "",
⋅⋅*  "traffic_request_history_resolution": "",
⋅⋅*  "traffic_request_method": "",
⋅⋅*  "traffic_filters": [ ],
⋅⋅*  "traffic_request_groups": [ ]
}

## Configuration description:
* “request_username”, “#request_secret" - Cxense username and password;

* Using this extractor, it is possible to obtain two types of tables:
  1. “Site” table, which describes all sites (columns: site_id, name, url, country). To get it, just write "site_table": “True”, If it is not necessary to obtain - "site_table": “False”.

  2. Another type - “Traffic” table, just write "traffic_table": “True" to get it or "traffic_table": “False” - if it not needed. This type of table can be modified depending on which methods, groups and filters will be selected.

	  a) A table showing the values (“events”, "sessionStarts","sessionStops", "sessionBounces", "activeTime", 	"uniqueUsers", "urls") of groups separately. 
	  (groups: "deviceType", “mobileBrand”, “browser", "connectionSpeed", “resolution", “colorDepth", "site",  			"exitLinkHost", "exitLinkUrl", "postalCode", "city", “url", "referrerUrl", "referrerHost", "referrerHostClass", 	"referrerSocialNetwork", “referrerSearchEngine”). 

	  groups description: https://wiki.cxense.com/display/cust/Event+groups
	  values description: https://wiki.cxense.com/display/cust/Cxense+Insight+Metrics
         ```
        “traffic_request_method": "/traffic/event",
  	"traffic_filters": [ ],   (field is empty)
  	"traffic_request_groups": [ "deviceType", “mobileBrand”]     ( names of the necessary groups are enough to 			insert here)
          ```

	  b) A table showing the values (the same as table a) of groups mix (the same groups as table a)

	  documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169348 

	  “traffic_request_method": "/traffic/event",
  	"traffic_filters": ["deviceType", “mobileBrand”],    ( names of the necessary groups are enough to insert here; at 		the same time it is better to take a small number of groups)
  	"traffic_request_groups": ["site”] 


	  c) A table showing the values (the same as table a) of groups mix (the same groups as table a + “template”) 
	
	  documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169350

	  “traffic_request_method": "/traffic/custom",
  	"traffic_filters": ["deviceType", “mobileBrand”],    ( names of the necessary groups are enough to insert here; at 		the same time it is better to take a small number of groups)
  	"traffic_request_groups": ["template”] 


	  d) A table showing the values (“events“, “urls”, “weight”) of groups mix (the same groups as table a + 	"category") 

	  documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169352

	  “traffic_request_method": "/traffic/keyword",
  	"traffic_filters": ["deviceType", “mobileBrand”],    ( names of the necessary groups are enough to insert here; at 		the same time it is better to take a small number of groups)
  	"traffic_request_groups": ["category"]


	  e) A table showing the values (“events“, “urls”, “weight”) of groups mix (the same groups as table a + 	"taxonomy") 

	  documentation: https://wiki.cxense.com/pages/viewpage.action?pageId=21169352

	  “traffic_request_method": "/traffic/keyword",
  	"traffic_filters": ["deviceType", “mobileBrand”],    ( names of the necessary groups are enough to insert here; at 		the same time it is better to take a small number of groups)
  	"traffic_request_groups": ["taxonomy"]


* “traffic_request_start”, “traffic_request_stop” - start and stop period
Time specification: https://wiki.cxense.com/display/cust/Traffic+time+specification

* “traffic_request_history_resolution” ("month", "week", "day", "hour" and “minute")
