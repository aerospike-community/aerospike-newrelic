# USER-GUIDE
## Predefined dashboards and graphs
-----------------------------------------
User installing new relic plugin for aerospike will get some per-defined dashboards and summary charts out of the box.
These include following dashboards.

   1. Overview :  This Dashboard shows per cluster stats graph. Added graphs are given below.
	   * ClusterSize
	   * Reads, Writes
	   * Used memory, disk
   2. Node Statistics : This Dashboard shows per node stats graph. Added graphs are given below.
	   * Reads, writes
	   * Used memory, disk
	   * Client connection
       * Node uptime
	   * Migration incoming, outgoing
	   * Objects
	   * Basic scan succeeded
	   * Batch initiate
   3. Namespaces : This Dashboard shows per node per namespace stats graph. Added graphs are given below.
	   * Used memory, disk
	   * Master, replica objects
	   * Expired, evicted objects
   4. Latency : This Dashboard shows per cluster latency stats graph. Added graphs are given below.
	   * Read, writes
	   * UDF, Query, Proxy
	 
Note :
	These predefined Dashboards shows all important information. Above it users can add their own custom Dashboard as per requirenment.


## Creation of custom Dashboards and graphs
---------------------------------------------------
Users can create custom dashboards using the data pushed by plugin. Custom dashboards usage might be restricted to certain types of new relic account types.

For creating custom Dashboard:
   * Click "Create custom dashboard" under Tools tab.
	 1. Tools -> Create custom dashboard
	 2. Choose a layout (Overview or Grid).
	 3. For Adding any new Chart or table
	    1. Click on “Add chart or table”.
	    2. Choose Agent type “Aerospike”.
	    3. Choose Component (For which you are adding dashboard).
	    4. Choose Visualizer(Chart or table)
	    5. Insert the required form fields
	       1. Title
	       2. Metric: Metric name would be same as the metric name pushed from plugin. check metric naming section(e.g; Component/aerospike/summary/used_memory[])
	       3. Value (select average value)
	       4. Chart type(Line or stacked area)

Users can add their own custom charts in these custom dashboards.


## Aerospike Metric Categories (Metric naming)
---------------------------------------------------
Plugin is used to push metrics in defined hierarchy/categories in order to distinguish stats viz; node, namespace, cluster stats.  
Categories are :

   1. Summary : In this category stats per cluster are getting pushed. Metrics are pushed in the following manner
	   * Component/aerospike/summary/{stat}
	       * Component/aerospike/summary/cluster_size[]
	       * Component/aerospike/summary/used_memory[]
	   * Component/aerospike/summary/{stat category}/{stat}
	       * Component/aerospike/summary/reads/success[]
	       * Component/aerospike/summary/writes/success[]
	   * Component/aerospike/summary/latency/{stat category}/{stat subcategory}/{stat}
	       * Component/aerospike/summary/latency/reads/0ms_to_1ms/value[]
	       * Component/aerospike/summary/latency/udf/0ms_to_1ms/value[]
   2. NodeStats: In this category stats per node are getting pushed. Metrics are pushed in the following manner
	   * Component/aerospike/nodeStats/{Node_IP}/{stat}
	       * Component/aerospike/nodeStats/{Node_IP}/used-bytes-disk[]
	   * Component/aerospike/throughputStats/{Node_IP}/{stat category}/{stat}
	       * Component/aerospike/throughputStats/{Node_IP}/reads/success[] 
   3. NamespaceStats: In this category stats per namespace are getting pushed. Metrics are pushed in the following manner
	   * Component/aerospike/namespaceStats/{Node_IP}/{namespace}/{stat}
	       * Component/aerospike/namespaceStats/{Node_IP}/{namespace}/used-bytes-memory[]
	       * Component/aerospike/namespaceStats/{Node_IP}/{namespace}/master-objects[]
   4. LatencyStat: In this category latency stats per node are getting pushed. Metrics are pushed in the following manner
	   * Component/aerospike/latencyStats/{Node_IP}/{stat category}/{stat subcategory}/{stat}
	       * Component/aerospike/latencyStats/{Node_IP}/writes_master/0ms_to_1ms/value[]
   5. ThroughputStat:  In this category throughput stats (reads and writes) per node are getting pushed. Metrics are pushed in the following 
	   * Component/aerospike/throughputStats/{Node_IP}/{stat category}/{stat}
	       * Component/aerospike/throughputStats/{Node_IP}/reads/success[]
	        
Note :
	User can choose from the above mentioned categories in order to create custom dashboards.
* {Node_IP} is the node IP, present within a cluster.
* {namespace} is the namespace name(e.g; test, bar) of a node.
* {stat} is the stat metric name. There are many stats pushed from newrelic plugin, User can find all the stat metric name here (http://www.aerospike.com/docs/reference/metrics/). In the given link NodeStats are under {Statistics} category and NamespaceStats are under {Namespace} category.

## Using wildcard "*" for grouping metrics
--------------------------------------------
While creating custom dashboards, user can group metrics using wildcard"*".  
For example .

1. If user wants to plot "used-bytes-memory" for all nodes in a single graph, then this can be done in the following way:
	* Component/aerospike/nodeStats/*/used-bytes-memory[]
2. To monitor similar stats of a node like batch_initiate, batch_error etc refer to the following format:
	* Component/aerospike/nodeStats/<ip>/batch*
3.  In order to monitor all batch matching stats of all ips(nodes) refer to the following format.
	* Component/aerospike/nodeStats/*/batch*
4. Any nodeStat of all nodes in cluster can be monitored in following way.
	* Component/aerospike/nodeStats/*/<stats>
5. Specific namespace stats for all nodes in cluster can be monitored in following way.
	* Component/aerospike/namespaceStats/*/<namespace>/<stats>
6. All namespace stats for all nodes in cluster can be monitored in following way.
	* Component/aerospike/namespaceStats/*/<stats>
	
Note :
* This wild card can be used once in between and once at the end.
* These are some valid wildcard usage
    * Component/aerospike/nodeStats/*/batch*
    * Component/aerospike/namespaceStats/*/<stats>
    * Component/aerospike/latencyStats/*/write_master/1ms_to_8ms/*
* Currently new relic does not allow usage of multiple wildcard characters in metric names as given below.
    * Component/aerospike/*/<ip>/*/<stat> does not found any matching metric.

## Limitations
--------------
* At present we don’t push any XDR stats.
	            
