#! /bin/bash

GIRAPH_SLAVES=1
INPUT=/user/bigdata/page-rank/input
OUTPUT=/user/bigdata/page-rank/output

hadoop fs -rm -r $OUTPUT

hadoop jar target/pagerank-0.1-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
-Dgiraph.zkList=localhost:2181 \
-Dgiraph.useSuperstepCounters=false \
-Dmapred.child.java.opts=-Xmx5g \
org.xdata.analytics.pagerank.PageRankVertex \
-w $GIRAPH_SLAVES \
-mc org.xdata.analytics.pagerank.PRMasterCompute \
-vif org.xdata.analytics.pagerank.PRInputFormat \
-of org.xdata.analytics.pagerank.PROutputFormat \
-vip $INPUT \
-op $OUTPUT \
-ca giraph.vertex.input.dir=$INPUT \
-ca mapreduce.task.timeout=10800000