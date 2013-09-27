Page Rank
==============================
This project is a Giraph/Hadoop implementation of a distributed version of the Page Rank algorithm.

Build
-----
Prior to building you must first download appache giraph and build a version for your cluster.  
Then install the giraph-core-with-dependencies.jar into your local mvn repository.

These are instructions for building Giraph 1.0 against CDH 4.2.0.

1. Download Giraph (http://giraph.apache.org/) -> (http://www.apache.org/dyn/closer.cgi/giraph/giraph-1.0.0)

2. Extract.

3. Find the hadoop_cdh4.1.2 profile within pom.xml and copy the entire section and paste below.

4. Edit the new section changing instances of 4.1.2 to 4.2.0 within the section.

5. From the command line at the top level type 'mvn -Phadoop_cdh4.2.0 -DskipTests clean install'

6. This will install giraph-core-1.0.0.jar in your local maven repository specifically usable for CDH 4.2.0

7. You should now be able to build the Page Rank job using ./build.sh

Example Run
-----------
A small example is included to verify installation and the general concept in the 'example' directory.

To run, go to the example directory and type

> ./run_example.sh

Other Information
-----------------

The graph must be stored as a bi-directional weighted graph with one vertex represented below in a 
tab-delimited file stored on hdfs.  The columns required are node id and the edge 
list.  The edge list should be a comma-separated list of edges where each edge is represented by the form id:weight.  

For example...

> 12345	1:1,2:1,9:33<br>
1	12345:1<br>
2	12345:1<br>
9	12345:33<br>

In this case vertex 12345 has 3 edges.  Each edge appears identically for both its vertices.


The giraph job outputs a tab-delimited hdfs file with the following fields: id (node/edge) and page rank.
