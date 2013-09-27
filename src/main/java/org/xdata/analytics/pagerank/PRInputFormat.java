package org.xdata.analytics.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;




/**
 * Reads in a graph from text file in hdfs.  Required formatin is a tab delimited file with 2 columns
 * id	edge list
 * 
 * the edge list is a comma seperated list of edges of the form    id:weight
 * 
 * The graph must be bi-directional   i.e.  if vertex 1 has edge 2:9, the vertex 2 must have id 1:9
 * This condition is not verified as the input is read, but results of the algorithim will not be correct,
 * and the run may fail with exceptions.  
 * 
 *
 */
public class PRInputFormat extends TextVertexInputFormat<Text,DoubleWritable,NullWritable>{

	@Override
	public TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new LouvainVertexReader();
	}
	
	protected class LouvainVertexReader extends TextVertexReader{

		@Override
		public Vertex<Text, DoubleWritable, NullWritable, Writable> getCurrentVertex()
				throws IOException, InterruptedException {

			String line = getRecordReader().getCurrentValue().toString();
			String[] tokens = line.split("\t");
			if (tokens.length != 2){
				throw new IllegalArgumentException("Invalid line: ("+line+")");
			}
			if (tokens[0].indexOf(',') != -1){
				throw new IllegalArgumentException("Bad ID in line: ("+line+")");
			}
			
			Text id = new Text(tokens[0]);
			
			Map<Text,NullWritable> edgeMap = new HashMap<Text,NullWritable>();
            String[] edges = tokens[1].split(",");
			for (int i = 0; i < edges.length ; i++){
				edgeMap.put( new Text(edges[i]), NullWritable.get());
				
			}
			ArrayList<Edge<Text,NullWritable>> edgesList = new ArrayList<Edge<Text,NullWritable>>();
			for (Map.Entry<Text,NullWritable> entry : edgeMap.entrySet()){
				edgesList.add(EdgeFactory.create(entry.getKey(),entry.getValue()));
			}

			Vertex<Text, DoubleWritable, NullWritable, Writable>  vertex = this.getConf().createVertex();
			vertex.initialize(id, new DoubleWritable(0),edgesList);
			return vertex;
		
		}
			
		

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
		
	}

}
