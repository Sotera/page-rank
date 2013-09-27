package org.xdata.analytics.pagerank;


import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;



/**
 * Outputs the graph as text in hdfs:
 * 
 * Format is a tab seperated file with
 *  id	rank
 * 
 */
public class PROutputFormat extends TextVertexOutputFormat<Text,DoubleWritable,NullWritable>{

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		return new PRVertexWriter();
	}

	
	protected class PRVertexWriter extends TextVertexWriter {
		@Override
		public void writeVertex(
				Vertex<Text, DoubleWritable, NullWritable, ?> vertex)
				throws IOException, InterruptedException {
			getRecordWriter().write(vertex.getId(), new Text(vertex.getValue().toString()));
		}
	}
	
	
	
	
	
}