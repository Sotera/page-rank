package org.xdata.analytics.pagerank;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;



public class PageRankVertex extends Vertex<Text, DoubleWritable, NullWritable, DoubleWritable> {

	public static final String VERTEX_COUNT_AGG = "org.xdata.analytics.pagerank.VERTEX_COUNT_AGG";
	//public static final String VERTEX_VOTE = "org.xdata.analytics.pagerank.VERTEX_COUNT_AGG";
	public static final String MAX_EPSILON = "org.xdata.analytics.pagerank.MAX_EPSILON";
	//private static final double EPSILON = 0.001;
	
	
	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		//int maxSuperSteps = this.getConf().getInt("superstep.limit", 10);
		float dampingFactor = this.getConf().getFloat("damping.factor", 0.85f);
		
		// on step 1 this will be the total number of vertices, on further steps
		// it is the number of vertices that want to continue.
		long N = ( (LongWritable) this.getAggregatedValue(VERTEX_COUNT_AGG)).get();
		//long votes = ( (LongWritable) this.getAggregatedValue(VERTEX_VOTE)).get();
		
		long step = getSuperstep();
		
		// count the total number of nodes.
		if (step == 0){
			this.aggregate(VERTEX_COUNT_AGG, new LongWritable(1L));
		}
		else if (step == 1){
			//set initial value
			this.setValue( new DoubleWritable(1.0 / N ));
			distributeRank();
		}
		else /*if (votes > 0 || step < 3 )*/{ // go until no one votes to continue
			
			double rank = 0;
			for (DoubleWritable partial : messages){
				rank += partial.get();
			}
			rank = ((1-dampingFactor)/N) + (dampingFactor*rank);
			double delta = Math.abs(rank - this.getValue().get()) / this.getValue().get();
			aggregate(MAX_EPSILON,new DoubleWritable(delta));
			//if (delta > EPSILON){
			//	this.aggregate(VERTEX_VOTE, new LongWritable(1L)); // vote to continue
			//}
			this.setValue(new DoubleWritable(rank));
			distributeRank();
			//System.out.println("step "+step+" ID: "+this.getId()+" updated rank to: "+this.getValue().toString());
		}
		
		/*else {
			voteToHalt();
		}*/

	}
	
	private void distributeRank(){
		double rank = this.getValue().get() / this.getNumEdges();
		this.sendMessageToAllEdges(new DoubleWritable(rank));
	}

}
