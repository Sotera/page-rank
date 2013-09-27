package org.xdata.analytics.pagerank;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;


public class PRMasterCompute extends DefaultMasterCompute {

	public static final double EPSILON = 0.001;
	
	 @Override
     public void initialize() throws InstantiationException, IllegalAccessException {
		 registerPersistentAggregator(PageRankVertex.VERTEX_COUNT_AGG, LongSumAggregator.class);
		 //registerAggregator(PageRankVertex.VERTEX_VOTE,LongSumAggregator.class);
		 registerAggregator(PageRankVertex.MAX_EPSILON,DoubleMaxAggregator.class);
     }
	 
	 
	 @Override
	 public void compute(){
		 long step = this.getSuperstep();
		 if (step >2){
			 double maxDelta = ((DoubleWritable) this.getAggregatedValue(PageRankVertex.MAX_EPSILON)).get();
			 System.out.println("step: "+step+" max delta: "+maxDelta);
			 if (maxDelta < EPSILON){
				 this.haltComputation();
				 System.out.println(maxDelta+" < "+EPSILON+" halting computation");
			 }
		 }
		 
		
	 }

}
