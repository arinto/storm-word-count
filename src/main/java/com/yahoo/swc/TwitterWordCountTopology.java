package com.yahoo.swc;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class TwitterWordCountTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		//use TwitterSampleSpout
		//builder.setSpout(id, spout)
		
		//use SplitTweet Bolt
		//builder.setBolt
		
		//use CountWord Bolt
		//builder.setBolt
		
		Config conf = new Config();
		conf.setDebug(true);
		
		//launch the topology
	}

}
