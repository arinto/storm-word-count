package com.yahoo.swc;

import com.yahoo.swc.bolt.CountWord;
import com.yahoo.swc.bolt.SplitTweet;
import com.yahoo.swc.spout.TwitterSampleSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TwitterWordCountTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		//use TwitterSampleSpout
		String twitterSampleSpoutId = "sample_tweet";
		builder.setSpout(twitterSampleSpoutId, new TwitterSampleSpout(), 1);
		
		//use SplitTweet Bolt
		String splitTweetBoltId = "split_tweet";
		builder.setBolt(splitTweetBoltId, new SplitTweet(), 10)
				.shuffleGrouping(twitterSampleSpoutId);
			
		//use CountWord Bolt
		String countWordBoltId = "count_word";
		builder.setBolt(countWordBoltId, new CountWord(), 20)
				.fieldsGrouping(splitTweetBoltId, new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(true);
		
		//launch the topology in LocalCluster
		String topologyName = "word_count_topology";
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName , conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

}
