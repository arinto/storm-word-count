package com.yahoo.swc;

import java.util.HashMap;

import com.yahoo.swc.bolt.CountWord;
import com.yahoo.swc.bolt.ForwardDecayCountWord;
import com.yahoo.swc.bolt.PrintCount;
import com.yahoo.swc.bolt.SplitTweet;
import com.yahoo.swc.spout.TwitterSampleSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/*
 *  Entry point for Storm word count program. Currently support two options
 *  1. nrml. Normal mode, count without any decay function or weight
 *  2. fedcount. Forward exponential decay count
 * 
 */

public class TwitterWordCountTopology {
	
	private static final String NORMAL_MODE = "nrml";
	private static final String FED_MODE = "fedcount";
	
	private static final HashMap<String, String> ALLOWED_OPTION = new HashMap<String, String>() {
		private static final long serialVersionUID = -4689033609960231840L;

		{
			put(NORMAL_MODE, "for normal counting");
			put(FED_MODE, "for forward-exponential-decay count");
		}
	};
		

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		if(args.length < 1 || !ALLOWED_OPTION.containsKey(args[0])){
			System.out.println("Usage \"TwitterWordCountTopology CountingMode\"");
			System.out.println("Where CountingMode is: ");
			for(String key: ALLOWED_OPTION.keySet()){
				System.out.println(key + ", " + ALLOWED_OPTION.get(key));
			}
		}
		
		TopologyBuilder builder = new TopologyBuilder();
		
		//use TwitterSampleSpout
		String twitterSampleSpoutId = "sample_tweet";
		int twitterSampleSpoutParallelism = 1;
		builder.setSpout(twitterSampleSpoutId, new TwitterSampleSpout(), twitterSampleSpoutParallelism);
		
		//use SplitTweet Bolt
		String splitTweetBoltId = "split_tweet";
		int splitTweetParallelism = 10;
		builder.setBolt(splitTweetBoltId, new SplitTweet(), splitTweetParallelism)
				.shuffleGrouping(twitterSampleSpoutId);
			
		//use CountWord Bolt
		//better code: use builder class to build specific bolt
		BaseRichBolt countWordBolt = null;
		String countWordBoltId = null;
		if(args[0].equals(NORMAL_MODE)){
			countWordBolt = new CountWord();
			countWordBoltId = "count_word";
		}else if (args[0].equals(FED_MODE)){
			countWordBolt = new ForwardDecayCountWord();
			countWordBoltId = "fwd_decay_count_word";
		}else{
			System.out.println(args[0] + " is a valid option but not yet implemented");
			return;
		}

		int countWordBoltParallelism = 10;
		builder.setBolt(countWordBoltId, countWordBolt, countWordBoltParallelism)
				.fieldsGrouping(splitTweetBoltId, new Fields("word"));
		
		//use PrinterCount Bolt to print the result
		String printerBoltId = "printer_bolt";
		int printerBoltPar = 10;
		builder.setBolt(printerBoltId, new PrintCount(), printerBoltPar)
			.fieldsGrouping(countWordBoltId, new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(false);
		
		if(args != null && args.length > 1){
			conf.setNumWorkers(3);
			
			StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
		}else{
			conf.setMaxTaskParallelism(3);
			
			String topologyName = "word_count_topology";
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName , conf, builder.createTopology());
			
			Utils.sleep(90*1000);
			
			cluster.killTopology(topologyName);
			cluster.shutdown();			
		}
	}
}
