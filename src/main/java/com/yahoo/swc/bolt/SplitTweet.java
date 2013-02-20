package com.yahoo.swc.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitTweet extends BaseRichBolt {

	/**
	 * Bolt to split the tweets into words
	 */
	private static final long serialVersionUID = 3046647784643054989L;
	private static final String REGEX_SPLIT = 
			"[,;\\s\":'<>()\\[\\]\\^\\!\\$\\%\\&\\*\\-" +
			"\\+=_\\{\\}\\.\\?\\\\/]+";
	
	private OutputCollector _outputCollector = null;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;
		
	}

	public void execute(Tuple tuple) {
		String tweetStatus = tuple.getString(0);
		String[] words = tweetStatus.split(REGEX_SPLIT);
		
		for(String word: words){
			_outputCollector.emit(new Values(word));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
