package com.yahoo.swc.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EfCountWord extends BaseRichBolt {
	
	private static final long serialVersionUID = -7650439263074510377L;
	
	private OutputCollector _outputCollector = null;
	private HashMap<String, Double> _efCounter = null;
	private double _alpha = 0.1;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;
		_efCounter = new HashMap<String, Double>();
	}

	public void execute(Tuple input) {
		//Exponential forgetting
		String word = input.getString(0);
		Double efCount = _efCounter.get(word);
		if(efCount == null){
			efCount = 0.0;
		}
		efCount = 1.0 + (1.0 - _alpha)*efCount;
		_efCounter.put(word, efCount);
		_outputCollector.emit(new Values(word, efCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "efcount"));		
	}

}
