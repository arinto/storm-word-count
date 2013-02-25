package com.yahoo.swc.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 *  Normal Count Word bolt. No decay function/weight is introduced in this implementation
 */

public class CountWord extends BaseRichBolt{

	
	private static final long serialVersionUID = -4206746564598093461L;
	private OutputCollector _outputCollector = null;
	private Map<String, MutableInt> _counter = null;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;
		_counter = new HashMap<String, MutableInt>();
	}

	public void execute(Tuple input) {		
		String word = input.getString(0);
		MutableInt count = _counter.get(word);
		if (count == null) {
			count = new MutableInt();				
		}
		count.increment();
		_counter.put(word, count);
		_outputCollector.emit(new Values(word, count));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
