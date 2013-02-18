package com.yahoo.swc.bolt;

import java.util.HashMap;
import java.util.Map;

import com.yahoo.swc.model.EfTimeCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;

public class EfTimeCountWord extends BaseRichBolt {

	private static final long serialVersionUID = -7070759934301780768L;
	private static final double NORMALIZATION_FACTOR = 10000.0; //10 seconds
	
	private OutputCollector _outputCollector = null;
	private HashMap<String, EfTimeCount> _efTimeCounter = null;

	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;
		_efTimeCounter = new HashMap<String, EfTimeCount>();
	}

	public void execute(Tuple input) {
		// Exponential forgetting with consideration of time
		String word = input.getString(0);
		EfTimeCount efTimeCount = _efTimeCounter.get(word);
		long currTime = Time.currentTimeMillis();
		
		if (efTimeCount == null) {
			efTimeCount = new EfTimeCount(currTime, 0.0);
		}

		double power = (efTimeCount.getTime() - currTime)/NORMALIZATION_FACTOR;
		double alpha = Math.pow(Math.E, power);

		efTimeCount.setTime(currTime);
		
		double count = 1.0 + alpha * efTimeCount.getCount();
		efTimeCount.setCount(count);

		_efTimeCounter.put(word, efTimeCount);
		_outputCollector.emit(new Values(word, count));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}
