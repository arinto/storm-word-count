package com.yahoo.swc.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableDouble;

import com.yahoo.swc.utils.TupleHelper;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;

/*
 * ForwardDecayCountWord, based on Cormode's Paper:
 * "Forward Decay: A Practical Time Decay Model for Streaming Systems"
 * 
 * Based on definition 3:
 * gFunction is positive monotone non-decreasing function
 * _landmark is landmark time 
 * 
 */

public class ForwardDecayCountWord extends BaseRichBolt {

	private static final long serialVersionUID = -7070759934301780768L;
	
	private int _tickFrequency = 10; //10 seconds
	private int _decayAlpha = 10*1000; //10 seconds in mili-seconds
	private OutputCollector _outputCollector = null;
	private HashMap<String, MutableDouble> _counter = null;
	private long _landmark;
	
	//when constructor get messy, refactor this code to use Builder class
	public ForwardDecayCountWord(int tickFrequencySecond, int decayAlpha){
		_tickFrequency = tickFrequencySecond;
		_decayAlpha = decayAlpha*1000;
	}

	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;
		_counter = new HashMap<String, MutableDouble>();
		_landmark = Time.currentTimeMillis();
	}

	public void execute(Tuple input) {
		if (TupleHelper.isTickTuple(input)) {
			// emit the latest count when tick tuple is received
			// latest count is defined as
			// (1/g(currentTime - _landmark))*(sigma_on_i(gFunction(ti-_landmark)))

			long currentTime = Time.currentTimeMillis();
			double latestCount = 0.0;
			double gFunctionCurrTime = gFunction(currentTime);
			for (Map.Entry<String, MutableDouble> entry: _counter.entrySet()){
				latestCount = (entry.getValue().doubleValue()/gFunctionCurrTime);
				_outputCollector.emit(new Values(entry.getKey(), latestCount));
			}
		} else {
			// get the word and store the sigma_on_i(gFunction(ti-_landmark))
			String word = input.getString(0);
			MutableDouble count = _counter.get(word);

			if (count == null) {
				count = new MutableDouble();
			}

			count.add(gFunction(Time.currentTimeMillis()));
			_counter.put(word, count);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
	
	@Override 
	public Map<String, Object> getComponentConfiguration(){
		Config conf = new Config();
		//setup Storm's tick
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, _tickFrequency);
		return conf;
	}
	
	//monotonic non-decreasing function to calculate the decay
	private double gFunction(long timeInMs){
		//use exponential decay
		double val = (1.0/_decayAlpha)*(Long.valueOf(timeInMs - _landmark).doubleValue());
		return Math.pow(Math.E, val);
	}
}
