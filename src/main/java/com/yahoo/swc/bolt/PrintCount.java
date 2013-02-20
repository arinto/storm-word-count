package com.yahoo.swc.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PrintCount extends BaseRichBolt {

	/**
	 * Bolt to print the current count to the console
	 */
	private static final long serialVersionUID = -6133323709164892042L;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// do nothing

	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		Object count = tuple.getValue(1);
		
		System.out.println(word +":" + count.toString());

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// do nothing
	}

}
