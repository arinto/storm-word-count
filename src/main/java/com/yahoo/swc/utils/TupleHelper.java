package com.yahoo.swc.utils;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/*
 * Helper class that contains Tuple-related utility functions
 */
public class TupleHelper {

	public static boolean isTickTuple(Tuple tuple){
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
}
