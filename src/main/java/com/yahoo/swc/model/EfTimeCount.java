package com.yahoo.swc.model;

public final class EfTimeCount {
	
	private long _time;
	private double _count;
	
	public EfTimeCount(long time, double count){
		_time = time;
		_count = count;
	}

	public long getTime() {
		return _time;
	}

	public void setTime(long time) {
		this._time = time;
	}

	public double getCount() {
		return _count;
	}

	public void setCount(double count) {
		this._count = count;
	}

	@Override
	public String toString() {
		return "EfTimeCount [ last occurrence =" + _time + ", count=" + _count + "]";
	}	
}
