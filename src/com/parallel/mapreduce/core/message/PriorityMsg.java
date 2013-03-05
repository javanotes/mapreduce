package com.parallel.mapreduce.core.message;

public class PriorityMsg<X> {

	public static enum PRIORITY {HI, LO, N}
	private final X data;
	private final PRIORITY priority;
	
	public PriorityMsg(X d, PRIORITY p){
		data = d;
		priority = p;
	}

	public X getData() {
		return data;
	}

	public PRIORITY getPriority() {
		return priority;
	}
}
