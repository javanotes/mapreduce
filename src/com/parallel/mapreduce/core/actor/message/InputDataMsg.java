package com.parallel.mapreduce.core.actor.message;


public class InputDataMsg<X> {

	private final X data;
	
	public InputDataMsg(X data){
		this.data = data;
	}

	public X getData() {
		return data;
	}
}
