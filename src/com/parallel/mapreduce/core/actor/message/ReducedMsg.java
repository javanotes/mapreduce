package com.parallel.mapreduce.core.actor.message;

import com.parallel.mapreduce.core.KeyValue;

public class ReducedMsg<K, V> {

	private final KeyValue<K, V> reduced;
	
	public ReducedMsg(KeyValue<K, V> y){
		reduced = y;
	}

	public KeyValue<K, V> getReduced() {
		return reduced;
	}
}
