package com.parallel.mapreduce.core.message;

import java.util.Collection;

import com.parallel.mapreduce.core.KeyValue;

public class MappedMsg<K,V> {
	
	private final Collection<KeyValue<K, V>> list;
	
	public MappedMsg(Collection<KeyValue<K, V>> list){
		this.list = list;
	}

	public Collection<KeyValue<K, V>> getList() {
		return list;
	}

}
