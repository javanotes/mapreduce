package com.parallel.mapreduce.core;

/**
 * A key-value dictionary entry for each mapped entity
 * @author esutdal
 *
 * @param <K> mapped key type
 * @param <V> mapped value type
 */
public class KeyValue<K, V> {
	
	public KeyValue(K key, V value){
		this.key = key;
		this.value = value;
	}

	public K key;
	public V value;
}
