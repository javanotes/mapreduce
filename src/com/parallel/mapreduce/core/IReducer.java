package com.parallel.mapreduce.core;

import java.util.Collection;

/**
 * Implement the class to provide the reduce logic. Will associate a collection of V for the key K.
 * Multiple threads will act on a single instance of this class. So synchronize instance variables if any accordingly.
 * @author esutdal
 *
 *
 * @param <K>	mapped key type
 * @param <V>	mapped value type
 */
public interface IReducer<K, V> {
	
	public KeyValue<K, V> reduceForKey(K key, Collection<V> values);
		

}
