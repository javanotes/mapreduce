package com.parallel.mapreduce.test.functions;

import java.util.Collection;

import com.parallel.mapreduce.core.IReducer;
import com.parallel.mapreduce.core.KeyValue;

/**
 * Simple reducer functionality for a "vowel count". Method reduceForKey() accepts a List of KeyValue for a particular key (a vowel). 
 * It would then simply add up the value (1) to get a total count for that vowel
 * @author esutdal
 *
 */
public class VowelCountReducer implements IReducer<String, Integer> {

	@Override
	public KeyValue<String, Integer> reduceForKey(String key,
			Collection<Integer> values) {
		int count = 0;
		
		for(int k : values){
			//System.out.println("Adding .." + k + " for " + key);
			count += k;
		}
		
		KeyValue<String, Integer> s = new KeyValue<String, Integer>(key, count);
		
		return s;
	}

}
