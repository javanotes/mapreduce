package com.parallel.mapreduce.functions;

import java.util.ArrayList;
import java.util.List;

import com.parallel.mapreduce.core.IMapper;
import com.parallel.mapreduce.core.KeyValue;

/**
 * Simple mapper functionality for a "vowel count". Method mapToKeyValue() accepts a string and return a List of KeyValue having key as 
 * a vowel (a,e,i,o,u) and value as 1, as to represent a single occurrence of that vowel
 * @author esutdal
 *
 */
public class VowelCountMapper implements IMapper<String, String, Integer> {

	@Override
	public List<KeyValue<String, Integer>> mapToKeyValue(String each) {
		List<KeyValue<String, Integer>> list = new ArrayList<KeyValue<String,Integer>>();
		
		/*try {
			//mimic some blocking
			Thread.sleep(1);
		} catch (InterruptedException e1) {
			
		}*/
		for(char c : each.toCharArray()){
			switch(c){
			case 'a': list.add(new KeyValue<String, Integer>("a", 1));break;
			case 'e': list.add(new KeyValue<String, Integer>("e", 1));break;
			case 'i': list.add(new KeyValue<String, Integer>("i", 1));break;
			case 'o': list.add(new KeyValue<String, Integer>("o", 1));break;
			case 'u': list.add(new KeyValue<String, Integer>("u", 1));break;
			default: break;
			}
		}
		
		return list;
	}

}
