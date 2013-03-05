package com.parallel.mapreduce.test.functions;

import java.util.Collection;
import java.util.Map.Entry;

import com.parallel.mapreduce.core.IReducer;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.test.Result;

public class CustomerReducer implements IReducer<String, Result> {

	@Override
	public KeyValue<String, Result> reduceForKey(String key,
			Collection<Result> values) {
		Result associated = new Result();
		
		for(Result value : values){
			associated.totalBalance += value.totalBalance;
			associated.outComeSuccess += value.outComeSuccess;
			associated.count += value.count == 0 ? 1 : value.count;
			for(Entry<String, Integer> each : value.education.entrySet()){
				if(value.education.containsKey(each.getKey())){
					value.education.put(each.getKey(), value.education.get(each.getKey()) + each.getValue());
				}
				else{
					value.education.put(each.getKey(), each.getValue());
				}
			}
		}
				
		return new KeyValue<String, Result>(key, associated);
	}
	
	

}
