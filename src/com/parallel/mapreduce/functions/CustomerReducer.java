package com.parallel.mapreduce.functions;

import java.util.Collection;

import com.parallel.mapreduce.core.IReducer;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.test.Result;

public class CustomerReducer implements IReducer<String, Result> {

	@Override
	public KeyValue<String, Result> reduceForKey(String key,
			Collection<Result> values) {
		Result associated = new Result();
		
		for(Result each : values)
			associated.associate(each);
		
		return new KeyValue<String, Result>(key, associated);
	}

}
