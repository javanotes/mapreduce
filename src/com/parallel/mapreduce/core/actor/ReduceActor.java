package com.parallel.mapreduce.core.actor;

import akka.actor.UntypedActor;

import com.parallel.mapreduce.core.IReducer;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.core.message.ReduceMsg;
import com.parallel.mapreduce.core.message.ReducedMsg;

public class ReduceActor<K, V> extends UntypedActor {
	
	private final IReducer<K, V> reduceFunction;
	
	public ReduceActor(IReducer<K, V> r){
		reduceFunction = r;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onReceive(Object arg0) throws Exception {
		if(arg0 instanceof ReduceMsg){
			ReduceMsg reduce = (ReduceMsg)arg0;
			
			KeyValue<K, V> reduced = (KeyValue<K, V>) reduceFunction.reduceForKey((K) reduce.getKey(), reduce.getElement());
			
			getSender().tell(new ReducedMsg<K, V>(reduced));
			
		}

	}

}
