package com.parallel.mapreduce.core.actor;

import java.util.Collection;

import akka.actor.UntypedActor;

import com.parallel.mapreduce.core.IMapper;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.core.message.MapMsg;
import com.parallel.mapreduce.core.message.MappedMsg;

public class MapActor<X, K, V> extends UntypedActor {
	
	private final IMapper<X, K, V> mapFunction;
	
	public MapActor(IMapper<X, K, V> m){
		mapFunction = m;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onReceive(Object arg0) throws Exception {
		if(arg0 instanceof MapMsg){
			MapMsg add = (MapMsg)arg0;
			
			Collection<KeyValue<K, V>> list = mapFunction.mapToKeyValue((X) add.getElement());
			getSender().tell(new MappedMsg<K, V>(list));
			
		}

	}

}
