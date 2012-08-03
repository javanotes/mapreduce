package com.parallel.mapreduce.core.actor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.SmallestMailboxRouter;

import com.parallel.mapreduce.core.IMapper;
import com.parallel.mapreduce.core.IReducer;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.core.actor.message.EndProduceMsg;
import com.parallel.mapreduce.core.actor.message.InputCollectionMsg;
import com.parallel.mapreduce.core.actor.message.InputDataMsg;
import com.parallel.mapreduce.core.actor.message.MapMsg;
import com.parallel.mapreduce.core.actor.message.MappedMsg;
import com.parallel.mapreduce.core.actor.message.ReduceMsg;
import com.parallel.mapreduce.core.actor.message.ReducedMsg;

public class MasterActor<X, K, V> extends UntypedActor {
	
	private final ActorRef mapRouter;
	private final ActorRef reduceRouter;
			
	/*
	 * We are employing a parallel divide and conquer algorithm for the reduction phase.
	 * Since the total input size is unknown, it is upto the user to set a proper partition size for each division.
	 *
	 */
	private int partitionSize;
				
	private final Collection<KeyValue<K, V>> finalResult;
	
	
	private final CountDownLatch latch;
		
	public MasterActor(
			final IMapper<X, K, V> mapFunction, 
			final IReducer<K, V> reduceFunction, 
			Collection<KeyValue<K, V>> y, 
			CountDownLatch latch,
			int partSize,
			int noOfMapper,
			int noOfReducer
			) {
		
		
		finalResult = y;
		this.latch = latch;
		partitionSize = partSize;		
		
		/*
		 * Creating sort of an 'Actor pool'. These are the "supervised" actors of the master actor. 
		 * A SmallestMailboxRouter would try to send a message to the routee with least pending messages.			
		 */
		
		mapRouter = getContext().actorOf(new Props(new UntypedActorFactory() {
						
			private static final long serialVersionUID = -2523958536560385L;

			@Override
			public Actor create() {
				
				return new MapActor<X, K, V>(mapFunction);
			}
		}).withRouter(new SmallestMailboxRouter(noOfMapper)));
		
		reduceRouter = getContext().actorOf(new Props(new UntypedActorFactory() {
			
			
			private static final long serialVersionUID = 345232609573669981L;

			@Override
			public Actor create() {
				
				return new ReduceActor<K, V>(reduceFunction);
			}
		}).withRouter(new SmallestMailboxRouter(noOfReducer)));
		
		
	}
	
	private boolean inputEnded = false;
	
	long start = 0;
	
	/*
	 * This counter keeps track of the recursion stop condition!
	 */
	private long counter = 0;
	
	/**
	 * Pass all elements from the input data collection through the map function
	 * @param source
	 */
	private void map(X each){
				
		//send the input to the map function 
		mapRouter.tell(new MapMsg<X, K, V>(each), getSelf());
						
		
	}
	
	
	
	/**
	 * Pass all elements from the input data collection through the map function
	 * @param source
	 */
	private void map(Collection<X> source){
				
		for(X each : source){
						
			//send the input to the map function. higher order function in Java?
			mapRouter.tell(new MapMsg<X, K, V>(each), getSelf());
						
		}
		
	}
	
	private boolean stopRecursion(){
		Set<K> set = new java.util.HashSet<K>();
		
		/*
		 * It is IMPORTANT that the Key should have equals()/hashCode() implemented accordingly, if it is a composite business object
		 * The recursion stop condition is evaluated by comparing the keys.
		 */
		boolean stop = true;
		for(KeyValue<K, V> each : finalResult){
			if(!set.add(each.key)){
				stop = false;
				break;
			}
		}
		
		return stop;
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onReceive(Object arg0) throws Exception {
		
		//this is a mapped data received from the map function
		if(arg0 instanceof MappedMsg){
			MappedMsg mapped = (MappedMsg)arg0;
						
			//pass it to reduce function
			aggregate(mapped.getList());
						
		}
		
		
		//this is the input collection sent from client to be mapped and reduced
		else if(arg0 instanceof InputCollectionMsg){
			InputCollectionMsg input = (InputCollectionMsg)arg0;
			
			//pass the input through map function
			map(input.getSource());
		}
		
		
		
		//this is the input data sent from client to be mapped and reduced
		else if(arg0 instanceof InputDataMsg){
			InputDataMsg input = (InputDataMsg)arg0;
			
			//pass the input through map function
			map((X) input.getData());
						
		}
		
		
		
		//this is a reduced data received from the reduce function
		else if(arg0 instanceof ReducedMsg){
			ReducedMsg reduced = (ReducedMsg)arg0;
			counter--;
						
			finalResult.add((KeyValue<K, V>) reduced.getReduced());
			
			//end of one pass			
			if(counter == 0){
				//check if only distinct keys remain in finalResult
				if(stopRecursion()){
					//make sure input generation has ended
					if(inputEnded)
						//no more messages will be consumed
						stop();
				}
				else{
					//recursive call to reduce list further
					aggregate(finalResult);
					//remove the last intermediate results
					finalResult.clear();
				}
			}
			//can configure this partition from input
			//this will tell us how much do we want to take in each division
			else if(finalResult.size() >= partitionSize){
				//recursive call to reduce list further				
				aggregate(finalResult);
				//remove the last intermediate results
				finalResult.clear();
			}
												
									
		}
		
		else if(arg0 instanceof EndProduceMsg){
			inputEnded = true;
			
			//check if only distinct keys remain in finalResult
			if(stopRecursion()){
				//no more messages will be consumed
				stop();
			}
			else{
				//recursive call to reduce list further
				aggregate(finalResult);
				finalResult.clear();
			}
		}
		else{
			unhandled(arg0);
		}

	}
				
	/**
	 * Stop the Akka system
	 */
	private void stop(){
		getContext().stop(getSelf());
		getContext().system().shutdown();
		
	}
	
	/**
	 * Callback method invoked by akka system
	 */
	@Override
	public void postStop(){
		latch.countDown();
	}
				
	/**
	 * Sort the mapped function result by key and combine each's value/s
	 * @param eachResult
	 */
	private void aggregate( Collection<KeyValue<K, V>> items){
				
		Map<K, Collection<V>> combine = new HashMap<K, Collection<V>>();
		for(KeyValue<K, V> keyVal : items){
			//sort by key and combine the values
			if(combine.containsKey(keyVal.key)){
				combine.get(keyVal.key).add(keyVal.value);
			}
			else{
				combine.put(keyVal.key, new ArrayList<V>());
				combine.get(keyVal.key).add(keyVal.value);
			}
			
		}
		
		for(Entry<K, Collection<V>> entry : combine.entrySet()){
			
			//now reduce the aggregations for each key
			reduceRouter.tell(new ReduceMsg<K, V>(entry.getValue(), entry.getKey()), getSelf());
			counter++;			
		}
		
			
	}

}
