package com.parallel.mapreduce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;

import com.parallel.mapreduce.core.IMapper;
import com.parallel.mapreduce.core.IReducer;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.core.actor.MasterActor;
import com.parallel.mapreduce.core.message.EndProduceMsg;
import com.parallel.mapreduce.core.message.InputCollectionMsg;
import com.parallel.mapreduce.core.message.InputDataMsg;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * A map-reduce framework, based on a single JVM (non distributed) multithreaded execution. Execution of parallel threads in multiple core
 * relies on the underlying thread scheduling performed by Scala/JVM.
 * <p>
 * Please note that NOT ALL problems can be solved in mapreduce pattern. The fundamental for implementing a mapreduce pattern is when:
 * <p>
 * <li> The job can be decomposed into similar sub functions </li>
 * <li> The sub functions are independent of each other</li>
 * <li> The sub function results can be grouped and re-composed <b>associatively (ordering is not important)</b> to get the desired result</li>
 * 
 * 
 *  
 * 
 * <p><p>
 * <h1>What is MapReduce</h1>(Adapted from: <a href="http://silviomassari.wordpress.com/2011/07/06/understanding-mapreduce-mongodb/">Understanding MapReduce</a>)<p>
 * It is an algorithm that can process a very large amount of data in parallel. 
 * <b>It receives three inputs, a source collection, a Map function and a Reduce function. And it will return a new data collection.</b>

	<p><i>Collection MapReduce(Collection source, Function map, Function reduce)</i><p>

	The algorithm is composed by few steps, the first one consists to execute the Map function to each item within the source collection. The Map will return zero or may instances of Key/Value objects.

 	<p><i>ArrayOfKeyValue Map(object itemFromSourceCollection)</i>

	So, we can say that Map�s responsibility is to convert an item from the source collection to zero or many instances of Key/Value objects.
	At the next step , the algorithm will sort all Key/Value instances and it will create new object instances where all values will be grouped by Key.

	The last step will executes the Reduce function by each grouped Key/Value instance.

	<p><i>ItemResult Reduce(KeyWithArrayOfValues item)</i>

 	The Reduce function will return a new item instance that will be included into the result collection.
	<b>The implementation of the Map and Reduce functions are specific for the task that we want to accomplish.</b>

 * 
 * 
 * @author esutdal
 *
 * @param <X> source object type (to be mapped)
 * @param <K> mapped key type
 * @param <V> mapped value type
 */
public class MapReduce<X, K, V> {
	
	private IMapper<X, K, V> map = null;
	private IReducer<K, V> reduce = null;
	
	private ActorRef masterActor = null;
	private int partSize = 999;
	
	private int mappers = 1000;
	
	/**
	 * Set the number of mapper actors. Default 1000
	 * @param mappers
	 */
	public void setMappers(int mappers) {
		this.mappers = mappers;
	}

	/**
	 * Set the number of reducer actors. Default 2000
	 * @param reducers
	 */
	public void setReducers(int reducers) {
		this.reducers = reducers;
	}
	private int reducers = 2000;
	
	/**
	 * We are employing a parallel divide and conquer algorithm for the reduction phase.
	 * Since the total input size is unknown, it is upto the user to set a proper partition size for each division.
	 * 
	 * Do not use too high a value since that will decrease the level of parallelism in the reduction phase.
	 * Defaults to 999. Should ideally be < Integer.MAX_VALUE
	 * @param pSize
	 */
	public void setPartitionSize(int pSize){
		partSize = pSize;
	}
	/*
	 * The facade and the akka system need to coordinate amongst themselves.
	 * Facade will wait till the master actor completes processing. This signalling will
	 * be done through the latch
	 */
	private final CountDownLatch _akLatch = new CountDownLatch(1);
	
	//from Akka docs
	private final static Config akkaConfig = ConfigFactory.parseString(
			
				"mapreduce-dispatcher.type = BalancingDispatcher \n" +
				"mapreduce-dispatcher.executor = fork-join-executor \n" +
				"mapreduce-dispatcher.fork-join-executor.parallelism-min = 8 \n" +
				"mapreduce-dispatcher.fork-join-executor.parallelism-factor = 3.0 \n" +
				"mapreduce-dispatcher.fork-join-executor.parallelism-max = 64 \n" /*+ 
				"prio-dispatcher.mailbox-type = akka.docs.dispatcher.DispatcherDocSpec$PriorityMailBox "*/
	);
			
		
	private final List<KeyValue<K, V>> results = new ArrayList<KeyValue<K, V>>();
	
	/**
	 * Instantiate the facade with a map and reduce function
	 * @param map	
	 * @param reduce
	 */
	public MapReduce(IMapper<X, K, V> map, IReducer<K, V> reduce){
		this.map = map;
		this.reduce = reduce;
		
	}
	
	private volatile boolean isAkkaInit = false;
	
	
	private void initAkka(){
		if(!isAkkaInit){
			akka = ActorSystem.create("actors", akkaConfig);
			
			masterActor = akka.actorOf(new Props(new UntypedActorFactory() {
				
				
				/**
				 * 
				 */
				private static final long serialVersionUID = -2797565506163000340L;

				@Override
				public Actor create() {
					
					return new MasterActor<X, K, V>(map, reduce, results, _akLatch, partSize, mappers, reducers);
				}
			}));
			isAkkaInit = true;
			startWait();
		}
		
	}
	
	private ActorSystem akka = null;
		
	/**
	 * For passing collection of data
	 * @param source
	 */
	public void execute(Collection<X> source){
		if(source != null){
			
			//initialise the Akka actor system
			initAkka();
			
			//start the system by passing message to the master Actor
			masterActor.tell(new InputCollectionMsg<X>(source));
						
		}
		
	}
	
	/**
	 * For passing each data
	 * @param source
	 */
	public void add(X eachData){
		if(eachData != null){
			
			//initialise the Akka actor system
			initAkka();
			
			//start the system by passing message to the master Actor
			masterActor.tell(new InputDataMsg<X>(eachData));
						
		}
		
	}
	
	private AtomicBoolean stop = new AtomicBoolean(false);
	
	private void startWait(){
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(!stop.get()){
					System.out.print(". ");
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						
					}
				}

				System.out.println("\nActor messaging completed ...");
			}
		}).start();
	}
	private void endWait(){
		stop.set(true);
	}
				
	public Collection<KeyValue<K, V>> getResult(){
		masterActor.tell(new EndProduceMsg());
		try {
			//wait while the processing is complete
			
			_akLatch.await();
			endWait();
						
		} catch (InterruptedException e) {
			
		}
		
		return results;
		
	}
	
			
}
