package com.parallel.mapreduce.core.actor;

import scala.Option;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.japi.Function;
import akka.util.Duration;

import com.parallel.mapreduce.core.message.FunctionMessage;
import com.parallel.mapreduce.core.message.PriorityMsg;
import com.parallel.mapreduce.core.message.PriorityMsg.PRIORITY;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Supervisor<X> extends UntypedActor {
	
	private final ActorRef worker;
	
	public Supervisor(){
		super();
		worker = getContext().actorOf(new Props(Worker.class).withDispatcher("prio-dispatcher"));
	}

	private static class Worker<X> extends UntypedActor{
		
		private static int foo = 0;
				
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void onReceive(Object arg0) throws Exception {
			if(arg0 instanceof PriorityMsg){
				
				/**
				 * This is the portion where we have to call the "fault probable" region of code
				 * and handle (re-throw) the exception accordingly to stop/resume/restart processing
				 */
				
				//mimic a failure on every 3rd message receipt
				if(foo++ % 3 == 0){
					throw new Exception("at 3rds");
				}
				System.out.println("Consumed: " + ((PriorityMsg<X>)arg0).getData());
			}
			else if(arg0 instanceof FunctionMessage){
				((FunctionMessage)arg0).f();
			}
			
		}
		
		@Override
		public void preRestart(Throwable reason, Option<Object> message){
			
			/*if(message.get() instanceof FunctionMessage){
				
				@SuppressWarnings("unchecked")
				FunctionMessage<? extends Closure> retry = new FunctionMessage<T extends Closure>();
				
				getSelf().tell(retry);
			}*/
			if(message.get() instanceof PriorityMsg){
				
				@SuppressWarnings("unchecked")
				PriorityMsg<X> retry = new PriorityMsg<X>(((PriorityMsg<X>)message.get()).getData(), PRIORITY.HI);
				
				getSelf().tell(retry);
			}
			super.preRestart(reason, message);
		}
	}
	//end of Worker class

	@Override
	public void onReceive(Object obj) throws Exception {
		//if(obj instanceof PriorityMsg){
			worker.tell(obj, getSelf());
		//}
		
	}
	
	//maximum 10 restarts in a minute
	//TODO make this configurable
	private static SupervisorStrategy strategy = new OneForOneStrategy(10, Duration.parse("1 minute"),
		    new Function<Throwable, Directive>() {
		      @Override
		      public Directive apply(Throwable t) {
		    	  return SupervisorStrategy.restart();
		    	  
		        /*if (t instanceof ArithmeticException) {
		          return resume();
		        } else if (t instanceof NullPointerException) {
		          return restart();
		        } else if (t instanceof IllegalArgumentException) {
		          return stop();
		        } else {
		          return escalate();
		        }*/
		      }
		    });
		 
		@Override
		public SupervisorStrategy supervisorStrategy() {
		  return strategy;
		}
		
		
		
		/**
		 * Test method
		 * @param strings
		 */
		public static void main(String...strings){
			Config akkaConfig = ConfigFactory.parseString(
					"\n"			 +		
					"prio-dispatcher.mailbox-type = com.parallel.mapreduce.core.queue.RetrialMailBox "
					);
			
			ActorSystem as = ActorSystem.create("a", akkaConfig);
			ActorRef start = as.actorOf(new Props(new UntypedActorFactory() {
				
				/**
				 * 
				 */
				private static final long serialVersionUID = -7523869600339500127L;

				@Override
				public Actor create() {
					
					return new Supervisor<String>();
				}
			}));
			
			for(int i=0; i<12; i++){
				start.tell(new PriorityMsg<String>(String.valueOf(i), PRIORITY.N));
			}
			
			as.shutdown();
		}

}
