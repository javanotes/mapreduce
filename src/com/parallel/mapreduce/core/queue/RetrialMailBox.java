package com.parallel.mapreduce.core.queue;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import scala.Option;
import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.ActorSystem.Settings;
import akka.dispatch.Envelope;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedMessageQueueSemantics;

import com.parallel.mapreduce.core.message.PriorityMsg;
import com.parallel.mapreduce.core.message.PriorityMsg.PRIORITY;
import com.typesafe.config.Config;

/**
 * A custom mailbox implementation that would give an opportunity to "re-try" processing
 * messages, so as to provide a guaranteed delivery system.
 * 
 * What this does is, if a PriorityMsg comes with priority HI, then it puts on head, else on tail by using a double ended queue
 * @author esutdal
 *
 */
public class RetrialMailBox implements MailboxType {
	
	private final Settings settings;
	private final Config config;
	
	private final PriorityGenerator prioGen = new PriorityGenerator() {
		
		@SuppressWarnings("rawtypes")
		@Override
		public int gen(Object message) {
			if (message instanceof PriorityMsg && ((PriorityMsg)message).getPriority() == PRIORITY.HI)
		          return 0; 
		        else if (message instanceof PriorityMsg && ((PriorityMsg)message).getPriority() == PRIORITY.LO)
		          return 2; 
		       
		        else
		          return 1; 
		}
	};
	
	//this constructor is needed by akka reflection
	public RetrialMailBox(Settings settings, Config config){
		this.settings = settings;
		this.config = config;
	}

	@Override
	public MessageQueue create(Option<ActorContext> option) {
		
		return new UnboundedMessageQueueSemantics() {
			
			private final LinkedBlockingDeque<Envelope> mailBox = new LinkedBlockingDeque<Envelope>();
			
			@Override
			public Queue<Envelope> queue() {
				
				return mailBox;
			}
			
			@Override
			public int numberOfMessages() {
				
				return mailBox.size();
			}
			
			@Override
			public boolean hasMessages() {
				
				return !mailBox.isEmpty();
			}
			
			@Override
			public void cleanUp(ActorContext actorcontext, MessageQueue messagequeue) {
				// do what??????
				
			}
			
			@Override
			public void enqueue(ActorRef actorref, Envelope envelope) {
				int priority = prioGen.gen(envelope.message());
				
				if(priority == 0)
					mailBox.offerFirst(envelope);
				else
					mailBox.offerLast(envelope);
				
			}
			
			@Override
			public Envelope dequeue() {
				
				return mailBox.poll();
			}
		};
	}

	

}
