package com.parallel.mapreduce.core.queue;

import scala.Option;
import akka.actor.ActorContext;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;

public class DurableMailBox implements MailboxType {

	@Override
	public MessageQueue create(Option<ActorContext> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
