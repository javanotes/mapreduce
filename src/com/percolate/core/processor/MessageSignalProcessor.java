package com.percolate.core.processor;

import com.sun.enterprise.ee.cms.core.MessageSignal;
import com.sun.enterprise.ee.cms.core.Signal;

public class MessageSignalProcessor extends SignalProcessor {

	@Override
	public void process(Signal signal) {
		MessageSignal msg = (MessageSignal)signal;
		
		System.out.println(
                ":Message Received from:" 
                + msg.getMemberToken()
                + ":[" + msg.toString() + "]");


	}

}
