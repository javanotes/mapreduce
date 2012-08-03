package com.percolate.core.processor;

import com.sun.enterprise.ee.cms.core.JoinNotificationSignal;
import com.sun.enterprise.ee.cms.core.Signal;

public class JoinSignalProcessor extends SignalProcessor {

	@Override
	public void process(Signal signal) {
		JoinNotificationSignal join = (JoinNotificationSignal)signal;
		System.out.println("Component: " +join.getMemberToken()+ " running ..");

	}

}
