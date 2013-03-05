package com.percolate.core.processor;

import java.util.HashMap;
import java.util.Map;

public class SignalProcessorFactory {
	
	private static final Map<String, String> processors = new HashMap<String, String>();
	
	static{
		processors.put("com.sun.enterprise.ee.cms.core.MessageSignal", "com.percolate.core.processor.MessageSignalProcessor");
		processors.put("com.sun.enterprise.ee.cms.core.JoinNotificationSignal", "com.percolate.core.processor.JoinSignalProcessor");
		processors.put("com.sun.enterprise.ee.cms.core.PlannedShutdownSignal", "com.percolate.core.processor.MessageSignalProcessor");
		processors.put("com.sun.enterprise.ee.cms.core.FailureNotificationSignal", "com.percolate.core.processor.MessageSignalProcessor");
		processors.put("com.sun.enterprise.ee.cms.core.FailureSuspectedSignal", "com.percolate.core.processor.MessageSignalProcessor");
	}
	
	public static SignalProcessor getProcessor(String signalKey){
		SignalProcessor signalProcessor = null;
		
		String processorClass = processors.get(signalKey);
		
		if(processorClass != null){
			try {
				
				signalProcessor = (SignalProcessor) Class.forName(processorClass).newInstance();
				
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return signalProcessor;
		
	}

}
