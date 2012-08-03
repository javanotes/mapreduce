package com.parallel.peer;

import java.lang.management.ManagementFactory;

import com.percolate.core.processor.SignalProcessor;
import com.percolate.core.processor.SignalProcessorFactory;
import com.sun.enterprise.ee.cms.core.CallBack;
import com.sun.enterprise.ee.cms.core.GMSConstants.shutdownType;
import com.sun.enterprise.ee.cms.core.GMSException;
import com.sun.enterprise.ee.cms.core.GMSFactory;
import com.sun.enterprise.ee.cms.core.GroupManagementService;
import com.sun.enterprise.ee.cms.core.Signal;
import com.sun.enterprise.ee.cms.core.SignalAcquireException;
import com.sun.enterprise.ee.cms.core.SignalReleaseException;

abstract class JVMComponent implements CallBack {

	protected static GroupManagementService gms = null;
	protected static String component = null;
	protected static String gmsGrp = null;
	protected static GroupManagementService.MemberType type = null;
	
	protected static String processId = null;
	
	static{
		processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}
	
	protected static boolean isMaster = false;
	
	protected JVMComponent(String c, String g, GroupManagementService.MemberType t){
		component = c;
		gmsGrp = g;
		type = t;
		
		try {
			
					
			if(gms == null){
						
				gms = (GroupManagementService) GMSFactory.startGMSModule(component, 
					gmsGrp, 
					type, 
					null);
			}
				
			gms.join();
			isMaster = gms.getGroupHandle().getGroupLeader().equals(component);
			
		} catch (GMSException e) {
			System.err.println("Could not start component: " + component + ". Exiting!!");
			e.printStackTrace();
			System.exit(1);
		}
		
		registerShutdownHook();
	}
	
	private void registerShutdownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				if(gms != null){
					if(isMaster){
						gms.shutdown(shutdownType.GROUP_SHUTDOWN);
						System.out.println("Percolate system stopped ...");
					}
					else{
						gms.shutdown(shutdownType.INSTANCE_SHUTDOWN);
						System.out.println("Component " + component + " stopped ...");
					}
				}
				
			}
		}));
	}
	
	private String signalKey(Signal signal){
		return signal.getClass().getSuperclass().getCanonicalName() + gmsGrp + component;
	}
	
	@Override
	public void processNotification(Signal signal) {
		try {
			System.out.println("Signal: " + signal.toString());
			signal.acquire();
			
			SignalProcessor processor = SignalProcessorFactory.getProcessor(signalKey(signal));
			if(processor != null)
				processor.process(signal);
			
			signal.release();
		} catch (SignalAcquireException e) {
			
			e.printStackTrace();
		} catch (SignalReleaseException e) {
			
			e.printStackTrace();
		}

	}
	
	abstract protected void registerListeners();

}
