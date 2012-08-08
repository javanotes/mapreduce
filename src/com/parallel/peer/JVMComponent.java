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
	protected static String componentName = null;
	protected static String gmsGrp = null;
	protected static GroupManagementService.MemberType type = null;
	
	protected static String processId = null;
	
	static{
		processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}
	
	protected static boolean isMaster = false;
	
	protected JVMComponent(String c, String g, GroupManagementService.MemberType t){
		componentName = c;
		gmsGrp = g;
		type = t;
		
		try {
			
			try {
				gms = GMSFactory.getGMSModule(gmsGrp);
			} catch (Exception e) {
				
			}
					
			if(gms == null){
						
				gms = (GroupManagementService) GMSFactory.startGMSModule(componentName, 
					gmsGrp, 
					type, 
					null);
				
				GMSFactory.setGMSEnabledState(gmsGrp, true);
			}
				
			gms.join();
			isMaster = gms.getGroupHandle().getGroupLeader().equals(componentName);
			
		} catch (GMSException e) {
			System.err.println("Could not start component: " + componentName + ". Exiting!!");
			e.printStackTrace();
			System.exit(1);
		}
				
	}
	
	protected void systemShutdown(){
		if(isMaster && gms != null){
			gms.shutdown(shutdownType.GROUP_SHUTDOWN);
		}
	}
	
	private String signalKey(Signal signal){
		return signal.getClass().getSuperclass().getCanonicalName() + gmsGrp + componentName;
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
