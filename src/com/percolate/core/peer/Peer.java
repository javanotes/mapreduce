package com.percolate.core.peer;

import java.lang.management.ManagementFactory;

import com.parallel.common.Constants;
import com.sun.enterprise.ee.cms.core.CallBack;
import com.sun.enterprise.ee.cms.core.GMSException;
import com.sun.enterprise.ee.cms.core.GMSFactory;
import com.sun.enterprise.ee.cms.core.GroupManagementService;
import com.sun.enterprise.ee.cms.core.Signal;
import com.sun.enterprise.ee.cms.core.GMSConstants.shutdownType;
import com.sun.enterprise.ee.cms.impl.client.FailureNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.FailureSuspectedActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.GroupLeadershipNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.MessageActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;

public class Peer{
	
	protected GroupManagementService gms = null;
	protected String componentName = null;
	protected String gmsGrp = null;
	protected GroupManagementService.MemberType type = null;
	
	protected static String processId = null;
	
	static{
		processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}
	
	protected boolean isMaster = false;
	
	private Peer(String c, String g, GroupManagementService.MemberType t){
		componentName = c;
		gmsGrp = g;
		type = t;
		
		if(gms == null){
					
			gms = (GroupManagementService) GMSFactory.startGMSModule(componentName, 
				gmsGrp, 
				type, 
				null);
			
			//GMSFactory.setGMSEnabledState(gmsGrp, true);
		}
		
		registerListeners();
		
		try {
			gms.join();
		} catch (GMSException e1) {
			
			e1.printStackTrace();
			System.err.println("Could not join to group!! Exiting ..");
			System.exit(1);
		}
		
		try {
			//allow some time to stabilize the GMS system
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			
		}
					
		isMaster = gms.getGroupHandle().getGroupLeader().equals(componentName);
	}
	
	protected void systemShutdown(){
		if(gms != null){
			if(isMaster)
				gms.shutdown(shutdownType.GROUP_SHUTDOWN);
			else
				gms.shutdown(shutdownType.INSTANCE_SHUTDOWN);
		}
	}
		
	private Peer(String peerName){
		this(peerName, Constants.GMS_GRP, GroupManagementService.MemberType.CORE);
		
	}
			
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		final Peer m = new Peer(args[0]);
				
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				m.systemShutdown();
				System.out.println("-- shutdown hook ran --");
			}
		}));
				
		System.out.println("Peer: [" + args[0] + "] started with processId: " + processId + " as " + (m.isMaster ? " MASTER" : " MEMBER .."));
				
	}


	
	private void registerListeners() {
			
		//register to receive notification when a process joins the group
				gms.addActionFactory(new JoinNotificationActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						System.out.println("JoinNotificationSignal received ...." + arg0.getMemberToken());
						
						
					}
				}));
				
				gms.addActionFactory(new GroupLeadershipNotificationActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						System.out.println("GroupLeadershipNotification received ...." + arg0.getMemberToken());
						
					}
				}));


				//register to receive notification when a group member leaves on a planned shutdown
				gms.addActionFactory(new PlannedShutdownActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						System.out.println("PlannedShutdownSignal received from ...." + arg0.getMemberToken());
						if(!isMaster){
							System.err.println("Master node has gone down! Shutting down slave ..");
							System.exit(0);
						}
					}
				}));


				//register to receive notification when a group member is suspected to have failed
				gms.addActionFactory(new FailureSuspectedActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						System.out.println("FailureSuspectedSignal received from ...." + arg0.getMemberToken());
						
					}
				}));


				//register to receive notification when a group member is confirmed failed
				gms.addActionFactory(new FailureNotificationActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						System.out.println("FailureNotificationSignal received from ...." + arg0.getMemberToken());
						if(!isMaster){
							System.err.println("Master node has gone down! Shutting down slave ..");
							System.exit(0);
						}
					}
				}));


				//register to receive notification when this process is selected to perform recovery operations on a failed member's resources
				//gms.addActionFactory(serviceName, new FailureRecoveryActionFactoryImpl(this));


				//register to receive messages from other group members to this registered component

				gms.addActionFactory(new MessageActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						System.out.println("Message notification received");
						
					}
				}), componentName);

		
	}

	

}
