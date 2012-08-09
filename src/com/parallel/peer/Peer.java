package com.parallel.peer;

import com.parallel.common.Constants;
import com.sun.enterprise.ee.cms.core.CallBack;
import com.sun.enterprise.ee.cms.core.GroupManagementService;
import com.sun.enterprise.ee.cms.core.Signal;
import com.sun.enterprise.ee.cms.impl.client.FailureNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.FailureSuspectedActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.GroupLeadershipNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.MessageActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;

public class Peer extends JVMComponent{
		
	private Peer(String peerName){
		super(peerName, Constants.GMS_GRP, GroupManagementService.MemberType.CORE);
		
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
				
		System.out.println("Peer: [" + args[0] + "] started with processId: " + processId + " as " + (isMaster ? " MASTER" : " MEMBER .."));
				
	}


	@Override
	protected void registerListeners() {
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
