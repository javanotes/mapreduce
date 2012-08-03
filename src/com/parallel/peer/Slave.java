package com.parallel.peer;

import com.parallel.common.Constants;
import com.sun.enterprise.ee.cms.core.CallBack;
import com.sun.enterprise.ee.cms.core.GMSConstants.shutdownType;
import com.sun.enterprise.ee.cms.core.GMSException;
import com.sun.enterprise.ee.cms.core.GroupManagementService;
import com.sun.enterprise.ee.cms.core.Signal;
import com.sun.enterprise.ee.cms.impl.client.MessageActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;

public class Slave extends JVMComponent {

	
	private Slave(String identity){
		super(identity, Constants.GMS_GRP, GroupManagementService.MemberType.CORE);
	}
	
	public static void main(String[] args) {
		Slave s = new Slave(args[0]);
		s.registerListeners();
		System.out.println("registered slave");
		//s.registerShutdownHook(false);
		try {
			gms.getGroupHandle().sendMessage(Constants.CORE_MGR, "This is slave".getBytes());
		} catch (GMSException e) {
			
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(1);

	}

	@Override
	protected void registerListeners() {
		
				//register to receive messages from other group members to this registered component

				gms.addActionFactory(new MessageActionFactoryImpl(new CallBack() {
					
					@Override
					public void processNotification(Signal arg0) {
						
						System.exit(0);
						
					}
				}), component);
								

		
	}

}
