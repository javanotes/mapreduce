package com.percolate.core.peer;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.QueueConfig;

public class HazelcastUtils {
	
	public static final String DEFAULT_QUEUE = "DEFAULT_QUEUE";
	
	private HazelcastUtils(){}
	
	public static Config getDefaultConfig(String groupName){
		QueueConfig queue = new QueueConfig();
		queue.setName(DEFAULT_QUEUE);
		queue.setMaxSizePerJVM(1000);
		
		GroupConfig group = new GroupConfig(groupName);
		
		Config config = new Config();
		config.addQueueConfig(queue);
		config.setGroupConfig(group);
		
		return config;
		
	}

}
