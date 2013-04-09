package com.parallel.io.async;

import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.percolate.core.peer.HazelcastUtils;

public class FileWatcher {
	
	private WatchService watchDog = null; 
	private File watchDirFile = null;;
	private Path watchDirPath = null;
	private HazelcastInstance clusteredInstance = null;
	private WatchKey registration = null;
	private boolean stop = false;
	
	private static FileWatcher _singleton = null;
	
	public static FileWatcher start(){
		if(_singleton == null){
			synchronized(FileWatcher.class){
				if(_singleton == null){
					_singleton = new FileWatcher();
					Runtime.getRuntime().addShutdownHook(new Thread(){
						@Override
						public void run(){
							_singleton.stop();
						}
					});
				}
			}
		}
		return _singleton;
		
	}
	
	private FileWatcher(){
		init();
	}
	
	private void run(){
		WatchKey event = null;
		System.out.println("Started file watcher ..");
		while(!stop){
			try {
				event = watchDog.take();
			} catch (InterruptedException e) {
				
			}catch (ClosedWatchServiceException e) {
				
				stop = true;
			}
			if(event != null && event.isValid()){
				List<WatchEvent<?>> events = event.pollEvents();
				//clusteredInstance.getQueue("events-bus").offer(events);
				for(WatchEvent<?> what : events){
					System.out.println(
							   "An event was found after file creation of kind " + what.kind() 
							   + ". The event occurred on file " + what.context() + ".");
				}
				event.reset();
			}
		}
		if(event != null && event.isValid())
			event.cancel();
		if(registration != null && registration.isValid())
			registration.cancel();
		if(clusteredInstance != null){
			clusteredInstance.shutdown();
		}
		System.out.println("Stopped file watcher ..");
	}
	
	public void stop(){
		if(watchDog != null){
			try {
				watchDog.close();
			} catch (IOException e) {
				
			}
		}
	}
	
	private static void sleep(int secs){
		try {
			Thread.sleep(secs * 1000);
		} catch (InterruptedException e) {}
	}
	
	private void init(){
		try {
			
			watchDog = FileSystems.getDefault().newWatchService();
			watchDirFile = new File("/home/esutdal/Downloads/nio-2");
			watchDirPath = watchDirFile.toPath();
			
			registration = watchDirPath.register(watchDog, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			sleep(2);
			clusteredInstance = Hazelcast.newHazelcastInstance(HazelcastUtils.getDefaultConfig("FILE_WATCHER_GRP"));
			sleep(1);
			run();
		} catch (IOException e) {
			System.err.println("******** FATAL *********");
			e.printStackTrace();
		}
	}
	
	public static void main(String...strings){
		FileWatcher.start();
	}

}
