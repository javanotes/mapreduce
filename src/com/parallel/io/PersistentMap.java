package com.parallel.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
@Deprecated
public class PersistentMap<K, V> implements java.util.Map<K, V>{
	
	private static String ioFolder = System.getProperty("user.home") + File.separator + "persistance" + File.separator + "maps";
	
	static{
		File f = new File(System.getProperty("user.home") + File.separator + "persistance");
		
		if(f.mkdir()){
			f = new File(ioFolder);
			f.mkdir();
		}
	}
	
	private FileChannel channel = null;
	private MappedByteBuffer fileMap = null;
	/*		
	public static synchronized boolean clear(){
		
		File dir = new File(ioFolder);
		
		for(File file : dir.listFiles()){
			
			if(!file.delete())
				return false;
			
		}
		
		return true;
	}
	private String fileId(K key){
		String id = null;
		byte [] bytes = null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(key);
			bytes = baos.toByteArray();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(oos != null){
				try {
					oos.close();
				} catch (IOException e) {
					
				}
			}
		}
		if(bytes != null){
			id = String.valueOf(Arrays.hashCode(bytes));
		}
		return id;
	}
	
	public boolean hasKey(K key){
		String file = fileId(key);
		if(file != null){
			//System.out.println("file: " + (ioFolder + File.separator + file));
			File f = new File(ioFolder + File.separator + file);
			if(f != null && f.exists() && f.isFile())
				return true;
		}
		return false;
		
	}
	
	public V remove(K key){
		V value = null;
		if(hasKey(key)){
			
			File f = new File(ioFolder + File.separator + fileId(key));
			value = get(key);
			while(f.exists() && !f.delete()){
				try {
					Thread.sleep(100);
					value = get(key);
				} catch (InterruptedException e) {
					
				}
			}
		}
		return value;
		
	}
	public boolean put(K key, V value){
		FileOutputStream fos = null;
		ObjectOutputStream oos = null;
		String file = null;
		if(hasKey(key)){
			file = fileId(key);
			
			try {
				fos = new FileOutputStream(ioFolder + File.separator + file, false);
				FileChannel fcn = fos.getChannel();
				FileLock lock = fcn.lock();
				if(lock.isValid()){
					oos = new ObjectOutputStream(fos);
					oos.writeObject(value);
					oos.flush();
				}
				lock.release();
				return true;
			} catch (FileNotFoundException e) {
				System.err.println(e.getMessage());
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			finally{
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
						
					}
				}
				if(fos != null){
					try {
						fos.close();
					} catch (IOException e) {
						
					}
				}
			}
			
			
		}
		else{
			file = fileId(key);
			
			try {
				fos = new FileOutputStream(new File(ioFolder + File.separator + file), false);
				FileChannel fcn = fos.getChannel();
				FileLock lock = fcn.lock();
				if(lock.isValid()){
					oos = new ObjectOutputStream(fos);
					oos.writeObject(value);
					oos.flush();
				}
				lock.release();
				return true;
			} catch (FileNotFoundException e) {
				System.err.println(e.getMessage());
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			finally{
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
						
					}
				}
				if(fos != null){
					try {
						fos.close();
					} catch (IOException e) {
						
					}
				}
			}
		}
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public V get(K key){
		V value = null;
		if(hasKey(key)){
			String file = fileId(key);
			RandomAccessFile fos = null;
			ObjectInputStream oos = null;
			try {
				fos = new RandomAccessFile(ioFolder + File.separator + file, "rw");
				FileChannel fcn = fos.getChannel();
				FileLock lock = fcn.lock();
				
				byte [] bytes = new byte [(int) fos.length()];//beware!
				if(lock.isValid()){
					fos.readFully(bytes);
				}
				lock.release();
				oos = new ObjectInputStream(new ByteArrayInputStream(bytes));
					
				try {
						value = (V) oos.readObject();
					} catch (ClassNotFoundException e) {
						System.err.println(e.getMessage());
					}
				
			} catch (FileNotFoundException e) {
				System.err.println(e.getMessage());
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			finally{
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
						
					}
				}
				if(fos != null){
					try {
						fos.close();
					} catch (IOException e) {
						
					}
				}
			}
		}
		return value;
		
	}*/
	
	public static void main(String...strings) throws Exception{
		PersistentMap<Integer, String> pm = new PersistentMap<Integer, String>();
		
		/*pm.put(1, "one");
		pm.put(2, "two");*/
		
		pm.put(3, "three");
		System.out.println(pm.get(1));
		
		//PersistentMap.clear();
				
	}
	
	public PersistentMap()throws Exception{
		memoryMap = new HashMap<K, V>();
		_id = Long.toHexString(System.currentTimeMillis());
		
		try {
			channel = new RandomAccessFile(ioFolder + File.separator + _id, "rw").getChannel();
			fileMap = channel.map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
			//fileMap.
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			throw new Exception("Unable to instantiate", e);
		} 
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				
			}
		});
		
	}
	
	private static int hash(int h)
    {
        h ^= h >>> 20 ^ h >>> 12;
        return h ^ h >>> 7 ^ h >>> 4;
    }
	
	private static int indexFor(int h, int length)
    {
        return h & length - 1;
    }
	
	public PersistentMap(String id)throws Exception{
		memoryMap = new HashMap<K, V>();
		_id = id;
		try {
			channel = new RandomAccessFile(ioFolder + File.separator + _id, "rw").getChannel();
			
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			throw new Exception("Unable to instantiate", e);
		} 
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				
			}
		});
	}
	
	public String id(){
		return _id;
	}
	
	
	private final Map<K, V> memoryMap;
	private final String _id;
	
	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean containsKey(Object arg0) {
		
		return false;
	}
	@Override
	public boolean containsValue(Object arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public Set<K> keySet() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void putAll(Map<? extends K, ? extends V> arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V get(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V put(K arg0, V arg1) {
		// TODO Auto-generated method stub
		arg0.hashCode();
		return null;
	}

	@Override
	public V remove(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
