package com.parallel.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serializer {
	
	/**
	 * 
	 * @param bArr
	 * @return
	 */
	public static Serializable getObject(byte [] bArr){
		Serializable obj = null;
		ObjectInputStream ois = null;
		if(bArr != null){
			
			try {
				ois = new ObjectInputStream(new ByteArrayInputStream(bArr));
				obj = (Serializable) ois.readObject();
			} catch (IOException e) {
				System.err.println(e.getMessage());
			} catch (ClassNotFoundException e) {
				System.err.println(e.getMessage());
			}
			finally{
				if(ois != null){
					try {
						ois.close();
					} catch (IOException e) {}
				}
			}
			
		}
		
		return obj;
	}

	/**
	 * 
	 * @param object
	 * @return
	 */
	public static byte[] getBytes(Serializable object){
		byte[] bytes = null;
		
		ObjectOutputStream oos = null;
		
		if(object != null){
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				oos = new ObjectOutputStream(baos);
				oos.writeObject(object);
				oos.flush();
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
			}
			
			bytes = baos.toByteArray();
		}
		
		return bytes;
	}
}
