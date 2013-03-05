package com.parallel.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * A class to act as a read only, non-scrollable file container in an off-heap storage
 * @author esutdal
 *
 */
public class FileHandler {

	private MappedByteBuffer fileBuff = null;
	
	private int offset = 100;
	private int index = 0;
	private String filePath = null;
	
	/**
	 * 
	 * @param filepath
	 * @param start - 0 to start from beginning
	 * @param size - 0 to read the entire file
	 */
	public FileHandler(String filepath, int start, int size){
		filePath = filepath;
		
		index = start;
		offset = size;
		
	}
	
	public String readLine(){
		StringBuffer input = new StringBuffer();
		
		int c = -1;
		boolean eol = false;

		while (fileBuff.hasRemaining() && !eol) {
		    switch (c = fileBuff.get()) {
			    case -1:
			    case '\n':
				eol = true;
				break;
			    case '\r':
				eol = true;
				break;
			    default:
				input.append((char)c);
				break;
		    }
		}

		if ((c == -1) && (input.length() == 0)) {
		    return null;
		}
		
		return input.toString();
	}	
	
	@Override
	public void finalize(){
		fileBuff = null;
	}
	public void loadFile() throws FileNotFoundException, IOException{
		
		FileChannel channel = null;
		FileInputStream file = null;
		try {
			file = new FileInputStream(filePath);
			int bytes = offset == 0 ? file.available() : offset;
			
			channel = file.getChannel();
			fileBuff = channel.map(MapMode.READ_ONLY, index, bytes);
			System.out.println("Bytes read: " +bytes);
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			
			e.printStackTrace();
			throw e;
		}
		finally{
			
			if(file != null){
				file.close();
			}
		}
		
		
	}
	
}
