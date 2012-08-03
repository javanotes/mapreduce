package com.parallel.common;

import java.io.Serializable;
import java.util.Random;

public class Message<T extends Serializable> implements Serializable {

	public static enum TYPE {REQ, RES, ACK, FAIL}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected T payLoad = null;
	
	private Long msgId = new Random().nextLong();
	
	protected void ID(Long val){
		msgId = val;
	}
	public Long ID(){
		return msgId;
	}
	
	public TYPE msgType = null;

}
