package com.parallel.mapreduce.core.message;

public class FunctionMessage<X extends Closure>{
	
	private final X function;
	public static enum PRIORITY {HI, LO, N}
	private final PRIORITY priority;
	
	public FunctionMessage(X x, PRIORITY p){
		function = x;
		priority = p;
	}
	
	public X get(){
		return function;
	}
	
	public void f(){
		function.doFunction();
	}
	
	public PRIORITY getPriority() {
		return priority;
	}

}
