package com.parallel.mapreduce.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Result {

	public long totalBalance = 0;
	public int outComeSuccess = 0;
	public int count = 0;
	
	public Map<String, Integer> education = new HashMap<String, Integer>();
	
	public long getTotalBalance() {
		return totalBalance;
	}
	
	public void print(){
		System.out.println("totalBalance: " + totalBalance);
		System.out.println("outComeSuccess: " + outComeSuccess);
		System.out.println("count: " + count);
		for(Entry<String, Integer> each : education.entrySet()){
			System.out.println("Education Type: " + each.getKey() + "\t" + "Count: " + each.getValue());
		}
	}
	
	public Result(long tBal, int oSucc, String eduTyp){
		totalBalance += tBal;
		outComeSuccess += oSucc;
		
		education.put(eduTyp, 1);
	}
	
	public Result(){
		
	}

	public int getOutComeSuccess() {
		return outComeSuccess;
	}

	public Map<String, Integer> getEducation() {
		return education;
	}

	
	
	
}
