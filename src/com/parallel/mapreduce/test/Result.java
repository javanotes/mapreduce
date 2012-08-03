package com.parallel.mapreduce.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Result {

	private long totalBalance = 0;
	private int outComeSuccess = 0;
	private int count = 0;
	
	private Map<String, Integer> education = new HashMap<String, Integer>();
	
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

	/**
	 * The association logic
	 * @param other
	 */
	public void associate(Result other){
		if(other != null){
			this.totalBalance += other.totalBalance;
			this.outComeSuccess += other.outComeSuccess;
			this.count += other.count == 0 ? 1 : other.count;
			for(Entry<String, Integer> each : other.education.entrySet()){
				if(education.containsKey(each.getKey())){
					education.put(each.getKey(), education.get(each.getKey()) + each.getValue());
				}
				else{
					education.put(each.getKey(), each.getValue());
				}
			}
		}
	}
	
	
}
