package com.parallel.mapreduce.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.parallel.io.FileHandler;
import com.parallel.mapreduce.MapReduce;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.test.functions.CustomerMapper;
import com.parallel.mapreduce.test.functions.CustomerReducer;
import com.parallel.mapreduce.test.functions.VowelCountMapper;
import com.parallel.mapreduce.test.functions.VowelCountReducer;

public class MapReduceTest {
	
	static final int STR_LEN = 100;
	static final long LIST_LEN = 5000000;
	
	private static final char[] alphabets = new char[26];
	private static final Random r = new Random();
	
	private static FileHandler file = null;
    static {
        for (char idx = 0; idx < 26; ++idx)
            alphabets[idx] = (char) ('a' + idx);
        
    }
    
    private final static List<String> header = new ArrayList<String>();
    private static BufferedReader bis = null;
    
    private static String peel(String head){
    	String str = head;
    	if(str.startsWith("\"")){
    		str = str.substring(1);
		}
		if(head.endsWith("\"")){
			str = str.substring(0, str.length() - 1);
						
		}
		
		return str;
    }
    private static void loadFile() throws IOException, FileNotFoundException{
    	try {
    		
			/*bis = new BufferedReader(new FileReader("/home/esutdal/workspace/mapreduce/src/com/parallel/mapreduce/test/bank-full.csv"));
			String [] heads = bis.readLine().split(";");*/
			
			
			file = new FileHandler("/home/esutdal/workspace/mapreduce/src/com/parallel/mapreduce/test/bank-full.csv", 0, 0);
			file.loadFile();
			String [] heads = file.readLine().split(";");
			
			if(heads != null){
				for(String head : heads){
					
					header.add(peel(head));
				}
			}
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
    }
    
    private static void releaseFile(){
    	if(bis != null){
    		try {
				bis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    }
    static int cpp = 1;
    private static Customer getNextCustomer(){
    	Customer c = null;
    	String[] heads = null;
		try {
			String line = null;
			if((line = file.readLine()) != null)
				heads = line.split(";");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(heads != null && heads.length == header.size()){
			cpp++;
			c = new Customer();
			for(String head : header){
				c.set(head, peel(heads[header.indexOf(head)]));
				
			}
		}
		else{
			System.err.println(cpp);
		}
		

		return c;
    	
    }
    
    private static String nextString(){
    	StringBuilder b = new StringBuilder();
    	
    	//500 char length random string
    	for(int i=0; i<STR_LEN; i++)
    		b.append(alphabets[r.nextInt(26)]);
    	
    	return b.toString();
    }
	
	private static List<String> getList(){
		List<String> list = new ArrayList<String>();
		//add random strings
		for(int i=0; i<LIST_LEN; i++){
			list.add(nextString());
		}
		System.out.println("End data generation ..");
		return list;
	}
	
	
	private static void linear(List<String> list, boolean block){
		int a=0,e=0,i=0,o=0,u=0;
		long stime = System.currentTimeMillis();
		for(String s : list){
			if (block) {
				try {
					//mimic some blocking
					Thread.sleep(5);
				} catch (InterruptedException e1) {

				}
			}
			for(char c : s.toCharArray()){
				switch(c){
				case 'a': ++a;break;
				case 'e': ++e;break;
				case 'i': ++i;break;
				case 'o': ++o;break;
				case 'u': ++u;break;
				default: break;
				}
			}
		}
		System.out.println("iterative::Total time: " + (System.currentTimeMillis() - stime) + "ms");
		System.out.println("------- Results<Iterative> --------");
		System.out.println("a:"+a);
		System.out.println("e:"+e);
		System.out.println("i:"+i);
		System.out.println("o:"+o);
		System.out.println("u:"+u);
		System.out.println("------------------------------------");
	}
		
	private static void testCustomer(){
		
		MapReduce<Customer, String, Result> executor = new MapReduce<Customer, String, Result>(new CustomerMapper(), new CustomerReducer());
		//can override the defaults as per need
		//executor.setMappers(450);
		try {
			loadFile();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("file loaded ..");
		Customer c = null;
		System.out.print("Executing Framework");
		long start = System.currentTimeMillis();
		//System.out.println("Start data reading ..");
		while((c = getNextCustomer()) != null){
			executor.add(c);
			
		}
		
		releaseFile();
		
		//System.out.println("End data reading....");
		
		Collection<KeyValue<String, Result>> result = executor.getResult();
		System.out.println("Time taken: " + (System.currentTimeMillis() - start)/1000 + " secs");
		for(KeyValue<String, Result> each : result){
			System.out.println("=========== " + each.key + " ============");
			each.value.print();
			System.out.println("=========================================");
		}
	}
	
	private static void testVowelCount(){
		/**
		 * Getting the count of vowels in 50000 random strings, each of 500 chars length
		 * We have a delay of 1ms applied for every string, to mimic some blocking
		 */
		MapReduce<String, String, Integer> executor = new MapReduce<String, String, Integer>(new VowelCountMapper(), new VowelCountReducer());
				
		//final List<String> list = getList();
		
		//System.out.println(list);
				
		Collection<KeyValue<String, Integer>> result;
		
						
		//Akka based map reduce  
		/*long start = System.currentTimeMillis();
		executor.execute(list);
		result = executor.getResult();
		System.out.println("Time taken: " + (System.currentTimeMillis() - start) + "ms");
		System.out.println("------- Results<Actor> --------");
		for(KeyValue<String, Integer> each : result){
			System.out.println(each.key + ":" + each.value);
		}
		System.out.println("-------------------------------");*/
		
		
		
		System.out.println("begin mapreduce ..");
		long stime = System.currentTimeMillis();
		for(int i=0; i<LIST_LEN; i++){
			executor.add(nextString());
			System.out.println(i);
		}
		System.out.println("End input generation ..");
		result = executor.getResult();
		System.out.println("End mapreduce. Total time: " + (System.currentTimeMillis() - stime) + "ms");
		
		System.out.println("------- Results<Actor> --------");
		for(KeyValue<String, Integer> each : result){
			System.out.println(each.key + ":" + each.value);
		}
		System.out.println("-------------------------------");
		
		//Simple iterative to compare the result
		//linear(list);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
			
		testCustomer();
		
		//testVowelCount();
		

	}

}
