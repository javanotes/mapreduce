package com.parallel.mapreduce.test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Customer {

	@SuppressWarnings("unused")
	private String age,job,marital,education,dflt,balance, housing, loan, contact, day, month, duration, campaign, pdays,previous, poutcome, y;

	private long _id;
	private static final Map<String, Field> props = new HashMap<String, Field>();
	
	public Customer(){
		_id = System.nanoTime();
	}
	
	@Override
	public boolean equals(Object obj){
		if(!(obj instanceof Customer))
			return false;
		if(obj == this)
			return true;
		else{
			return ((Customer)obj)._id == this._id;
		}
		
	}
	
	@Override
	public int hashCode(){
		return new Long(_id).hashCode();
		
	}
	
	public void print(){
		System.out.println("--------- Begin ----------");
		for(Entry<String, Field> entry : props.entrySet()){
			try {
				System.out.println(entry.getKey() + " : " + entry.getValue().get(this));
			} catch (Exception e) {
				System.err.println(e.getMessage());
			} 
		}
		System.out.println("---------- End -----------");
	}
	
	private static Field getField(String name){
		Field f = null;
		if(props.containsKey(name)){
			f = props.get(name);
		}
		else{
			synchronized (props) {
				if (!props.containsKey(name)) {
					try {

						f = Customer.class.getDeclaredField(name
								.equals("default") ? "dflt" : name);
						props.put(name, f);
					} catch (SecurityException e) {

					} catch (NoSuchFieldException e) {
						System.err.println(e.getMessage());
					}
				}
			}
		}
		
		return f;
	}
	
	/**
	 * Get the property value
	 * @param prop
	 * @return
	 */
	public String get(String prop){
		String value = null;
		Field f = getField(prop);
		if(f != null){
			try {
				value = (String) f.get(this);
			} catch (IllegalArgumentException e) {
				
			} catch (IllegalAccessException e) {
				
			}
		}
		return value;
		
	}
	
	/**
	 * Set the property value
	 * @param prop
	 * @param value
	 */
	public void set(String prop, String value){
		
		Field f = getField(prop);
		if(f != null){
			try {
				f.set(this, value);
			} catch (IllegalArgumentException e) {
				
			} catch (IllegalAccessException e) {
				
			} 
			 
		}
		 
	}
		
	
}
