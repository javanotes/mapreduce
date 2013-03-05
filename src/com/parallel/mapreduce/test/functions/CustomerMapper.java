package com.parallel.mapreduce.test.functions;

import java.util.ArrayList;
import java.util.List;

import com.parallel.mapreduce.core.IMapper;
import com.parallel.mapreduce.core.KeyValue;
import com.parallel.mapreduce.test.Customer;
import com.parallel.mapreduce.test.Result;

public class CustomerMapper implements IMapper<Customer, String, Result> {
	
	@Override
	public List<KeyValue<String, Result>> mapToKeyValue(Customer each) {
		
		List<KeyValue<String, Result>> keyVals = new ArrayList<KeyValue<String,Result>>();
		
		long balance = Long.parseLong(each.get("balance"));
		int outcome = "success".equals(each.get("poutcome")) ? 1 : 0;
		Result oneSuch = new Result(balance, outcome, each.get("education"));
		
		//single management guys
		if("management".equals(each.get("job")) && "single".equals(each.get("marital"))){
			keyVals.add(new KeyValue<String, Result>("ManagementSingles", oneSuch));
		}
		
		//management guys with no tertiary education
		if("management".equals(each.get("job")) && !"tertiary".equals(each.get("education"))){
			keyVals.add(new KeyValue<String, Result>("ManagementNonTertiary", oneSuch));
		}
		
		//married entrepreneurs
		if("entrepreneur".equals(each.get("job")) && "married".equals(each.get("marital"))){
			keyVals.add(new KeyValue<String, Result>("EntrepreneurMarried", oneSuch));
		}
		
		//all admins
		if("admin.".equals(each.get("job"))){
			keyVals.add(new KeyValue<String, Result>("Admin", oneSuch));
		}
		
		
		return keyVals;
	}

}
