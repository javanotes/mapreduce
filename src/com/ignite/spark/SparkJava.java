package com.ignite.spark;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

public class SparkJava {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Spark.get(new Route("/hello") {
			@Override
			public Object handle(Request request, Response response) {
				return "<i>Hello World!</i>";
			}
		});

	}

}
