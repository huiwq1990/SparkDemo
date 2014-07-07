package org.ww.spark.example;

public class SparkConfig {
	
	public static String master = "spark://master:7077";
	
	public static String sparkhome = "/home/hadoop/platform/programs/spark-1.0.0/";
	
	public static String projectbasepath = "E:/GitHub/SparkDemo/SparkDemo/";
			
	public static String projectjar = projectbasepath + "target/SparkDemo-0.0.1-SNAPSHOT.jar";
	
	public static String deplibpath = projectbasepath + "target/dependency/";
}
