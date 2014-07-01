package hg.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestKmeans {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 String logFile = "hdfs://master:9000/test/spark/test.txt"; // Should be some file on your system
		    SparkConf conf = new SparkConf().setAppName("Simple Application");

		    JavaSparkContext sc = new JavaSparkContext(conf);
		    JavaRDD<String> logData = sc.textFile(logFile).cache();
	
				parsedData = data.map(lambda line: array([float(x) for x in line.strip("\n").split(",")]))
				model = KMeans.train(parsedData,5,initializationMode="random")
	
	}

}
