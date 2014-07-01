package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.examples.mllib.JavaLR.ParsePoint;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class SGD {

	  public static void main(String[] args) {
	    if (args.length != 3) {
	      System.err.println("Usage: JavaLR <input_dir> <step_size> <niters>");
	      System.exit(1);
	    }
	    SparkConf sparkConf = new SparkConf().setAppName("JavaLR");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    
	    String filename = args[0];
	    
	    MLUtils.loadLibSVMFile(sc,filename,false);
//	    MLUtils.
	    JavaRDD<String> lines = sc.textFile(args[0]);
	    JavaRDD<LabeledPoint> points = lines.map(new ParsePoint()).cache();
	    double stepSize = Double.parseDouble(args[1]);
	    int iterations = Integer.parseInt(args[2]);

	    // Another way to configure LogisticRegression
	    //
	    // LogisticRegressionWithSGD lr = new LogisticRegressionWithSGD();
	    // lr.optimizer().setNumIterations(iterations)
	    //               .setStepSize(stepSize)
	    //               .setMiniBatchFraction(1.0);
	    // lr.setIntercept(true);
	    // LogisticRegressionModel model = lr.train(points.rdd());

	    LogisticRegressionModel model = LogisticRegressionWithSGD.train(points.rdd(),
	      iterations, stepSize);

	    System.out.print("Final w: " + model.weights());

	    sc.stop();
	  }
}
