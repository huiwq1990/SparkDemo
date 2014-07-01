/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.mllib;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import scala.Tuple2;

/**
 * Logistic regression based classification using ML Lib.
 */
public final class JavaLR {

  static class ParsePoint implements Function<String, LabeledPoint> {
    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public LabeledPoint call(String line) {
      String[] parts = SPACE.split(line);
      double y = Double.parseDouble(parts[0]);
//      String[] tok = SPACE.split(parts[1]);
      double[] x = new double[parts.length];
      for (int i = 1; i < parts.length; ++i) {
        x[i] = Math.log(Double.parseDouble(parts[i]));
      }
      return new LabeledPoint(y, Vectors.dense(x));
    }
  }

  
  
  static class ParseVectorPoint implements Function<String, Vector> {
	    private static final Pattern COMMA = Pattern.compile(",");
	    private static final Pattern SPACE = Pattern.compile(" ");

	    @Override
	    public Vector call(String line) {
	      String[] parts = SPACE.split(line);
	      double y = Double.parseDouble(parts[0]);
//	      String[] tok = SPACE.split(parts[1]);
	      
	      double[] x = new double[parts.length];
	      for (int i = 1; i < parts.length; ++i) {
	        x[i] = Math.log(Double.parseDouble(parts[i]));
	      }
	      return  Vectors.dense(x);
	    }
	  }
  
  
  
  public static void main(String[] args) {
//    if (args.length != 3) {
//      System.err.println("Usage: JavaLR <input_dir> <step_size> <niters>");
//      System.exit(1);
//    }
    SparkConf sparkConf = new SparkConf().setAppName("JavaLR");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> trainStrSet = sc.textFile(args[0]);
    JavaRDD<LabeledPoint> trainSet = trainStrSet.map(new ParsePoint()).cache();
    
    
    JavaRDD<String> testStrSet = sc.textFile(args[0]);
    JavaRDD<Vector> testSet= testStrSet.map(new ParseVectorPoint()).cache();
//    val splits = points.s
//    	    val training = splits(0).cache()
//    	    val test = sp/lits(1).cache()
//
//    	    val numTraining = training.count()
//    	    val numTest = test.count()
//    	    println(s"Training: $numTraining, test: $numTest.")
System.out.println(trainSet.count());
//    double stepSize = Double.parseDouble(args[1]);
//    int iterations = Integer.parseInt(args[2]);

    // Another way to configure LogisticRegression
    //
    // LogisticRegressionWithSGD lr = new LogisticRegressionWithSGD();
    // lr.optimizer().setNumIterations(iterations)
    //               .setStepSize(stepSize)
    //               .setMiniBatchFraction(1.0);
    // lr.setIntercept(true);
    // LogisticRegressionModel model = lr.train(points.rdd());

//    LogisticRegressionModel model = LogisticRegressionWithSGD.train(points.rdd(),
//      iterations, stepSize);
    
    
    LinearRegressionWithSGD algorithm = new LinearRegressionWithSGD();
    algorithm.optimizer()
      .setNumIterations(100)
      .setStepSize(0.1)
      .setUpdater( new SquaredL2Updater())
      .setRegParam(0.1);
//    Logger.getRootLogger.setLevel(Level.WARN);
    LinearRegressionModel model = algorithm.run(trainSet.rdd());
    



    JavaRDD<Double> prediction = model.predict(testSet);
    
    List<Double> p = prediction.toArray();
    
    for(Double b : p){
    	System.out.println(b);
    }
    
    
//    prediction.zip(other)
//    	    val prediction = model.predict(testSet.rdd().);
//    	    val predictionAndLabel = prediction.zip(test.map(_.label))
//
//    	    val loss = predictionAndLabel.map { case (p, l) =>
//    	      val err = p - l
//    	      err * err
//    	    }.reduce(_ + _)
//    	    val rmse = math.sqrt(loss / numTest)

    sc.stop();
  }
}
