package org.ww.spark.example;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class ParsePoint {
	  private static final Pattern SPACE = Pattern.compile(" ");

		//  @Override
		  public Vector call(String line) {
		    String[] tok = SPACE.split(line);
		    double[] point = new double[tok.length];
		    for (int i = 0; i < tok.length; ++i) {
		      point[i] = Double.parseDouble(tok[i]);
		    }
		    return Vectors.dense(point);
		  }
		}