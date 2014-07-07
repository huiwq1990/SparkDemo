package org.ww.spark.example;



import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zarchary
 * Date: 14-1-19
 * Time: 下午6:23
 * To change this template use File | Settings | File Templates.
 */
public final class WordCount {

public static void main(String[] args) throws Exception { 

	JavaSparkContext ctx = new JavaSparkContext("spark://192.168.1.60:7077", "WordCount",
            "/home/hadoop/platform/programs/spark-0.9.1",
            "C://kw.jar");
    JavaRDD<String> lines = ctx.textFile("hdfs://192.168.1.60:9000/test/wordcountinput/wordcountinput", 1);

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        public Iterable<String> call(String s) {
            return Arrays.asList(s.split(" "));
        }
    });
    System.out.println("====words:"+words.count());

    JavaPairRDD<String, Integer> ones = words.map(new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
        }
    });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
        }
    });
    System.out.println("====结果:"+counts.count());


//    List<Tuple2<String, Integer>> output = counts.collect();
//    for (Tuple2 tuple : output) {
//        System.out.println(tuple._1 + ": " + tuple._2);
//    }
    System.exit(0);
}

        }
