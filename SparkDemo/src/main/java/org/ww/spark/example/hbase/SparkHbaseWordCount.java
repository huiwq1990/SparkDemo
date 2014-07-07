package org.ww.spark.example.hbase;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkHbaseWordCount{
	private static final Pattern SPACE = Pattern.compile(" ");
	public Log log = LogFactory.getLog(SparkHbaseWordCount.class);

	// 声明静态配置
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.1.61");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}
		
	public static void main(String[] args) throws Exception {

//		Logger.getRootLogger().setLevel(Level.WARN);

		// 需要读取的hbase表名
		String tableName = "sparkhbasewordcount";

		Scan scan = new Scan();
		// scan.setStartRow(Bytes.toBytes("yangjiang 2013-07-20"));
		// scan.setStopRow(Bytes.toBytes("yangjiang 2013-07-22"));
		scan.addFamily(Bytes.toBytes("cf"));
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("content"));
		
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

//		printTable(tableName,scan);

		SparkConf sparkConf = new SparkConf().setAppName("SparkHbaseWordCount");
		sparkConf.setMaster("spark://master:7077");
		sparkConf.setSparkHome("/home/hadoop/platform/programs/spark-1.0.0/");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// 获得hbase查询结果Result
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc
				.newAPIHadoopRDD(conf, TableInputFormat.class,
						ImmutableBytesWritable.class, Result.class);

		long result = hBaseRDD.count();
		System.out.println("number lines : "+result);

		
		JavaRDD<String> words = hBaseRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
			@Override
			public Iterable<String> call(
					Tuple2<ImmutableBytesWritable, Result> arg0)
					throws Exception {
				byte[] o = arg0._2()
						.getValue(Bytes.toBytes("cf"),
								Bytes.toBytes("content"));
				if (o != null) {
					// Integer i = (int) Bytes.toDouble(o);
					String line = Bytes.toString(o);
					String[] tok = SPACE.split(line);
					
					return Arrays.asList(tok);
				}
				return null;
			}

		});
		
		JavaPairRDD<String, Integer> ones = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String word) {
						return new Tuple2<String, Integer>(word, 1);
					}
				});

		// 数据累加
		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});


		// 打印出最终结果
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2 tuple : output) {
			System.out.println(tuple._1 + ": " + tuple._2);
		}


	}

	public static void printTable(String tableName,Scan scan) throws Exception{
		HTable table = new HTable(conf, tableName);
		System.out.println("sssss");
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			for (KeyValue kv : r.raw()) {
				System.out
						.println(String
								.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
										Bytes.toString(kv.getRow()),
										Bytes.toString(kv.getFamily()),
										Bytes.toString(kv.getQualifier()),
										Bytes.toString(kv.getValue()),
										kv.getTimestamp()));
			}
		}

		rs.close();
		table.close();
	}

}
