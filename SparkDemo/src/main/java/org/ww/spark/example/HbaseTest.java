package org.ww.spark.example;

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



import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * <pre>
 *
 * spark hbase 测试
 *
 *  Created with IntelliJ IDEA.
 * User: zhangdonghao
 * Date: 14-1-26
 * Time: 上午9:24
 * To change this template use File | Settings | File Templates.
 * </pre>
 *
 * @author zhangdonghao
 */
public class HbaseTest implements Serializable {

    public Log log = LogFactory.getLog(HbaseTest.class);
    
	// 声明静态配置
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"192.168.1.61");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

    /**
     * 将scan编码，该方法copy自 org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
     *
     * @param scan
     * @return
     * @throws IOException
     */
    public static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
 }

    public void start() {
        //初始化sparkContext，这里必须在jars参数里面放上Hbase的jar，
        // 否则会报unread block data异常
    	
        try {
        
//        JavaSparkContext sc = new JavaSparkContext("spark://master:7077", "hbaseTest",
//                "/home/hadoop/software/spark-0.8.1",
//                new String[]{"target/ndspark.jar", "target\\dependency\\hbase-0.94.6.jar"});

        //使用HBaseConfiguration.create()生成Configuration
        // 必须在项目classpath下放上hadoop以及hbase的配置文件。
//        Configuration conf = HBaseConfiguration.create();
//    	conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum",
//				"192.168.1.61");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
        //设置查询条件，这里值返回用户的等级
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("yangjiang 2013-07-21"));
        scan.setStopRow(Bytes.toBytes("yangjiang 2013-07-22"));
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tempmax"));
System.out.println("ss");
   
            //需要读取的hbase表名
            String tableName = "t_weather";
//            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            conf.set(TableInputFormat.SCAN, convertScanToString(scan));
//            conf.get
            
            HTable table = new HTable(conf, tableName);
System.out.println("sssss");
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.raw()) {
                    System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.", 
                            Bytes.toString(kv.getRow()), 
                            Bytes.toString(kv.getFamily()), 
                            Bytes.toString(kv.getQualifier()), 
                            Bytes.toString(kv.getValue()),
                            kv.getTimestamp()));
                }
            }
            
            rs.close();
//            SparkConf sparkConf = new SparkConf().setAppName("SparkHbase");
//            JavaSparkContext sc = new JavaSparkContext(sparkConf);
//            
//            //获得hbase查询结果Result
//            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
//                    TableInputFormat.class, ImmutableBytesWritable.class,
//                    Result.class);
            
            
//            long result = hBaseRDD.count();
//System.out.println(result);
//            从result中取出用户的等级，并且每一个算一次
//            JavaPairRDD<Integer, Integer> levels = hBaseRDD.map(
//                    new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Integer>() {
//                        @Override
//                        public Tuple2<Integer, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2)
//                                throws Exception {
//                            byte[] o = immutableBytesWritableResultTuple2._2().getValue(
//                                    Bytes.toBytes("info"), Bytes.toBytes("levelCode"));
//                            if (o != null) {
//                                return new Tuple2<Integer, Integer>(Bytes.toInt(o), 1);
//                            }
//                            return null;
//                        }
//                    });
//
//            //数据累加
//            JavaPairRDD<Integer, Integer> counts = levels.reduceByKey(new Function2<Integer, Integer, Integer>() {
//                public Integer call(Integer i1, Integer i2) {
//                    return i1 + i2;
//                }
//            });
//     
            
            //打印出最终结果
//            List<Tuple2<Integer, Integer>> output = counts.collect();
//            for (Tuple2 tuple : output) {
//                System.out.println(tuple._1 + ": " + tuple._2);
//            }

//            hBaseRDD.count();
        } catch (Exception e) {
            log.warn(e);
        }

    }

    /**
     * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
     * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
     */
    public static void main(String[] args) throws InterruptedException {

        new HbaseTest().start();

        System.exit(0);
    }
}