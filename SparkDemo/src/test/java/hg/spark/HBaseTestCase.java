package hg.spark;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTestCase {

	static Configuration cfg = HBaseConfiguration.create();
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		org.
		String tablename = "hbase_tb";
		String columnFamily = "cf";
		try {
			HBaseTestCase.create(tablename, columnFamily);
			HBaseTestCase.put(tablename, "row1", columnFamily, "cl1", "hello world!");
			HBaseTestCase.get(tablename, "row1");
			HBaseTestCase.scan(tablename);
			if(true==HBaseTestCase.delete(tablename)) {
				System.out.println("Delete table:"+tablename+" success!");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	/**
	 * create a table :table_name(columnFamily) 
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public static void create(String tablename, String columnFamily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if(admin.tableExists(tablename)) {
			System.out.println("table exists!");
			System.exit(0);
		}
		else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}

	/**
	 * put a row data into table 
	 * @param tablename
	 * @param row
	 * @param columnFamily
	 * @param column
	 * @param data
	 * @throws Exception
	 */
	public static void put(String tablename, String row, String columnFamily, String column, String data) throws Exception{
		HTable table = new HTable(cfg, tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put '"+row+"', '"+columnFamily+":"+column+"', '"+data+"'");
		
	}
	
	/**
	 * get a row data from a table
	 * @param tablename
	 * @param row
	 * @throws Exception
	 */
	public static void get(String tablename, String row) throws Exception {
		HTable table = new HTable(cfg, tablename);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		System.out.println("Get: "+result);
	}
	
	/**
	 * show all data from a table
	 * @param tablename
	 * @throws Exception
	 */
	public static void scan(String tablename) throws Exception {
		HTable table = new HTable(cfg, tablename);
		Scan s =new Scan();
		ResultScanner rs = table.getScanner(s);
		for(Result r:rs) {
			System.out.println("Scan: "+r);
		}
	}

	/**
	 * delete a table's data
	 * @param tablename
	 * @return
	 * @throws IOException
	 */
	public static boolean delete(String tablename) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if(admin.tableExists(tablename)) {
			try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
}
