import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.*;

public class HBaseTest {

	static Configuration conf;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.141.208.43,10.141.208.44,10.141.208.45");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master.port", "60000");
		conf.setLong("hbase.rpc.timeout", Long.parseLong("60000"));
		conf = HBaseConfiguration.create(conf);
	}
	
	static Random rd = new Random();
	
	public static void main(String[] args) {
		
		String tableName = "hbasetest";
		
		try {
			Connection conn = ConnectionFactory.createConnection(conf);
			HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
		
			if(admin.tableExists(tableName)) {				
				admin.disableTable(tableName);
				admin.deleteTable(tableName);				
			}
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));
			admin.createTable(tableDesc);
			
			HTable table = (HTable)conn.getTable(TableName.valueOf("hbasetest"));
			//table.setAutoFlushTo(false);			
			//table.setWriteBufferSize(8 * 1024 * 1024);
			
			System.out.println("---import data: start---");
			long t1s = System.currentTimeMillis();
			
			Random rand = new Random();
			String curkey = null;
			String curdes = "This is a description";
			int curval = 0;
			String curstr = "";
			for(int i = 0; i < 10000; i++) {
				
				long curlong = rand.nextLong();
				curkey = "rowkey" + curlong;
				curval = rand.nextInt(1000);
				curstr = String.valueOf(curval);
				
				Put put = new Put(Bytes.toBytes(curkey));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("des"), Bytes.toBytes(curdes));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("val"), Bytes.toBytes(curstr));
				//put.setWriteToWAL(false);

				if(i % 1000 == 0) {
					System.out.println(curkey + " " + curdes + " " + curval);
				}
				
				table.put(put);
			}
			
			long t1e = System.currentTimeMillis();
			System.out.println("---import data: end---");
			
			table.close();
						
			if(admin.tableExists(tableName)) {				
				admin.disableTable(tableName);
				admin.deleteTable(tableName);				
			}
			tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));
			admin.createTable(tableDesc);
			
			table = (HTable)conn.getTable(TableName.valueOf("hbasetest"));
			table.setAutoFlushTo(false);			
			table.setWriteBufferSize(8 * 1024 * 1024);
			
			System.out.println("---batched import data: start---");
			long t2s = System.currentTimeMillis();
						
			curkey = null;
			curdes = "This is a description";
			curval = 0;
			
			List<Put> putList = new ArrayList<Put>();
			
			for(int i = 0; i < 1000000; i++) {
				
				long curlong = rand.nextLong();
				curkey = "rowkey" + curlong;
				curval = rand.nextInt(1000);
				curstr = String.valueOf(curval);
				
				Put put = new Put(Bytes.toBytes(curkey));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("des"), Bytes.toBytes(curdes));
				put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("val"), Bytes.toBytes(curstr));
				//put.setWriteToWAL(false);
				
				if(i % 10000 == 0) {
					System.out.println(curkey + " " + curdes + " " + curval);
				}
				
				putList.add(put);
				if(putList.size() == 20000) {
					table.put(putList);
					putList.clear();
				}
			}
			
			long t2e = System.currentTimeMillis();
			System.out.println("---batched import data: end---");
			
			table.close();

			table = (HTable)conn.getTable(TableName.valueOf("hbasetest"));
			
			System.out.println("---scan data: start---");
			long t3s = System.currentTimeMillis();
			
			int count = 0;
			
			Scan scan = new Scan();
			ResultScanner results = table.getScanner(scan);
			for(Result result: results) {
				count++;
				System.out.println("scan rows -> " + count);
				System.out.println(count);
				for(Cell cell: result.rawCells()) {
					String value = new String(CellUtil.cloneValue(cell));
					System.out.println(value);
				}
			}
			
			long t3e = System.currentTimeMillis();
			System.out.println("---scan data: end---");
			
			table.close();
			
			table = (HTable)conn.getTable(TableName.valueOf("hbasetest"));
			
			System.out.println("---select data: start---");
			long t4s = System.currentTimeMillis();
			
			count = 0;
			
			SingleColumnValueFilter scvf = new SingleColumnValueFilter(  
			        Bytes.toBytes("cf"),   
			        Bytes.toBytes("val"),   
			        CompareFilter.CompareOp.GREATER,   
			        Bytes.toBytes("900"));  
			
			scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("val"));
			
			scan = new Scan();
			scan.setFilter(scvf);

			results = table.getScanner(scan);
			for(Result result: results) {
				count++;
				System.out.println("scan rows -> " + count);
				for(Cell cell: result.rawCells()) {
					System.out.println(count + ": key->" + new String(CellUtil.cloneRow(cell)) + " val->" + new String(CellUtil.cloneValue(cell)));
				}
			}
			
			long t4e = System.currentTimeMillis();
			System.out.println("---select data: end---");
			
			table.close();
			
			admin.close();
			
			System.out.println("import data: " + (t1e - t1s));
			System.out.println("batched import data: " + (t2e - t2s));
			System.out.println("scan data: " + (t3e - t3s));
			System.out.println("select data: " + (t4e - t4s));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
