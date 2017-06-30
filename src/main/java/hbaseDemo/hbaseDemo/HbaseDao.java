package hbaseDemo.hbaseDemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class HbaseDao {
	private static Configuration conf = HBaseConfiguration.create();

	// 创建表
	public static void createTable(String tablename, String columnFamily) throws Exception {
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		TableName tableNameObj = TableName.valueOf(tablename);

		if (admin.tableExists(tableNameObj)) {
			System.out.println("Table exists!");
			System.exit(0);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
		admin.close();
		connection.close();
	}

	// 删除表
	public static void deleteTable(String tableName) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			TableName table = TableName.valueOf(tableName);
			admin.disableTable(table);
			admin.deleteTable(table);
			System.out.println("delete table " + tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 插入一行记录
	public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			table.close();
			connection.close();
			System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void deleteRow(String tableName, String rowkey) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			List<Delete> list = new ArrayList<Delete>();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void QueryAll(String tableName,int count) {
		Connection connection;
		int num=1;
		try {
			connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily()) + ":"
							+ new String(keyValue.getQualifier()) + "====值:" + new String(keyValue.getValue()));
				}
				if(num++==count)
					break;
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void QueryByCondition(String tableName, String rawKey) {

		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get scan = new Get(rawKey.getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				System.out.println("列：" + new String(keyValue.getFamily()) + ":" + new String(keyValue.getQualifier())
						+ "====值:" + new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void QueryByCondition(String tableName, String column, String value) {

		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(column), Bytes.toBytes("name"), CompareOp.EQUAL,
					Bytes.toBytes(value)); // 当列column1的值为aaa时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println("列：" + new String(keyValue.getFamily()) + ":"
							+ new String(keyValue.getQualifier()) + "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void QueryByCondition(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println(
							"列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void updateTable(String tableName, String rowKey, String familyName, String columnName, String value)
			throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
		table.put(put);
		System.out.println("update table Success!");
	}

	public static void main(String[] args) throws Exception {
		// HbaseDao.createTable("testTb", "info");
		// HbaseDao.addRecord("testTb", "001", "info", "name", "zhangsan");
		// HbaseDao.addRecord("testTb", "001", "info", "age", "20");
		// HbaseDao.addRecord("testTb", "002", "info", "name", "lisi");
		// HbaseDao.addRecord("testTb", "002", "info", "age", "20");
		HbaseDao.QueryAll("mediad:MediaD_VideoRawData",1);
//		System.out.println();
//		HbaseDao.QueryByCondition("testTb", "001");
//		System.out.println();
//		HbaseDao.QueryByCondition("testTb", "info", "lisi");
		// HbaseDao.deleteTable("testTb");
	}
	static {
		conf.set("hbase.rootdir", "hdfs://nameservice1/hbase");
		// 设置Zookeeper,直接设置IP地址
//		准生产集群gs-server-1016,gs-server-1002,gs-server-1021
//		生产集群gs-server-1038,gs-server-1046,gs-server-1039,gs-server-1040,gs-server-1047
//		HBase集群gs-server-1027:2181,gs-server-1028:2181,gs-server-1031:2181,gs-server-1032:2181,gs-server-1033:2181
//		测试集群gs-server-v-129,gs-server-v-128,gs-server-v-127
		String cs="gs-server-v-129,gs-server-v-128,gs-server-v-127";
		String hbase="gs-server-1027:2181,gs-server-1028:2181,gs-server-1031:2181,gs-server-1032:2181,gs-server-1033:2181";
		String zsc="gs-server-1016,gs-server-1002,gs-server-1021";
		String sc="gs-server-1038,gs-server-1046,gs-server-1039,gs-server-1040,gs-server-1047";
		conf.set("hbase.zookeeper.quorum", cs);
	}
}
