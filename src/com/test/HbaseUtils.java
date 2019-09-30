package com.test;
import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

import com.sungoal.hbase.HbaseParams;
import com.sungoal.model.hbase.HbaseTrackModel;
import com.sungoal.utils.ClientConf;
import com.sungoal.utils.DataType;

public class HbaseUtils implements Serializable {
	private static final long serialVersionUID = 2597925609158654766L;
	public static Connection connection;
	public static HBaseAdmin admin;
	public static Configuration conf;
	public static String namespace = "default";
	private static long regionMaxFileSize = 1073741824000L;
	private static Algorithm compressionType = Algorithm.valueOf("SNAPPY");
	private static ExecutorService pool = Executors.newFixedThreadPool(100);

	public static void initClient() throws IOException {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "192.168.0.41");
		conf.set("zookeeper.znode.parent", ClientConf.znodeParent);
		/*
		 * if (ClientConf.isKerberos) {
		 * conf.set("hadoop.security.authentication", "kerberos");
		 * conf.set("hadoop.security.authorization", "true");
		 * conf.set("hbase.security.authentication", "kerberos");
		 * conf.set("hbase.security.authorization", "true");
		 * conf.set("hbase.master.kerberos.principal", "hbase/_HOST@BIGDATA");
		 * conf.set("hbase.regionserver.kerberos.principal",
		 * "hbase/_HOST@BIGDATA"); System.setProperty("java.security.krb5.conf",
		 * ClientConf.krb5file); UserGroupInformation.setConfiguration(conf);
		 * UserGroupInformation.loginUserFromKeytab(ClientConf.keytab,
		 * ClientConf.keytabfile); User loginedUser =
		 * User.create(UserGroupInformation.getLoginUser()); connection =
		 * ConnectionFactory.createConnection(conf, pool, loginedUser); } else {
		 */
		connection = ConnectionFactory.createConnection(conf);
		// }

		admin = (HBaseAdmin) connection.getAdmin();
		System.out.println("初始化Hbase连接!");
	}

	public static void init() throws IOException {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "192.168.0.41");
		conf.set("zookeeper.znode.parent", ClientConf.znodeParent);
		
		connection = ConnectionFactory.createConnection(conf);
	

		admin = (HBaseAdmin) connection.getAdmin();
	}

	public static Connection getConn() {
		Connection conn = null;

		try {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.set("hbase.zookeeper.quorum", "192.168.0.41");
			conf.set("zookeeper.znode.parent", ClientConf.znodeParent);
			
			conn = ConnectionFactory.createConnection(conf);
	
		} catch (Exception var2) {
			var2.printStackTrace();
		}

		return conn;
	}

	public static void closeConn() throws IOException {
		connection.close();
		admin.close();
	}

	public static void createTable(String tableName, String[] family)
			throws Exception {
		TableName table = TableName.valueOf(tableName);
		if (admin.tableExists(table)) {
			System.out.println("create table failed.the table already exists");
		} else {
			HTableDescriptor tableDescriptor = new HTableDescriptor(table);
			if (family != null) {
				for (int i = 0; i < family.length; ++i) {
					HColumnDescriptor hd = new HColumnDescriptor(family[i]);
					hd.setCompressionType(compressionType);
					tableDescriptor.addFamily(hd);
				}
			}

			admin.createTable(tableDescriptor);
			System.out.println("create table successed");
		}
	}

	public static void createTable(String tableName, String[] family,
			byte[][] regions) throws Exception {
		TableName table = TableName.valueOf(tableName);
		HTableDescriptor builder = new HTableDescriptor(table);
		builder.setMaxFileSize(regionMaxFileSize);
		builder.setValue("SPLIT_POLICY",
				ConstantSizeRegionSplitPolicy.class.getName());

		for (int i = 0; i < family.length; ++i) {
			HColumnDescriptor hd = new HColumnDescriptor(family[i]);
			hd.setCompressionType(compressionType);
			builder.addFamily(hd);
		}

		admin.createTable(builder, regions);
		admin.disableTable(table);
		builder.addCoprocessor("com.sungoal.hbase.coprocessor."
				+ tableName.split(":")[1] + "EndPoint");
		builder.addCoprocessor("com.sungoal.hbase.coprocessor."
				+ tableName.split(":")[1] + "Coprocessor");
		admin.modifyTable(table, builder);
		admin.enableTable(table);
		System.out.println("表" + tableName + "创建完成");
	}

	public static boolean tableExists(String tableName) throws Exception {
		TableName table = TableName.valueOf(tableName);
		return admin.tableExists(table);
	}

	public static void dropTable(String tableName) throws Exception {
		TableName table = TableName.valueOf(tableName);
		if (admin.tableExists(table)) {
			if (admin.isTableEnabled(table)) {
				admin.disableTable(table);
			}

			admin.deleteTable(table);
		}

		System.out.println("drop table successed");
	}

	public static void truncateTable(String tableName) throws Exception {
		TableName table = TableName.valueOf(tableName);
		if (admin.tableExists(table)) {
			admin.disableTable(table);
			admin.truncateTable(table, true);
		}

		System.out.println("truncate table successed");
	}

	public static Table getHTable(String tableName) throws Exception {
		return connection.getTable(TableName.valueOf(tableName));
	}

	public static Table getHTable(Connection conn, String tableName)
			throws Exception {
		return conn.getTable(TableName.valueOf(tableName));
	}

	public static BufferedMutator getBuffTable(String tableName)
			throws Exception {
		return connection.getBufferedMutator(TableName.valueOf(tableName));
	}

	public static BufferedMutator getBuffTable(Connection conn, String tableName)
			throws Exception {
		return conn.getBufferedMutator(TableName.valueOf(tableName));
	}

	public static Put convertEntityToPut(byte[] startRowKey, Object obj,
			String tableName) throws IllegalArgumentException,
			IllegalAccessException, NoSuchFieldException, SecurityException,
			IntrospectionException, InvocationTargetException,
			NoSuchMethodException {
		Map<byte[], byte[]> map = new LinkedHashMap();
		HbaseTrackModel model = (HbaseTrackModel) obj;

		for (int i = 0; i < model.getHFieldNames().length; ++i) {
			String fieldName = model.getHFieldNames()[i];
			byte[] column = fieldName.getBytes();
			Field field = obj.getClass().getDeclaredField(fieldName);
			field.setAccessible(true);
			String value = "";
			if (field.get(obj) != null) {
				value = field.get(obj).toString();
			}

			byte[] columValue = value.getBytes();
			map.put(column, columValue);
		}

		Method method = obj.getClass().getDeclaredMethod("createDataRowKey",
				byte[].class, String.class);
		byte[] rowkey = (byte[]) method.invoke(obj, startRowKey, tableName);
		Put p = new Put(rowkey);
		Iterator iterator = map.entrySet().iterator();

		while (iterator.hasNext()) {
			Entry<byte[], byte[]> entry = (Entry) iterator.next();
			p.addColumn(HbaseParams.InfoFamily.getBytes(),
					(byte[]) entry.getKey(), (byte[]) entry.getValue());
		}

		return p;
	}

	public static void updateTableCount(String tableName, long count)
			throws Exception {
		Table table = getHTable(DataType.TABLECOUNT.value);
		Result result = table.get(new Get(Bytes.toBytes(tableName)));
		if (!result.isEmpty()) {
			count += Bytes.toLong(result.getValue(
					Bytes.toBytes(HbaseParams.InfoFamily),
					Bytes.toBytes("total")));
		}

		Put p = new Put(Bytes.toBytes(tableName));
		p.addColumn(Bytes.toBytes(HbaseParams.InfoFamily),
				Bytes.toBytes("total"), Bytes.toBytes(count));
		table.put(p);
		table.close();
	}

	public static void deleteTableCount(String tableName) throws Exception {
		Table table = getHTable(DataType.TABLECOUNT.value);
		Put p = new Put(Bytes.toBytes(tableName));
		p.addColumn(Bytes.toBytes(HbaseParams.InfoFamily),
				Bytes.toBytes("total"), Bytes.toBytes(0));
		table.put(p);
		table.close();
	}

	public static long rowCount(String tableName)
			throws IllegalArgumentException, Throwable {
		Table table = getHTable(DataType.TABLECOUNT.value);
		Result result = table.get(new Get(Bytes.toBytes(tableName)));
		long count = 0L;
		if (!result.isEmpty()) {
			count = Bytes.toLong(result.getValue(
					Bytes.toBytes(HbaseParams.InfoFamily),
					Bytes.toBytes("total")));
		}

		table.close();
		return count;
	}

	public static long rowCount(Connection conn, String tableName)
			throws IllegalArgumentException, Throwable {
		Table table = conn.getTable(TableName
				.valueOf(DataType.TABLECOUNT.value));
		Result result = table.get(new Get(Bytes.toBytes(tableName)));
		long count = 0L;
		if (!result.isEmpty()) {
			count = Bytes.toLong(result.getValue(
					Bytes.toBytes(HbaseParams.InfoFamily),
					Bytes.toBytes("total")));
		}

		table.close();
		return count;
	}

	public static void updateTableCount(Connection conn, String tableName,
			long count) throws Exception {
		Table table = conn.getTable(TableName
				.valueOf(DataType.TABLECOUNT.value));
		Result result = table.get(new Get(Bytes.toBytes(tableName)));
		if (!result.isEmpty()) {
			count += Bytes.toLong(result.getValue(
					Bytes.toBytes(HbaseParams.InfoFamily),
					Bytes.toBytes("total")));
		}

		Put p = new Put(Bytes.toBytes(tableName));
		p.addColumn(Bytes.toBytes(HbaseParams.InfoFamily),
				Bytes.toBytes("total"), Bytes.toBytes(count));
		table.put(p);
		table.close();
	}

	/*
	 * public static int getRegionNum(String tableName) throws
	 * IllegalArgumentException, Throwable { List<HRegionInfo> list = admin
	 * .getTableRegions(Bytes.toBytes(tableName)); return list.size(); }
	 */

	/*
	 * public static void addRegion(String tableName, byte[] rowKey, int
	 * regionNum) throws IllegalArgumentException, Throwable { while
	 * (getRegionNum(tableName) != regionNum) { Thread.currentThread();
	 * Thread.sleep(100L); }
	 * 
	 * admin.split(TableName.valueOf(tableName), rowKey); }
	 */

	public static void addRegion(String tableName)
			throws IllegalArgumentException, Throwable {
		admin.split(TableName.valueOf(tableName));
	}

	/*
	 * public static boolean hasColumnFamily(String tableName, String
	 * familyName) throws IllegalArgumentException, Throwable { return
	 * admin.getTableDescriptor(tableName.getBytes()).hasFamily(
	 * familyName.getBytes()); }
	 * 
	 * public static void addColumnFamily(String tableName, String familyName)
	 * throws IllegalArgumentException, Throwable { HColumnDescriptor
	 * columnDescriptor = new HColumnDescriptor(familyName);
	 * columnDescriptor.setCompressionType(compressionType);
	 * admin.addColumn(tableName, columnDescriptor); }
	 */
	public static List<String> getAllColumnFamilyOfTable(String tableName)
			throws IOException, Exception {
		List<String> list = new ArrayList();
		HTableDescriptor table = getHTable(tableName).getTableDescriptor();
		HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
		HColumnDescriptor[] var7 = columnDescriptors;
		int var6 = columnDescriptors.length;

		for (int var5 = 0; var5 < var6; ++var5) {
			HColumnDescriptor hColumnDescriptor = var7[var5];
			list.add(hColumnDescriptor.getNameAsString());
		}

		return list;
	}

	/*
	 * public static void deleteColumnFamily(String tableName, String
	 * familyName) throws IOException, Exception { TableName table =
	 * TableName.valueOf(tableName); if (admin.tableExists(table)) {
	 * admin.disableTable(table); admin.deleteColumn(tableName, familyName);
	 * admin.enableTable(table); System.out.println(familyName + "删除成功..."); }
	 * 
	 * }
	 */
	public static List<String> getAllTables() throws IOException {
		List<String> list = new ArrayList();
		HTableDescriptor[] tables = admin.listTables();
		HTableDescriptor[] var5 = tables;
		int var4 = tables.length;

		for (int var3 = 0; var3 < var4; ++var3) {
			HTableDescriptor hTableDescriptor = var5[var3];
			list.add(hTableDescriptor.getNameAsString());
		}

		return list;
	}

	/*
	 * public static void saveLastCommitOffset(Connection conn, String rowkey,
	 * Offset[] offsetRanges) throws Exception { Table table = getHTable(conn,
	 * DataType.KAFKA_CONSUMER_OFFSET.value); Put put = new
	 * Put(Bytes.toBytes(rowkey)); offsetRanges[] var8 = offsetRanges; int var7
	 * = offsetRanges.length;
	 * 
	 * for (int var6 = 0; var6 < var7; ++var6) { OffsetRange offsetRange =
	 * var8[var6]; put.addColumn(Bytes.toBytes(HbaseParams.InfoFamily),
	 * Bytes.toBytes(offsetRange.partition()),
	 * Bytes.toBytes(offsetRange.untilOffset())); }
	 * 
	 * table.put(put); table.close(); }
	 * 
	 * 
	 * public static void saveLastCommitOffset(String rowkey, OffsetRange[]
	 * offsetRanges) throws Exception { Table table =
	 * getHTable(DataType.KAFKA_CONSUMER_OFFSET.value); Put put = new
	 * Put(Bytes.toBytes(rowkey)); OffsetRange[] var7 = offsetRanges; int var6 =
	 * offsetRanges.length;
	 * 
	 * for (int var5 = 0; var5 < var6; ++var5) { OffsetRange offsetRange =
	 * var7[var5]; put.addColumn(Bytes.toBytes(HbaseParams.InfoFamily),
	 * Bytes.toBytes(offsetRange.partition()),
	 * Bytes.toBytes(offsetRange.untilOffset())); }
	 * 
	 * table.put(put); table.close(); }
	 * 
	 * public static Map<TopicPartition, Long> getLastCommitOffset(String
	 * rowkey, String topic) throws Exception { Map<TopicPartition, Long>
	 * partitions = new HashMap(); Table table =
	 * getHTable(DataType.KAFKA_CONSUMER_OFFSET.value); Get get = new
	 * Get(Bytes.toBytes(rowkey)); Result result = table.get(get); if
	 * (!result.isEmpty()) { Iterator var7 = result.listCells().iterator();
	 * 
	 * while (var7.hasNext()) { Cell cell = (Cell) var7.next(); TopicPartition
	 * partition = new TopicPartition(topic,
	 * Bytes.toInt(cell.getQualifierArray(), cell.getQualifierOffset(),
	 * cell.getQualifierLength())); partitions.put( partition,
	 * Bytes.toLong(cell.getValueArray(), cell.getValueOffset(),
	 * cell.getValueLength())); } }
	 * 
	 * table.close(); return partitions; }
	 */
	public static void deleteColumnValue(String tableName, String rowkey,
			String columnFamily, String columnKey) throws Exception {
		Table table = getHTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey));
		table.delete(delete);
		table.close();
	}

	public static void insertColumnValue(String tableName, String rowkey,
			String columnFamily, String columnKey, String columnValue)
			throws Exception {
		insertColumnValue(tableName, rowkey, columnFamily, columnKey,
				Bytes.toBytes(columnValue));
	}

	public static void cacheMultitrack(String cacheId, String key, byte[] value)
			throws Exception {
		insertColumnValue(DataType.MULTITRACKCACHE.value, cacheId,
				HbaseParams.InfoFamily, key, value);
	}

	public static void cacheMultitrack(Put value) throws Exception {
		Table table = getHTable(DataType.MULTITRACKCACHE.value);
		table.put(value);
		table.close();
	}

	public static byte[] getCacheMultitrack(String cacheId, String key)
			throws Exception {
		Table table = getHTable(DataType.MULTITRACKCACHE.value);
		Get g = new Get(Bytes.toBytes(cacheId));
		g.addColumn(Bytes.toBytes(HbaseParams.InfoFamily), Bytes.toBytes(key));
		Result result = table.get(g);
		byte[] track = null;
		if (!result.isEmpty()) {
			track = result.getValue(Bytes.toBytes(HbaseParams.InfoFamily),
					Bytes.toBytes(key));
		}

		table.close();
		return track;
	}

	public static void deleteCacheMultitrack(String cacheId)
			throws IOException, Exception {
		Table table = getHTable(DataType.MULTITRACKCACHE.value);
		Delete delete = new Delete(Bytes.toBytes(cacheId));
		table.delete(delete);
		table.close();
	}

	public static void insertColumnValue(String tableName, String rowkey,
			String columnFamily, String columnKey, byte[] columnValue)
			throws Exception {
		BufferedMutator table = getBuffTable(tableName);
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKey),
				columnValue);
		table.mutate(put);
		table.close();
	}
}