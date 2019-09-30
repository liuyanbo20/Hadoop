package com.test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import com.sungoal.utils.ClientConf;

public class HBaseTool {
	public static Connection connection;
	public static HBaseAdmin admin;
	public static Configuration conf;
	public static String namespace = "default";
	private static long regionMaxFileSize = 1073741824000L;
	private static Algorithm compressionType = Algorithm.valueOf("SNAPPY");
	private static ExecutorService pool = Executors.newFixedThreadPool(100);

	public void init() throws IOException {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", ClientConf.servers);
		conf.set("zookeeper.znode.parent", ClientConf.znodeParent);
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hadoop.security.authorization", "true");
		conf.set("hbase.security.authentication", "kerberos");
		conf.set("hbase.security.authorization", "true");
		conf.set("hbase.master.kerberos.principal", "hbase/_HOST@BIGDATA");
		conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@BIGDATA");
		System.setProperty("java.security.krb5.conf", ClientConf.krb5file);
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(ClientConf.keytab,
				ClientConf.keytabfile);
		User loginedUser = User.create(UserGroupInformation.getLoginUser());
		connection = ConnectionFactory
				.createConnection(conf, pool, loginedUser);

	}

}
