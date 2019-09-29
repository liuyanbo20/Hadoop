package com.test;

import com.sungoal.utils.ClientConf;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HadoopUtils {
	private static String imageUrl = "/archives/image/";
	private static String fileUrl = "/archives/file/";
	private static FileSystem fs = null;
	private static String hadoopUrl = "hdfs://master";

	public static void init() throws URISyntaxException {
		Configuration conf = new Configuration();

		try {
			conf.set("fs.defaultFS", hadoopUrl);
			conf.set("dfs.nameservices", "master");
			conf.set("dfs.ha.namenodes.master", "nn1");
			conf.set("dfs.namenode.rpc-address.master.nn1", ClientConf.nn1);
			//conf.set("dfs.namenode.rpc-address.sungoal.nn2", ClientConf.nn2);
			conf.set("dfs.client.failover.proxy.provider.master",
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
			fs = FileSystem.get(new URI(hadoopUrl), conf);
		} catch (IOException var2) {
			var2.printStackTrace();
		}

	}

	public static String uploadImage(String imageType, InputStream in,
			String idCard) throws IOException {
		String hdfsUrl = hadoopUrl + imageUrl + idCard;
		String fileName = UUID.randomUUID().toString().replaceAll("-", "")
				+ "." + imageType;
		addFile(hdfsUrl, fileName, in);
		return hdfsUrl + "/" + fileName;
	}

	public static String uploadFile(String fileName, InputStream in,
			String idCard) throws IOException {
		String hdfsUrl = hadoopUrl + fileUrl + idCard;
		addFile(hdfsUrl, fileName, in);
		return hdfsUrl + "/" + fileName;
	}

	private static void addFile(String hdfsUrl, String fileName, InputStream in)
			throws IOException {
		OutputStream out = fs.create(new Path(hdfsUrl + "/" + fileName));
		IOUtils.copyBytes(in, out, 4096, true);
		IOUtils.closeStream(out);
	}

	public static InputStream getFileStream(String hdfsFileName)
			throws IOException {
		Path path = new Path(hdfsFileName);
		FSDataInputStream in = fs.open(path);
		InputStream inputsream = in.getWrappedStream();
		return inputsream;
	}

	public static void markDir(String hdfsFilePath) throws IOException {
		Path path = new Path(hdfsFilePath);
		fs.mkdirs(path);
	}

	public static void cat(String hdfsFileName) throws IOException {
		Path path = new Path(hdfsFileName);
		FSDataInputStream fsdis = null;

		try {
			fsdis = fs.open(path);
			IOUtils.copyBytes(fsdis, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(fsdis);
		}

	}

	public static void downloadFile(String hdfsFileName, String localFileName)
			throws IOException {
		Path path = new Path(hdfsFileName);
		fs.copyToLocalFile(false, path, new Path(localFileName), true);
	}

	public static void delete(String filePath) throws IOException {
		Path path = new Path(filePath);
		fs.deleteOnExit(path);
	}

	public static void ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		FileStatus[] var6 = list;
		int var5 = list.length;

		for (int var4 = 0; var4 < var5; ++var4) {
			FileStatus f = var6[var4];
			System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(),
					f.isDirectory(), f.getLen());
		}

	}
}