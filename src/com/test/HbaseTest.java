package com.test;


public class HbaseTest {

	public static void main(String[] args) {
		HbaseUtils hbaseUtils = new HbaseUtils();
		String[] family={"id","name","sex","age"};
		try {
			// hbaseUtils.init();
			hbaseUtils.initClient();
			hbaseUtils.createTable("userinfo", family);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
