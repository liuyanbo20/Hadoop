package com.test;


public class Test {

	public static void main(String[] args) {
		HadoopUtils hadoopUtils=new HadoopUtils();
		try {
			hadoopUtils.init();
			hadoopUtils.markDir("/liuyanbo20");
			hadoopUtils.ls("/");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
