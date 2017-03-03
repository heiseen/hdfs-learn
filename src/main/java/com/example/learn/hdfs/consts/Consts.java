package com.example.learn.hdfs.consts;

public class Consts {

	public static final String FILE_SEPARATOR = "/";

	public static final String POM_XML = "pom.xml";

	public static final String NUM_ZERO = "0";

	public static final String USERNAME = "zhouwei";
	public static final String HDFS_URI_PREFIX = "hdfs://localhost:9000/user/" + USERNAME;

	public static final String POM_XML_HDFS_URI = HDFS_URI_PREFIX + FILE_SEPARATOR + POM_XML;

	private Consts() {
	}

}
