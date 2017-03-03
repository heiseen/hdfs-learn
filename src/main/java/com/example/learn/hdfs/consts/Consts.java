package com.example.learn.hdfs.consts;

public class Consts {

	// ==========================================================================
	public static final String FILE_SEPARATOR = "/";

	public static final String USER_NAME = "zhouwei";
	public static final String USER_HOME = "/user/" + USER_NAME;

	public static final String HDFS_URI_PREFIX = "hdfs://localhost:9000" + USER_HOME;

	// ==========================================================================
	public static final long HDFS_BLOCK_SIZE = 128 * 1024 * 1024;
	
	public static final String NUM_ZERO = "0";

	public static final String POM_XML = "pom.xml";
	public static final String POM_XML_PATH = USER_HOME + FILE_SEPARATOR + POM_XML;
	public static final String POM_XML_HDFS_URI = HDFS_URI_PREFIX + FILE_SEPARATOR + POM_XML;

	private Consts() {
	}

}
