package com.example.learn.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemCat {
	
	private static final String FILE_NAME = "startup.sh";
	private static final String FILE_URI = Consts.URI_PREFIX + FILE_NAME;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(FILE_URI), conf);
		Path path = new Path(FILE_URI);
		try (InputStream in = fs.open(path)) {
			IOUtils.copy(in, System.out, 4096);
		}
	}

}
