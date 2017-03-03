package com.example.learn.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.example.learn.hdfs.consts.Consts;

public class FileSystemCat {

	private static final String FILE_URI = Consts.HDFS_USER_HOME + Consts.FILE_SEPARATOR + Consts.POM_XML;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(FILE_URI), conf);
		Path path = new Path(FILE_URI);
		try (InputStream in = fs.open(path)) {
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
	}

}
