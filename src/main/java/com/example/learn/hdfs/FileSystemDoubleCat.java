package com.example.learn.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.example.learn.hdfs.consts.Consts;

public class FileSystemDoubleCat {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(Consts.POM_XML_HDFS_URI), conf);
		Path path = new Path(Consts.POM_XML_HDFS_URI);
		try (FSDataInputStream in = fs.open(path)) {
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
	}

}
