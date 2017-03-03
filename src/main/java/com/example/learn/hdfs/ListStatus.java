package com.example.learn.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.example.learn.hdfs.consts.Consts;

public class ListStatus {

	public static void main(String[] args) throws Exception {
		String uri = Consts.HDFS_ADDRESS;
		FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());

		Path[] paths = new Path[1];
		paths[0] = new Path(Consts.HDFS_USER_HOME);
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p);
		}
	}

}
