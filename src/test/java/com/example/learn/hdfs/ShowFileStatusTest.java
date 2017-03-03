package com.example.learn.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.example.learn.hdfs.consts.Consts;

public class ShowFileStatusTest {

	private static FileSystem fs;

	@BeforeClass
	public static void beforeClass() throws IOException {
		fs = FileSystem.get(URI.create(Consts.POM_XML_HDFS_URI), new Configuration());
	}

	@AfterClass
	public static void afterClass() throws IOException {
		if (fs != null) {
			fs.close();
		}
	}

	// Assert.assertThat(stat, Matchers.is();

	@Test
	public void fileStatusForFile() throws IOException {
		Path file = new Path(Consts.POM_XML_HDFS_URI);
		FileStatus stat = fs.getFileStatus(file);
		Assert.assertThat(stat.getPath().toUri().getPath(), Matchers.is(Consts.POM_XML_PATH));
		Assert.assertThat(stat.isDirectory(), Matchers.is(false));
		Assert.assertThat(stat.getModificationTime(),
				Matchers.is(Matchers.lessThanOrEqualTo(System.currentTimeMillis())));
		Assert.assertThat(stat.getReplication(), Matchers.is((short) 3));
		 Assert.assertThat(stat.getBlockSize(), Matchers.is(Consts.HDFS_BLOCK_SIZE));
	}

	@Test
	public void testDir() throws IOException {
		Path file = new Path(Consts.POM_XML_HDFS_URI);
		FileStatus stat = fs.getFileStatus(file);
		Assert.assertThat(stat.getPath().toUri().getPath(), Matchers.is(Consts.POM_XML_PATH));

	}

}
