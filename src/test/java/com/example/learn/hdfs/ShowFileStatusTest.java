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

	// Assert.assertThat(stat, Matchers.is());

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
		Assert.assertThat(stat.getOwner(), Matchers.is(Consts.USER_NAME));
		Assert.assertThat(stat.getGroup(), Matchers.is(Consts.GROUP_NAME));
		Assert.assertThat(stat.getPermission().toString(), Matchers.is("rw-r--r--"));
	}

	@Test
	public void testDir() throws IOException {
		Path file = new Path(Consts.FILE_SEPARATOR);
		FileStatus stat = fs.getFileStatus(file);
		Assert.assertThat(stat.getPath().toUri().getPath(), Matchers.is(Consts.FILE_SEPARATOR));
		Assert.assertThat(stat.isDirectory(), Matchers.is(true));
		Assert.assertThat(stat.getLen(), Matchers.is(0L));
		Assert.assertThat(stat.getModificationTime(),
				Matchers.is(Matchers.lessThanOrEqualTo(System.currentTimeMillis())));
		Assert.assertThat(stat.getReplication(), Matchers.is((short) 0));
		Assert.assertThat(stat.getBlockSize(), Matchers.is(0L));
		Assert.assertThat(stat.getOwner(), Matchers.is(Consts.USER_NAME));
		Assert.assertThat(stat.getGroup(), Matchers.is(Consts.GROUP_NAME));
		Assert.assertThat(stat.getPermission().toString(), Matchers.is("rwxr-xr-x"));
	}

}
