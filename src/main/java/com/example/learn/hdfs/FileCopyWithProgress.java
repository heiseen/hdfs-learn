package com.example.learn.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import com.example.learn.hdfs.consts.Consts;
import com.google.common.base.Joiner;

public class FileCopyWithProgress {

	public static void main(String[] args) throws Exception {
		String localSrc = Consts.POM_XML;
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(Consts.POM_XML_HDFS_URI), conf);
		OutputStream out = fs.create(new Path(Consts.POM_XML_HDFS_URI), new Progressable() {
			@Override
			public void progress() {
				System.out.print(".");
			}
		});
		System.out.println();

		IOUtils.copyBytes(in, out, 4096, true);
	}

	static String newNowFileUri() {
		StringBuilder builder = new StringBuilder();
		LocalDateTime dateTime = LocalDateTime.now();
		String year = Integer.toString(dateTime.getYear());
		String month = to2DigitsString(dateTime.getMonthValue());
		String day = to2DigitsString(dateTime.getDayOfMonth());
		String hour = to2DigitsString(dateTime.getHour());
		String minute = to2DigitsString(dateTime.getMinute());
		String second = to2DigitsString(dateTime.getSecond());
		String hms = hour + minute + second;
		return Joiner.on(Consts.FILE_SEPARATOR)
				.appendTo(builder, Consts.HDFS_URI_PREFIX, year, month, day, hms, Consts.POM_XML).toString();
	}

	static String to2DigitsString(int i) {
		String ret = Integer.toString(i);
		if (ret.length() < 2) {
			ret = Consts.NUM_ZERO + ret;
		}
		return ret;
	}

}
