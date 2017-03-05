package com.example.learn.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class FileBackupConfig {
	final CompressionCodec codec;
	final FileSystem fileSystem;

	public FileBackupConfig(String codecClassName, Configuration codecConfig, FileSystem fs)
			throws ClassNotFoundException {
		this(newCompressionCodec(codecClassName, codecConfig), fs);
	}

	public FileBackupConfig(CompressionCodec codec, FileSystem fs) {
		this.codec = codec;
		this.fileSystem = fs;
	}

	static CompressionCodec newCompressionCodec(String className, Configuration conf) throws ClassNotFoundException {
		Class<?> codecClass = Class.forName(className);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		return codec;
	}

	@Override
	public String toString() {
		return "FileBackupConfig [codec=" + codec + ", fileSystem=" + fileSystem + "]";
	}

}
