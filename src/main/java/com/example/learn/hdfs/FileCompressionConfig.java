package com.example.learn.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class FileCompressionConfig {

	public static final int DEFAULT_BUFFER_SIZE = 4097;

	final CompressionCodec codec;
	final FileSystem fileSystem;
	final int bufferSize;

	public FileCompressionConfig(String codecClassName, Configuration codecConfig, FileSystem fs)
			throws ClassNotFoundException {
		this(newCompressionCodec(codecClassName, codecConfig), fs);
	}

	public FileCompressionConfig(CompressionCodec codec, FileSystem fs) {
		this(codec, fs, DEFAULT_BUFFER_SIZE);
	}

	public FileCompressionConfig(CompressionCodec codec, FileSystem fs, int bufferSize) {
		this.codec = codec;
		this.fileSystem = fs;
		this.bufferSize = bufferSize;
	}

	static CompressionCodec newCompressionCodec(String className, Configuration conf) throws ClassNotFoundException {
		Class<?> codecClass = Class.forName(className);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		return codec;
	}

}
