package com.example.learn.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import com.example.learn.hdfs.consts.Consts;

public class FileCompression {

	private final FileCompressionConfig config;

	public FileCompression(FileCompressionConfig config) throws ClassNotFoundException {
		this.config = config;
	}

	public static void main(String[] args) throws Exception {
		GzipCodec codec = new GzipCodec();
		Configuration hdfsConfig = new Configuration();
		URI uri = URI.create(Consts.HDFS_ADDRESS);
		FileSystem fileSystem = FileSystem.get(uri, hdfsConfig, Consts.USER_NAME);
		String fromPath = Consts.POM_XML_HDFS_URI;
		String toPath = fromPath.replace(Consts.HDFS_USER_HOME, Consts.HDFS_USER_HOME + "/backup") + ".gz";

		// compress and save the compressed file into HDFS
		FileCompressionConfig conf = new FileCompressionConfig(codec, fileSystem);
		FileCompression backup = new FileCompression(conf);
		backup.processFile(fromPath, toPath);
		
		// close FileSystem
		fileSystem.close();
	}

	void processFile(String from, String to) throws IOException {
		Path fromPath = new Path(from);
		Path toPath = new Path(to);
		processFile(fromPath, toPath);
	}

	void processFile(Path fromPath, Path toPath) throws IOException {
		InputStream in = newInputStream(fromPath);
		CompressionOutputStream out = newOutputStream(toPath);
		IOUtils.copyBytes(in, out, config.bufferSize, true);
	}

	InputStream newInputStream(Path path) throws IOException {
		return config.fileSystem.open(path);
	}

	CompressionOutputStream newOutputStream(Path path) throws IOException {
		FSDataOutputStream fsdos = config.fileSystem.create(path);
		return config.codec.createOutputStream(fsdos);
	}

}
