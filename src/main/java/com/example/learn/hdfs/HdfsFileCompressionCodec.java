package com.example.learn.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import com.example.learn.hdfs.consts.Consts;

public class HdfsFileCompressionCodec {

	public static void main(String[] args) throws Exception {
		// codec
		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
		CompressionCodec codec = newCompressionCodec(codecClassName);
		String inPathStr = Consts.POM_XML_HDFS_URI;
		
		// input
		FileSystem fileSystem = FileSystem.get(URI.create(inPathStr), new Configuration());
		Path inPath = new Path(inPathStr);
		InputStream in = fileSystem.open(inPath);
		
		// output
		String outPathStr = inPathStr.replace(Consts.HDFS_USER_HOME, Consts.HDFS_USER_HOME + "/backup") + ".gz";
		Path outPath = new Path(outPathStr);
		CompressionOutputStream out = codec.createOutputStream(fileSystem.create(outPath));
		IOUtils.copyBytes(in, out, 4096, false);
		
		// close
		out.close();
		in.close();
	}

	private static CompressionCodec newCompressionCodec(String className) throws ClassNotFoundException {
		Class<?> codecClass = Class.forName(className);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		return codec;
	}

}
