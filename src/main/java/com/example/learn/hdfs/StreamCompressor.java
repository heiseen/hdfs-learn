package com.example.learn.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import com.example.learn.hdfs.consts.Consts;

public class StreamCompressor {

	public static void main(String[] args) throws Exception {
		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codecClass = Class.forName(codecClassName);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		InputStream in = new FileInputStream(Consts.POM_XML);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		CompressionOutputStream out = codec.createOutputStream(baos);
		IOUtils.copyBytes(in, out, 4096, false);
		
		in.close();
		out.finish();
		byte[] compressedBytes = baos.toByteArray();
		System.out.println("size after compression: " + compressedBytes.length + " B");
		out.close();
	}

}
