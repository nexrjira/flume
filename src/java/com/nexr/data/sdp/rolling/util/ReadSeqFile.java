package com.nexr.data.sdp.rolling.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReadSeqFile {
	public static void main(String args[]) throws IOException{
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		if (fs.getFileStatus(path).isDir()) {
			for (FileStatus file : fs.listStatus(path)) {
				readFile(conf, file.getPath());
			}
		} else {
			readFile(conf, path);
		}
	}

	private static void readFile(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			long position = reader.getPosition();
			while(reader.next(key, value)){
				System.out.println(key + " " + value);
			}
		}finally{
			IOUtils.closeStream(reader);
		}
	}
}
