package com.nexr.data.sdp.rolling.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;



public class TextToSequence {
	
	public static void main(String args[]) throws IOException{
		String lnName = "agent1";
		String fileName = "tx.log";
		String dataType = "tx";
		
		FileReader fileReader;
		BufferedReader reader;
		
		String input = args[0];
		String output = args[1];
		
		System.out.println("Input " + input + "  output " + output);
		Configuration conf = new Configuration();
		int count =0;
		FileSystem fs = FileSystem.get(URI.create(output), conf);
		Path path = new Path(output);
		
		LogRecordKey key = new LogRecordKey();
		LogRecord record = new LogRecord();
	
		SequenceFile.Writer writer = null;
		try{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), record.getClass());
			fileReader = new FileReader(new File(input));
			reader = new BufferedReader(fileReader);
			String line;
			long offset = 0;
			while((line = reader.readLine()) != null){
				offset += line.length();
				key.setDataType(dataType);
				key.setTime(String.valueOf(System.currentTimeMillis()));
				key.setLogId(getMd5Key(line.getBytes(), fileName, lnName, offset));
				
				record.add("content", line);
				
				writer.append(key, record);
				
				count++;
			}
			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			
			IOUtils.closeStream(writer);
		}
		System.out.println("Ended ~~ " + count);
	}

	static public String getMd5Key(byte[] body, String fileName, String lnName, long offset){
		java.security.MessageDigest md5 = null;
		try {
			md5 = java.security.MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		md5.update(body);
		
		byte[] bid = md5.digest();
		int iLen = bid.length;
		StringBuffer buffer = new StringBuffer();
		
		
		for(int i=0; i<iLen; i++){
			buffer.append(String.format("%02X", 0xFF&bid[i]));
		}
		
		String res = buffer.toString()+fileName.hashCode()+lnName.hashCode()+offset;
		
		return res;
		
	}
}
