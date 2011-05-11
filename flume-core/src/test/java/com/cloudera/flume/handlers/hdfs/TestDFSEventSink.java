package com.cloudera.flume.handlers.hdfs;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;

public class TestDFSEventSink {
	@Test
	public void testBuilder() {
		Context ctx = new DFSEventSinkContext();
		DFSEventSink snk = (DFSEventSink) DFSEventSink.builder().build(ctx, "hdfs://localhost/test");
		Assert.assertEquals(TestKey.class.getName(), snk.keyClz.getName());
		Assert.assertEquals(TestValue.class.getName(), snk.valueClz.getName());
	}
	
	@Test
	public void testBuilderNotExistClass() {
		Context ctx = new DFSEventSinkContext("NotKey", "NotValue");
		DFSEventSink snk = (DFSEventSink) DFSEventSink.builder().build(ctx, "hdfs://localhost/test");
		Assert.assertEquals(WriteableEventKey.class.getName(), snk.keyClz.getName());
		Assert.assertEquals(WriteableEvent.class.getName(), snk.valueClz.getName());
	}

	/**
	 * xmlFilter => DFSEventSink(path, {keyClassName="com.cloudera.flume.handlers.hdfs.LogKey", valueClassName="com.cloudera.flume.handlers.hdfs.AttributeRecord")})
	 * 로 쓴 시퀀스 파일이 잘 쓰여졌는데 테스트 해보기 위한 테스트 케이스 
	 * 수작업으로 돌려야 한다. 

	 * @throws IOException
	 */
	public void testWriteAndRead() throws IOException {
		Path path = new Path("file:///Users/bitaholic/work/tmp/output-log.00000026.20110511-170222163+0900.1305100942163943000.seq.seq");
		FileSystem hdfs = path.getFileSystem(FlumeConfiguration.get());
		
		FlumeConfiguration conf = FlumeConfiguration.get();
		Reader reader = new SequenceFile.Reader(hdfs, path, conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long position = reader.getPosition();
		while(reader.next(key, value)) {
			String syncSeen = reader.syncSeen() ? "*" : "";
			System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
			
			LogKey logKey = (LogKey)key;
			Map<String, byte[]> keyMap = logKey.getKeyMap();
			for(String kkk : keyMap.keySet()) {
				System.out.println(kkk + " : " + new String(keyMap.get(kkk)));
			}
			System.out.println("-------");
			
			AttributeRecord r = (AttributeRecord)value;
			Map<String, byte[]> m = r.getAttrs();
			for(String k : m.keySet()) {
				System.out.println(k + " : " + new String(m.get(k)));
			}
			position = reader.getPosition();
		}
		IOUtils.closeStream(reader);
	}
	
	class DFSEventSinkContext extends Context {
		DFSEventSinkContext() {
			putValue(DFSEventSink.KEY_CLASS_NAME, "com.cloudera.flume.handlers.hdfs.TestDFSEventSink$TestKey");
			putValue(DFSEventSink.VALUE_CLASS_NAME, "com.cloudera.flume.handlers.hdfs.TestDFSEventSink$TestValue");
		}
		
		DFSEventSinkContext(String keyClzName, String valueClzName) {
			putValue(DFSEventSink.KEY_CLASS_NAME, keyClzName);
			putValue(DFSEventSink.VALUE_CLASS_NAME, valueClzName);
		}
	}
	
	class TestKey extends WriteableEventKey {
		
	}
	
	class TestValue extends WriteableEvent {
		
	}
}
