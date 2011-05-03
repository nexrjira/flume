package com.cloudera.flume.handlers.hdfs;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.flume.conf.Context;

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
