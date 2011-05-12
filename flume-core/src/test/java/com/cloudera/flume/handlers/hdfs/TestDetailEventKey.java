package com.cloudera.flume.handlers.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

public class TestDetailEventKey {

	@Test
	public void testNewObject() {
		Event e = new EventImpl();
		e.set("key1", "key1value".getBytes());
		
		List<String> keys = new ArrayList<String>();
		keys.add("key1");
		
		new DetailEventKey(e, keys);
	}
	
	@Test(expected = NullPointerException.class)
	public void testNewObjectNullKey() {
		Event e = new EventImpl();
		e.set("key1", "key1value".getBytes());
		
		new DetailEventKey(e, null);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testNewObjectEmptyKey() {
		Event e = new EventImpl();
		e.set("key1", "key1value".getBytes());
		
		List<String> keys = new ArrayList<String>();
		
		new DetailEventKey(e, keys);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testNewObjectUnmatchKey() {
		Event e = new EventImpl();
		e.set("key1", "key1value".getBytes());
		
		List<String> keys = new ArrayList<String>();
		keys.add("key2");
		
		new DetailEventKey(e, keys);
	}
	
	@Test
	public void testReadWrite() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dOut = new DataOutputStream(out);
		
		Event e = new EventImpl();
		e.set("testkey1", "testvalue1".getBytes());
		e.set("testkey2", "testvalue2".getBytes());
		List<String> keys = new ArrayList<String>();
		keys.add("testkey1");
		keys.add("testkey2");
		
		DetailEventKey key = new DetailEventKey(e, keys);
		key.write(dOut);
		
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		DataInputStream dIn = new DataInputStream(in);

		DetailEventKey key2 = new DetailEventKey(e);
		key2.readFields(dIn);
		
		Assert.assertEquals(0, key.compareTo(key2));
	}
	
	@Test
	public void testCompare() throws IOException {
		
		Event e = new EventImpl();
		e.set("testkey1", "testvalue1".getBytes());
		e.set("testkey2", "testvalue2".getBytes());
		List<String> keys = new ArrayList<String>();
		keys.add("testkey1");
		keys.add("testkey2");
		
		DetailEventKey key = new DetailEventKey(e, keys);
		
		e = new EventImpl();
		e.set("testkey1", "testvalue3".getBytes());
		e.set("testkey2", "testvalue4".getBytes());
		keys = new ArrayList<String>();
		keys.add("testkey1");
		keys.add("testkey2");
		DetailEventKey key2 = new DetailEventKey(e, keys);
		
		Assert.assertEquals(true, key.compareTo(key2)<0);
	}
	
}
