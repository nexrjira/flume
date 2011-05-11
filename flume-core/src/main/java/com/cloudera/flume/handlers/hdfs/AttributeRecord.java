package com.cloudera.flume.handlers.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

/**
 * Flume Event의 기본적인 메타데이터 및 body 는 저장하지 않고 Attribute들만 저장하고 읽는다.
 * @author bitaholic
 *
 */
public class AttributeRecord extends WriteableEvent{
	
	private Map<String, byte[]> map;
	
	public AttributeRecord(Event e) {
		super(e);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Map<String, byte[]> map = unserializeMap(in);
		e = new EventImpl(null, -1l, null, -1l, null, map);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		serializeMap(out, e.getAttrs());
	}
}