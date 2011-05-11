package com.cloudera.flume.handlers.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.google.common.base.Preconditions;

public class DetailEventKey extends WriteableEventKey {
	static final Logger LOG = LoggerFactory.getLogger(DetailEventKey.class);
	
	/**
	 * 레코드 키에 들어갈 속성 키 들 
	 */
	private Map<String, byte[]> keyMap;
	private Event e;
	
	/**
	 * 
	 * @param e : Flume의 Event
	 * @param key e 에서 시퀀스 파일의 레코드 키를 생성할 때 쓰일 Map의 속성 키들 
	 */
	public DetailEventKey(Event e, List<String> keys) {
		this(e);
		init(keys);
	}
	
	public DetailEventKey(Event e) {
		this.e = e;
	}
	
	public DetailEventKey() {
		this.e = null;
		this.keyMap = null;
	}
	
	protected void init(List<String> keys) {
		Preconditions.checkNotNull(e, "Event is null");
		Preconditions.checkNotNull(keys, "Key list are empty");
		Preconditions.checkArgument((keys.size() > 0), "Key List are empty");
		this.keyMap = new HashMap<String, byte[]>();
		
		for(String key : keys) {
			if(!e.getAttrs().containsKey(key)) {
				throw new IllegalArgumentException("There is no key in Event : " + key);
			}
			keyMap.put(key, e.get(key));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if(keyMap == null) {
			keyMap = new HashMap<String, byte[]>();
		}
		int sz = in.readInt();
		for(int i=0; i<sz; i++) {
			String key = in.readUTF();
			int l = in.readInt();
			byte[] value = new byte[l];
			in.readFully(value);
			keyMap.put(key, value);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(keyMap.size());
		for(String key : keyMap.keySet()) {
			out.writeUTF(key);
			byte[] v = keyMap.get(key);
			out.writeInt(v.length);
			out.write(v);
		}
	}

	/**
	 * Map에 들어가 있는 순서대로 비교한다. 
	 */
	@Override
	public int compareTo(WriteableEventKey k) {
		if(!(k instanceof DetailEventKey)) {
			throw new ClassCastException("Comparing different types of objects");
		}
		DetailEventKey other = (DetailEventKey)k;
		if(this.keyMap.size() != other.getKeyMap().size()) {
			throw new IllegalArgumentException("Map size are not same");
		}
		
		int result = 0;
		
		for(String key : keyMap.keySet()) {
			result = new String(keyMap.get(key)).compareTo(new String(other.keyMap.get(key)));
			if(result != 0) {
				return result;
			}
		}
		return result;
	}
	
	public Map<String, byte[]> getKeyMap() {
		return keyMap;
	}

	public void setKeyMap(Map<String, byte[]> keyMap) {
		this.keyMap = keyMap;
	}

	@Override
	public int hashCode() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			for(byte[] b : keyMap.values()) {
					out.write(b);
			}
			out.flush();
		} catch (IOException e) {
			LOG.error("Failed to write keys to array");
		} 
		return ByteBuffer.wrap(out.toByteArray()).hashCode();
	}
}