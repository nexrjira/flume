package com.nexr.agent.cp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.text.TailSource;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

public class CheckpointDeco extends EventSinkDecorator<EventSink> {

	public static final String ATTR_CK_TYPE = "CheckpointType";
	public static final String ATTR_CK_TAG = "CheckpointTag";
	public static final byte[] CK_START = "CheckpointStart".getBytes(CharEncUtils.RAW);
	public static final byte[] CK_END = "CheckpointEnd".getBytes(CharEncUtils.RAW);
	
	final byte[] tag;
	
	private Map<String, Long> offsetMap;
	private CheckPointManager cpManager;
		
	public CheckpointDeco(EventSink s, byte[] tag) {
		this(s, tag, null);
	}
	
	public CheckpointDeco(EventSink s, byte[]  tag, CheckPointManager cpManager) {
		super(s);
		this.tag = tag;
		this.offsetMap = new HashMap<String, Long>();
		this.cpManager = cpManager;
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		// TODO Offset정보를 담은 Event가 오면 넘기지 말고 offset과 파일이름을 저장하고 버린다.
		byte[] offset = e.get(TailSource.A_TAILSRCOFFSET);
		if(offset != null) {
			long o = ByteBuffer.wrap(offset).asLongBuffer().get();
			//TODO 파일이름이 메타 정보로 없을 때 처리 
			String fileName = ByteBuffer.wrap(e.get(TailSource.A_TAILSRCFILE)).toString();
			offsetMap.put(fileName, o);
		} else {
			super.append(e);
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		// TODO RollSink가 현재의 Sink를 닫았기 때문에 EndEvent를 보내줘야 한다.
		super.append(closeEvent());
		cpManager.addPendingQ(new String(tag), offsetMap);
		super.close();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		// RollSink가 새로운 Sink를 생성했기 때문에 StartEvent를 보내줘야 한다.
		super.open();
		super.append(openEvent());
	}

	public Event openEvent() {
		Event e = new EventImpl(new byte[0]);
		e.set(ATTR_CK_TYPE, CK_START);
		e.set(ATTR_CK_TAG, tag);
		return e;
	}
	
	public Event closeEvent() {
		Event e = new EventImpl(new byte[0]);
		e.set(ATTR_CK_TYPE, CK_END);
		e.set(ATTR_CK_TAG, tag);
		return e;
	}

	public void setCpManager(CheckPointManager cpManager) {
		this.cpManager = cpManager;
	}
	
	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {

			@Override
			public EventSinkDecorator<EventSink> build(Context context,
					String... argv) {
				Preconditions.checkArgument(argv.length == 0, "usage: checkpointInjector");
				//TODO context.getValue(LogicalNodeContext.C_LOGICAL) 이 항상 있는가? 
				return new CheckpointDeco(null, (context.getValue(LogicalNodeContext.C_LOGICAL) + Clock.nanos()).getBytes(), 
						FlumeNode.getInstance().getCheckPointManager());
			}
		};
	}
}
