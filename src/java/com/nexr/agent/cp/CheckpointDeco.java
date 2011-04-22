package com.nexr.agent.cp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.handlers.text.TailSource;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

public class CheckpointDeco extends EventSinkDecorator<EventSink> {

	public static final String ATTR_CK_TYPE = "CheckpointType";
	public static final String ATTR_CK_TAG = "CheckpointTag";
	public static final byte[] CK_START = "CheckpointStart".getBytes(CharEncUtils.RAW);
	public static final byte[] CK_END = "CheckpointEnd".getBytes(CharEncUtils.RAW);
	
	byte[] tag;
	
	private Map<String, Long> offsetMap;
	private CheckPointManager cpManager;
	private String logicalNodeName;
	
	private RollTrigger trigger;
		
	public CheckpointDeco(EventSink s) {
		this(s, null, null, 10000l);
	}

	public CheckpointDeco(EventSink s, String logicalNodeName, 
			CheckPointManager cpManager, long maxAge) {
		super(s);
		this.offsetMap = new HashMap<String, Long>();
		this.cpManager = cpManager;
		this.logicalNodeName = logicalNodeName;
		this.trigger = new TimeTrigger(maxAge);
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		if(trigger.isTriggered()) {
			rotate();
		}
		
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
		super.append(closeEvent());
		cpManager.addPendingQ(new String(tag), logicalNodeName, offsetMap);
		super.close();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		super.open();
		resetTag();
		trigger.reset();
		super.append(openEvent());
	}
	
	private void rotate() throws IOException, InterruptedException {
		super.append(closeEvent());
		cpManager.addPendingQ(new String(tag), logicalNodeName,
				new HashMap<String, Long>(offsetMap));
		
		resetTag();
		offsetMap.clear();
		trigger.reset();
		super.append(openEvent());
	}
	
	private void resetTag() {
		tag = (logicalNodeName + Clock.nanos()).getBytes();
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
				Preconditions.checkArgument(argv.length <= 1, "usage: checkpointInjector[(collector, maxMillis)]");
				Preconditions.checkNotNull(context.getValue(LogicalNodeContext.C_LOGICAL),
						"Context does not have a logical node name");
				
				
				FlumeConfiguration conf = FlumeConfiguration.get();
				long delayMillis = conf.getAgentLogMaxAge();
				if(argv.length >= 1) {
					delayMillis = Long.parseLong(argv[0]);
				}
				String logicalNodeName = context.getValue(LogicalNodeContext.C_LOGICAL);
				return new CheckpointDeco(null, logicalNodeName,
						FlumeNode.getInstance().getCheckPointManager(), delayMillis);
			}
		};
	}
}
