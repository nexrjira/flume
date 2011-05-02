package com.nexr.agent.cp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	static Log LOG = LogFactory.getLog(CheckpointDeco.class);

	public static final String ATTR_CK_TYPE = "CheckpointType";
	public static final String ATTR_CK_TAG = "CheckpointTag";
	public static final byte[] CK_START = "CheckpointStart".getBytes(CharEncUtils.RAW);
	public static final byte[] CK_END = "CheckpointEnd".getBytes(CharEncUtils.RAW);
	
	byte[] tag;
	
	private Map<String, Long> offsetMap;
	private CheckPointManager cpManager;
	private String logicalNodeName;
	
	private boolean sentStart = false;
	private boolean isAlive = false;
	
	private RollTrigger trigger;

	private long maxAge;
		
	public CheckpointDeco(EventSink s) {
		this(s, null, null, 10000l);
	}

	public CheckpointDeco(EventSink s, String logicalNodeName, 
			CheckPointManager cpManager, long maxAge) {
		super(s);
		this.offsetMap = new HashMap<String, Long>();
		this.cpManager = cpManager;
		this.logicalNodeName = logicalNodeName;
		this.maxAge = maxAge;
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
			String fileName = new String(ByteBuffer.wrap(e.get(TailSource.A_TAILSRCFILE)).array());
			offsetMap.put(fileName, o);
		} else {
			super.append(e);
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		LOG.info("checkpoint close()");
		sendEndEvent();
		super.close();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		LOG.info("checkpointDeco open()");
		super.open();
		resetTag();
		trigger.reset();
		super.append(openEvent());
	}
	
	private void sendEndEvent() throws IOException, InterruptedException {
		super.append(closeEvent());
		if(offsetMap.size() > 0) {
			cpManager.addPendingQ(new String(tag), logicalNodeName,
					new HashMap<String, Long>(offsetMap));
			resetTag();
		}
		offsetMap.clear();
		trigger.reset();
	}
	
	/**
	 * 정해진 시간 마다 EndEvent를 보내고 현재 파일 오프셋 정보를 Manager 에게 보낸다. 
	 * 그리고 새로운 태그로 StartEvent를 보낸다. 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void rotate() throws IOException, InterruptedException {
		LOG.info("start to rotate");
		sendEndEvent();		
		super.append(openEvent());
		LOG.info("Completed rotate");
	}
	
	private void resetTag() {
		tag = (logicalNodeName + Clock.nanos()).getBytes();
		LOG.info("resetTag : " + new String(tag));
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
	
	void checkAlive() {
		if(isAlive == true) {
			isAlive = false;
		} else {
			//send end event
			
		}
		
		Thread t = new TimeChecker(2000l);
		try {
			t.sleep(2000l);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	class TimeChecker extends Thread {

		boolean doing = true;
		long checkLatencyMs;
		
		TimeChecker(long checkLatencyMs) {
			this.checkLatencyMs = checkLatencyMs;
		}
		
		@Override
		public void run() {
			while(doing) {
				checkAlive();
				try {
					Thread.sleep(checkLatencyMs);

				} catch (InterruptedException e) {
				}
			}
		}
		
	}
}
