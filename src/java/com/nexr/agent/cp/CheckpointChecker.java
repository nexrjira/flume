package com.nexr.agent.cp;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.google.common.base.Preconditions;

public class CheckpointChecker<S extends EventSink> extends
		EventSinkDecorator<S> {
	
	static final Logger LOG = LoggerFactory.getLogger(CheckpointChecker.class);

	private AckListener listener;

	public CheckpointChecker(S s, AckListener l) {
		super(s);
		Preconditions.checkNotNull(l);
		this.listener = l;
	}
	
	public CheckpointChecker(S s) {
		super(s);
		this.listener = new AckListener() {
			@Override
			public void end(String group) {
				LOG.info("ended " + group);
			}

			@Override
			public void err(String group) {
				LOG.info("erred " + group);
			}

			@Override
			public void start(String group) {
				LOG.info("start " + group);
			}

		    @Override
		    public void expired(String group) throws IOException {
		        LOG.info("expired " + group);
		    }
		};
	}

	@Override
	public void close() throws IOException, InterruptedException {
		//TODO checkpoint stop Server
		super.close();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		FlumeNode.getInstance().getCheckPointManager().startServer();
		super.open();
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		byte[] btyp = e.get(CheckpointDeco.ATTR_CK_TYPE);
		
		if(btyp == null) {
			super.append(e);
			return;
		}
		
		byte[] btag = e.get(CheckpointDeco.ATTR_CK_TAG);
		String k = new String(btag);
		
		if(Arrays.equals(btyp, CheckpointDeco.CK_START)) {
			listener.start(k);
			return;
		} else if(Arrays.equals(btyp, CheckpointDeco.CK_END)) {
			listener.end(k);
			return;
		}
		super.append(e);
	}
	
	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {

			@Override
			public EventSinkDecorator<EventSink> build(Context context,
					String... argv) {
				Preconditions.checkArgument(argv.length == 0, "usage: checkpointChecker");
				//TODO 이 빌더로 생성하는 일은 없을 것이다. 
				// FlumeNode.getInstance().getCollectorAckListener()
				// 로 보내면 안된다. 
				return new CheckpointChecker<EventSink>(null, 
						FlumeNode.getInstance().getCollectorAckListener());
			}
		};
	}
}
