package com.nexr.agent.cp;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;

public class TestCheckpointDeco {

	@Test
	public void testBuilder() {
		CheckpointDeco.builder().build(LogicalNodeContext.testingContext());
	}
	
	@Test
	public void testCheckpointEvent() throws IOException, InterruptedException {
//		EventSink msnk = mock(EventSink.class);
		MemorySink snk = new MemorySink("sink");
		CheckPointManager manager = mock(CheckPointManagerImpl.class);
		
		final String TAG_ID = "tag1";
		
		CheckpointDeco deco = new CheckpointDeco(snk, TAG_ID.getBytes(), manager);
		
		deco.open();
		
 		EventImpl event = new EventImpl();
 		deco.append(event);
 		
 		deco.close();
 		
 		Assert.assertEquals(3, snk.getCount());
 		Assert.assertNotNull(snk.eventList);
 		Event startEvent = snk.eventList.get(0);
 		byte[] type = startEvent.get(CheckpointDeco.ATTR_CK_TYPE);
 		Assert.assertEquals(new String(CheckpointDeco.CK_START), new String(type));
 		byte[] tag = startEvent.get(CheckpointDeco.ATTR_CK_TAG);
 		Assert.assertEquals(TAG_ID, new String(tag));
 		
 		Event endEvent = snk.eventList.get(2);
 		type = endEvent.get(CheckpointDeco.ATTR_CK_TYPE);
 		Assert.assertEquals(new String(CheckpointDeco.CK_END), new String(type));
 		tag = endEvent.get(CheckpointDeco.ATTR_CK_TAG);
 		Assert.assertEquals(TAG_ID, new String(tag));
	}
	
	class MemorySink extends AccumulatorSink {
		
		List<Event> eventList = new ArrayList<Event>();

		public MemorySink(String name) {
			super(name);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void append(Event e) throws IOException, InterruptedException {
			eventList.add(e);
			super.append(e);
		}
	}
}
