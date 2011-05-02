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
import com.cloudera.util.Clock;

public class TestCheckpointDeco {

	@Test
	public void testBuilder() {
		CheckpointDeco.builder().build(LogicalNodeContext.testingContext());
	}
	
	@Test
	public void testCheckpointEvent() throws IOException, InterruptedException {
		MemorySink snk = new MemorySink("sink");
		CheckPointManager manager = mock(CheckPointManagerImpl.class);
		
		CheckpointDeco deco = new CheckpointDeco(snk, "node1", manager, 10000l);
		
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
 		Assert.assertEquals(new String(deco.tag), new String(tag));
 		
 		Event endEvent = snk.eventList.get(2);
 		type = endEvent.get(CheckpointDeco.ATTR_CK_TYPE);
 		Assert.assertEquals(new String(CheckpointDeco.CK_END), new String(type));
 		tag = endEvent.get(CheckpointDeco.ATTR_CK_TAG);
 		Assert.assertEquals(new String(deco.tag), new String(tag));
	}
	
	@Test
	public void testRotate() throws IOException, InterruptedException {
		MemorySink snk = new MemorySink("sink");
		CheckPointManager manager = mock(CheckPointManagerImpl.class);
		
		CheckpointDeco deco = new CheckpointDeco(snk, "node1", manager, 2000l);
		
		deco.open(); // -> startEvent
		deco.append(new EventImpl()); // -> dataEvent
		Clock.sleep(3000l); // sleep 3 sec -> rotate (end->start)
		deco.append(new EventImpl());// -> dataEvent
		deco.append(new EventImpl());// -> dataEvent
		deco.close(); // ->endEvent
		
		Assert.assertEquals(7, snk.getCount());
	}
	
	/**
	* 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testFlush() throws IOException, InterruptedException {
		MemorySink snk = new MemorySink("Sink");
		CheckPointManager manager = mock(CheckPointManagerImpl.class);
		
		CheckpointDeco deco = new CheckpointDeco(snk, "node1", manager, 2000l);
		
		deco.open(); // send start event 1
		deco.append(new EventImpl()); // send data event 2
		deco.append(new EventImpl()); // send data event 3
		deco.append(new EventImpl()); // send data event 4
		Clock.sleep(3000l);
		
		//rotate // send close event 5
		
		Assert.assertEquals(5, snk.getCount());
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
