package com.cloudera.flume.collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;

public class MemorySink extends AccumulatorSink {

	public List<Event> eventList = new ArrayList<Event>();

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
