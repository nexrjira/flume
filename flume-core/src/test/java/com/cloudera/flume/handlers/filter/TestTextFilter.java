package com.cloudera.flume.handlers.filter;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;

public class TestTextFilter {
	@Test
	public void testBuild() throws FlumeSpecException {
	    String spec = "{textfilter => null}";
	    FlumeBuilder.buildSink(new Context(), spec);
	}
	
	@Test
	public void testBuild1() throws FlumeSpecException {
	    String spec = "{textfilter(\"<Start>\") => null}";
	    FlumeBuilder.buildSink(new Context(), spec);
	}
	
	@Test
	public void testBuild2() throws FlumeSpecException {
	    String spec = "{textfilter(\"<Start>\", \"<End>\") => null}";
	    FlumeBuilder.buildSink(new Context(), spec);
	}
	
	@Test
	public void testBuild3() throws FlumeSpecException {
	    String spec = "{textfilter(\"<Start>\", \"<End>\", \"|\") => null}";
	    FlumeBuilder.buildSink(new Context(), spec);
	}
	
	@Test
	public void testParseText() throws FlumeSpecException, IOException, InterruptedException {
		String log = "<Start>BKQ1303807758426|" +
			"KT|SDP|1|2011-04-26 17:49:08/423|SC|" +
			"WAMUI_2|147.6.234.2|Sample.class|" +
			"getId()|IN_REQ|err02|SC|153.333.120.101|" +
			"getId()|name:NaOon Choi^id=LangBam<End>";
	    String spec = "{textfilter => null}";
	    EventSink sink = FlumeBuilder.buildSink(new Context(), spec);
	    sink.open();
	    sink.append(new EventImpl(log.getBytes()));
	    sink.close();
	}
}
