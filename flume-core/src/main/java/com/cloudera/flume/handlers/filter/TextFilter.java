package com.cloudera.flume.handlers.filter;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;
import com.nexr.sdp.parser.TextLogParser;

/**
 * @author Daegeun Kim
 */
public class TextFilter extends EventSinkDecorator<EventSink> {
	private Logger LOG = LoggerFactory.getLogger(getClass());
	
	private String prefix;
	private String suffix;
	private String delimiter;

	public TextFilter(EventSink s, String prefix, String suffix, String delimiter) {
		super(s);
		this.prefix = prefix;
		this.suffix = suffix;
		this.delimiter = delimiter;
	}

	@Override
	public void append(final Event e) throws IOException, InterruptedException {
		TextLogParser parser = new TextLogParser(prefix, suffix, delimiter);
		parser.parse(new TextLogParser.NoName() {
			@Override
			public void add(String key, String value) {
				e.set(key, value.getBytes());
			}
		}, new String(e.getBody()));
//		DummyLogRecord record = new DummyLogRecord();
//		parser.parse(record, new String(e.getBody()));
//		for (Map.Entry<String, Object> entry : record.values().entrySet()) {
//			String value = entry.getValue().toString();
//			e.set(entry.getKey(), value.getBytes());
//		}
		super.append(e);
	}

	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {
			@Override
			public EventSinkDecorator<EventSink> build(Context context, String... argv) {
				Preconditions.checkArgument(argv.length >= 0, "usage: textfilter([prefix=\"<Start>\"[,suffix=\"<End>\"[,delimiter=\"|\"]]])");

				String prefix = "<Start>";
				String suffix = "<End>";
				String delimiter = "|";
				if (argv.length >= 1) {
					prefix = argv[0];
				}
				if (argv.length >= 2) {
					suffix = argv[1];
				}
				if (argv.length >= 3) {
					delimiter = argv[2];
				}
				EventSinkDecorator<EventSink> snk = new TextFilter(null, prefix, suffix, delimiter);
				return snk;
			}
		};
	}
}
