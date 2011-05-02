package com.cloudera.flume.handlers.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

public class XmlFilter extends EventSinkDecorator<EventSink> {
	Log LOG = LogFactory.getLog(XmlFilter.class);
	private SAXParser sax;

	public XmlFilter(EventSink s) {
		super(s);
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		ByteArrayInputStream bis = new ByteArrayInputStream(e.getBody());
		try {
			sax.parse(bis, new TransactionXmlHandler(e));
		} catch (SAXException e1) {
			e1.printStackTrace();
		}
		super.append(e);
	}

	static final String SYSTEM_HEADER = "SystemHeader";
	static final String DATA_HEADER = "DataHeader";
	static final String BODY = "Body";
	static final String TRANSACTION_LOG = "TransactionLog";

	class TransactionXmlHandler extends DefaultHandler {
		String currentTag;
		String parentTag;
		String text;
		Event e;

		TransactionXmlHandler(Event e) {
			this.e = e;
		}

		@Override
		public void startDocument() throws SAXException {
			super.startDocument();
		}

		@Override
		public void endDocument() throws SAXException {
			super.endDocument();
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if (qName.equals(SYSTEM_HEADER) || qName.equals(DATA_HEADER)
					|| qName.equals(BODY)) {
				parentTag = qName;
			} else {
				currentTag = qName;
			}
			super.startElement(uri, localName, qName, attributes);
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			super.endElement(uri, localName, qName);
			if (qName.equals(SYSTEM_HEADER) || qName.equals(DATA_HEADER)
					|| qName.equals(BODY) || qName.equals(TRANSACTION_LOG)) {
			} else {
				e.set(parentTag + "." + currentTag, text.getBytes());
			}

		}

		@Override
		public void characters(char[] ch, int start, int length)
				throws SAXException {
			text = new String(ch, start, length);
			super.characters(ch, start, length);
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		super.close();
	}

	@Override
	public void open() throws IOException, InterruptedException {
		LOG.info("init sax parser");
		try {
			sax = SAXParserFactory.newInstance().newSAXParser();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		super.open();
	}

	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {
			@Override
			public EventSinkDecorator<EventSink> build(Context context,
					String... argv) {
				Preconditions.checkArgument(argv.length == 0,
						"usage: xmlFilter");
				return new XmlFilter(null);
			}
		};
	}

}
