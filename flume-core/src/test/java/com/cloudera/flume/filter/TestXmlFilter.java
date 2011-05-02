package com.cloudera.flume.handlers.filter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Test;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TestXmlFilter {
	@Test
	public void testXmlParser() throws ParserConfigurationException, SAXException, IOException {
		SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
		FileInputStream fis = new FileInputStream("/Users/bitaholic/work/xml-input/xmlinputsingle.xml");
		parser.parse(fis, new MyHandler());
	}
	
	class MyHandler extends DefaultHandler {
		String currentTag;
		String text;

		@Override
		public void startDocument() throws SAXException {
			System.out.println("startDocument()");
			super.startDocument();
		}

		@Override
		public void endDocument() throws SAXException {
			System.out.println("endDocument()");
			super.endDocument();
		}
		
		

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			System.out.println("startElm : " + localName +", qName : " +qName + ", uri : " + uri);
			currentTag = localName;
			super.startElement(uri, localName, qName, attributes);
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			System.out.println("endElm : " + localName + ", qName : " + qName + ", uri : " + uri);
			super.endElement(uri, localName, qName);
		}

		@Override
		public void characters(char[] ch, int start, int length)
				throws SAXException {
			System.out.println("character : " + new String(ch, start, length));
			super.characters(ch, start, length);
		}
	}
}
