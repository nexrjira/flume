package com.nexr.sdp.parser;

import static org.hamcrest.CoreMatchers.is;

import org.junit.Assert;
import org.junit.Test;

import com.nexr.sdp.parser.TextLogParser.NoName;

/**
 * @author Daegeun Kim
 */
public class TestTextLogParser {
//	@Test
//	public void test() {
//		String log = "<Start>BKQ1303807758426|" +
//				"KT|SDP|1|2011-04-26 17:49:08/423|SC|" +
//				"WAMUI_2|147.6.234.2|Sample.class|" +
//				"getId()|IN_REQ|err02|SC|153.333.120.101|" +
//				"getId()|name:NaOon Choi^id=LangBam<End>";
//		
//		TextLogParser parser = new TextLogParser("<Start>", "<End>", "|");
//		long start = System.currentTimeMillis();
//		for (int i = 0; i <= 3000; i++) {
////			DummyLogRecord record = new DummyLogRecord();
////			parser.parse(record, log);
//			parser.parse(new NoName() {
//				@Override
//				public void add(String key, String value) {
////					System.out.println(key + " " + value);
//				}
//			}, log);
//		}
//		System.out.println(System.currentTimeMillis() - start);
//		
////		Assert.assertThat(log, is(composeLogRecod("<Start>", "<End>", "|", record)));
//	}
//	
//	@Test
//	public void testParse() {
//		String log = "<Start>BKQ1303807758426|" +
//				"KT|SDP|1|2011-04-26 17:49:08/423|SC|" +
//				"WAMUI_2|147.6.234.2|Sample.class|" +
//				"getId()|IN_REQ|err02|SC|153.333.120.101|" +
//				"getId()|name:NaOon Choi^id=LangBam<End>";
//		
//		TextLogParser parser = new TextLogParser("<Start>", "<End>", "|");
//		DummyLogRecord record = new DummyLogRecord();
//		parser.parse(record, log);
//		
//		Assert.assertThat(log, is(composeLogRecod("<Start>", "<End>", "|", record)));
//	}
//
//	/*
//	 * 분석한 로그데이터를 다시 병합
//	 */
//	private String composeLogRecod(String prefix, String suffix, String delimiter, DummyLogRecord record) {
//		StringBuilder sb = new StringBuilder(prefix);
//		boolean first = true;
//		for (String key : TextLogParser.ORDERS) {
//			sb.append(first ? "" : delimiter).append(record.values.get(key));
//			first = false;
//		}
//		sb.append(suffix);
//		return sb.toString();
//	}
}
