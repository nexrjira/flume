package com.nexr.sdp.parser;

import java.util.StringTokenizer;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;

/**
 * @author Daegeun Kim
 */
public class TextLogParser {
	private String prefix;
	private String suffix;
	private String delimiter;
	static final String[] ORDERS = new String[] { 
			SdpLogConstant.TRANSACTION_ID,
			SdpLogConstant.CORP_ID,
			SdpLogConstant.SYSTEM_ID,
			SdpLogConstant.SEQUENCE_NUMBER,
			SdpLogConstant.TIMESTAMP,
			SdpLogConstant.MODULE_TYPE,
			SdpLogConstant.SERVER_NAME,
			SdpLogConstant.SERVER_IP,
			SdpLogConstant.OBJECT_NAME,
			SdpLogConstant.METHOD_NAME,
			SdpLogConstant.LOG_TYPE,
			SdpLogConstant.RESULT_CODE,
			SdpLogConstant.MODULE_NAME,
			SdpLogConstant.COUNTER_PARTIP,
			SdpLogConstant.COUNTER_PARTNAME,
			SdpLogConstant.PAYLOAD
	};
	
	public static interface NoName {
		void add(String key, String value);
	}
	
	public TextLogParser() {
		this("<Start>", "<End>");
	}
	
	public TextLogParser(String prefix, String suffix) {
		this(prefix, suffix, "|");
	}
	
	public TextLogParser(String prefix, String suffix, String delimiter) {
		this.prefix = prefix;
		this.suffix = suffix;
		this.delimiter = delimiter;
	}
	
	public void parse(NoName event, String data) {
		if (validateRecord(data)) {
			data = data.substring(prefix.length(), data.length() - suffix.length());
		}
		StringTokenizer tokenizer = new StringTokenizer(data, delimiter);
		int current = 0;
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			if (ORDERS.length <= current) {
				break;
			}
			event.add(ORDERS[current], token);
			current++;
		}
	}
	
	public Object parse(LogRecord record, String data) {
		if (validateRecord(data)) {
			data = data.substring(prefix.length(), data.length() - suffix.length());
		}
		StringTokenizer tokenizer = new StringTokenizer(data, delimiter);
		int current = 0;
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			if (ORDERS.length <= current) {
				break;
			}
			record.add(ORDERS[current], token);
			current++;
		}
		return record;
	}

	private boolean validateRecord(String data) {
		return data.startsWith(prefix) && data.endsWith(suffix);
	}
}
