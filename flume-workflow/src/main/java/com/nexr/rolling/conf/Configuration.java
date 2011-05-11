package com.nexr.rolling.conf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class Configuration {
	protected Properties _properties;
	private final String _resourcePath;

	public Configuration() {
		_resourcePath = "config.properties";
	}

	public Configuration(final String path) {
		_properties = PropertyUtil.loadProperties(path);
		_resourcePath = PropertyUtil.getPropertiesFilePath(path);
	}

	public Configuration(File file) {
		_properties = PropertyUtil.loadProperties(file);
		_resourcePath = file.getAbsolutePath();
	}

	public String getResourcePath() {
		return _resourcePath;
	}

	public boolean containsProperty(final String key) {
		return _properties.containsKey(key);
	}

	public String getProperty(final String key) {
		final String value = _properties.getProperty(key);
		if (value == null) {
			throw new IllegalStateException("no property with key '" + key + "' found");
		}
		return value.trim();
	}

	public String getProperty(final String key, final String defaultValue) {
		String value = _properties.getProperty(key);
		if (value == null) {
			value = defaultValue;
		}
		return value;
	}

	protected void setProperty(String key, String value) {
		_properties.setProperty(key, value);
	}

	public int getInt(final String key) {
		return Integer.parseInt(getProperty(key));
	}
	
	public int getInt(final String key, int defaultValue) {
		return Integer.parseInt(getProperty(key, Integer.toString(defaultValue)));
	}
	
	public boolean getBoolean(final String key, boolean defaultValue) {
		return Boolean.parseBoolean(getProperty(key, Boolean.toString(defaultValue)));
	}

	public long getLong(final String key) {
		return Long.parseLong(getProperty(key));
	}

	public File getFile(final String key) {
		return new File(getProperty(key));
	}

	public Class<?> getClass(final String key) {
		final String className = getProperty(key);
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("can not create class " + className, e);
		}
	}

	public boolean contain(String key) {
		return getProperty(key) != null;
	}

	public void readConf(InputStream in) throws IOException {
		_properties = new Properties();
		try {
			_properties.load(in);
		} catch (final IOException e) {
			throw new RuntimeException("unable to load config.properties", e);
		}
	}

	public void writeConf(OutputStream out) throws IOException {
		_properties.store(out, this.getClass().getName());
	}

	public void readFields(DataInput in) throws IOException {
		_properties = new Properties();
		try {
			int size = in.readInt();
			byte[] contents = new byte[size];
			in.readFully(contents);

			ByteArrayInputStream input = new ByteArrayInputStream(contents);
			_properties.load(input);
		} catch (final IOException e) {
			throw new RuntimeException("unable to load config.properties", e);
		}
	}

	public void write(DataOutput output) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		_properties.store(out, this.getClass().getName());

		byte[] contents = out.toByteArray();

		output.writeInt(contents.length);
		output.write(contents);
	}

}
