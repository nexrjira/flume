package com.nexr.rolling.exception;

public class RollingException extends Exception {

	private static final long serialVersionUID = 1L;

	public RollingException(Exception e) {
		super(e);
	}

	public RollingException(String message) {
		super(message);
	}

	public RollingException(final String msg, final Throwable t) {
		super(msg, t);
	}
}
