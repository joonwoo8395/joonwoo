package com.mobigen.tacs.cms.message;

public class BusMessage {
	// private static final long serialVersionUID = 38360000001L;

	private int mLength;
	private String mCommand;
	private String mValue;

	public String getmValue() {
		return mValue;
	}

	public void setmValue(String mValue) {
		this.mValue = mValue;
	}

	public int getLength() {
		return this.mLength;
	}

	public void setLength(int length) {
		this.mLength = length;
	}

	public String getCommand() {
		return this.mCommand;
	}

	public void setCommand(String command) {
		this.mCommand = command;
	}

	public String toString() {
		return this.mCommand + this.toString(this.mLength);
	}

	private String toString(int length) {
		String str = String.format("%010d", this.mLength) + this.mValue;
		return str;
	}
}