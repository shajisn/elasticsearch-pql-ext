package org.elasticsearch.pql.netty.model;

import java.nio.charset.Charset;

public class RequestData {
	private final Charset charset = Charset.forName("UTF-8");

	private String requestQuery;
	
	public RequestData(String requestQuery) {
		this.requestQuery = requestQuery;
	}
	
	public String getRequestQuery() {
		return requestQuery;
	}

	public void setRequestQuery(String requestQuery) {
		this.requestQuery = requestQuery;
	}

	public byte[] getDataBytes() {
		return this.requestQuery.getBytes(charset);
	}
	
	public int getLength() {
		return this.requestQuery.length();
	}
	
	@Override
	public String toString() {
		return "RequestData [" + requestQuery + "]";
	}

}
