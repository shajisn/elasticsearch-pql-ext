package org.elasticsearch.pql.netty.model;

import java.nio.charset.Charset;

public class ResponseData {
	private final Charset charset = Charset.forName("UTF-8");
	
	private String sWords;

    public void setWords(String sWords) {
        this.sWords = sWords;
    }
    
    public String getWords() {
        return this.sWords;
    }
    
    public byte[] getDataBytes() {
		return this.sWords.getBytes(charset);
	}
	
	public int getLength() {
		return this.sWords.length();
	}

    @Override
    public String toString() {
        return "ResponseData [" + "Words =" + this.sWords + ']';
    }
}
