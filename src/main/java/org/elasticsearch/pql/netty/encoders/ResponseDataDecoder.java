package org.elasticsearch.pql.netty.encoders;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.pql.netty.model.ResponseData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

public class ResponseDataDecoder extends ReplayingDecoder<ResponseData> {
	
	private static final Logger log = LogManager.getLogger(ResponseDataDecoder.class);
	
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		
    	log.info("ResponseDataDecoder decoding ... ");
    	
		if (!in.isReadable())
	        return;
		
		byte[] bytes;
		int length = in.readInt();
		if(length > 25000) {
			log.info("Cannot process Response message with buffer length = " + length);
			return;
		}
		log.info("Response buffer length = " + length);
		if (in.hasArray()) {
		    bytes = in.array();
		} else {
		    bytes = new byte[length];
		    in.getBytes(in.readerIndex(), bytes);
		}
		
		
		String response = new String(bytes);
		log.info("ResponseDataDecoder decoded message = " + response);
		ResponseData data = new ResponseData();
		data.setWords(response);
		out.add(data);
	}
}