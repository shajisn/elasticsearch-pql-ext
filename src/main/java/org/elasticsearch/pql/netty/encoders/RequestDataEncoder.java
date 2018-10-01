package org.elasticsearch.pql.netty.encoders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.pql.netty.model.RequestData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RequestDataEncoder extends MessageToByteEncoder<RequestData> {

	private static final Logger log = LogManager.getLogger(RequestDataEncoder.class);

	@Override
	protected void encode(ChannelHandlerContext ctx, RequestData msg, ByteBuf out) throws Exception {
		log.info("RequestDataEncoder encoding message : " + msg.getRequestQuery());
		int length = msg.getLength();
		byte[] data = msg.getDataBytes();
		ByteBuf buff = ctx.alloc().buffer(length);
		buff.writeBytes(data);
		out.writeInt(length);
		out.writeBytes(buff);
	}
}
