package org.elasticsearch.pql.netty.handlers;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.pql.netty.model.ResponseData;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class InboundResponseHandler extends SimpleChannelInboundHandler<ResponseData> {
	
	static final Logger log = LogManager.getLogger(InboundResponseHandler.class);

	final BlockingQueue<String> answer = new LinkedBlockingQueue<String>();

	public InboundResponseHandler() {
	}
	

	/**
	 * blocking nlp lookup function which will wait for the
	 * response and notify the caller
	 * 
	 * @return
	 */
	public String wait_for_response() {
		boolean interrupted = false;
		try {
			for (;;) {
				try {
					return answer.take();
				} catch (InterruptedException ignore) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) {
		// Send the query to lookup
		log.info("Channel Active, Sending request..");
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, ResponseData msg) {
		log.info("Channel Read0 Adding listener.." + msg);
		ctx.channel().close().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				boolean offered = answer.offer(msg.getWords());
				assert offered;
			}
		});
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		log.warn("Error in channell .." + cause.getLocalizedMessage());
		cause.printStackTrace();
		ctx.close();
	}

}
