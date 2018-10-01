package org.elasticsearch.pql.netty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.pql.netty.encoders.RequestDataEncoder;
import org.elasticsearch.pql.netty.encoders.ResponseDataDecoder;
import org.elasticsearch.pql.netty.handlers.InboundResponseHandler;
import org.elasticsearch.pql.netty.model.RequestData;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NettyClient {
	private static final Logger log = LogManager.getLogger(NettyClient.class);

	String host = "localhost";
	int port = 9191;
	
	Channel ch = null;
	InboundResponseHandler inboundChannel = new InboundResponseHandler(); 
			
	public InboundResponseHandler getInboundChannel() {
		return inboundChannel;
	}

	public void setInboundChannel(InboundResponseHandler inboundChannel) {
		this.inboundChannel = inboundChannel;
	}

	private EventLoopGroup group = new NioEventLoopGroup();

	public NettyClient() throws InterruptedException {
		//Initialize the client ...
		Bootstrap b = new Bootstrap();
		b.group(group);
		b.channel(NioSocketChannel.class);
		b.option(ChannelOption.SO_KEEPALIVE, true);
		b.handler(new LoggingHandler(LogLevel.DEBUG));
		
		b.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new RequestDataEncoder(), 
						new ResponseDataDecoder(),
						inboundChannel);
			}
		});
		
		log.info("NLP Client connecting ...");
		ch = b.connect(host, port).sync().channel();
	}
	
	@Override
	protected void finalize() throws Throwable {
		log.info("NettyClient destroying instance ...");
		super.finalize();
	}
	
	public void ShutDown() throws Exception {
		log.info("NettyClient destroying instance ...");
		ch.closeFuture().sync();
		group.shutdownGracefully();
	}

	public ChannelFuture lookup_nlp_data(String query) throws Exception {
		//Create a request and write the data
		RequestData req = new RequestData(query);
        ChannelFuture writeFuture = ch.write(req);
//        writeFuture.addListener(querySender);
        ch.flush();
        return writeFuture;
	}
	
	private final ChannelFutureListener querySender = new ChannelFutureListener() {
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (!future.isSuccess()) {
				log.info("Operation succesfully completed ...");
			} else {
				log.warn("Operation failed ...");
				future.cause().printStackTrace();
				future.channel().close();
			}
		}
	};
	
	public static void main(String []args) {
//		String sWords = "";
//		List<String> words = new ArrayList<String>(Arrays.asList(sWords.split(",")));
//		if(words == null || words.size() == 0) {
//			System.out.println(" Empty message");
//		}
//		
		ChannelFuture writeFuture;
		String query = "trump";
		String sWords = "";
		NettyClient thisInst = null;
		try {
			thisInst  = new NettyClient();
			writeFuture = thisInst.lookup_nlp_data(query);
			if (writeFuture != null) {
				writeFuture.sync();
			}
			sWords = thisInst.inboundChannel.wait_for_response();
			List<String> words = new ArrayList<String>(Arrays.asList(sWords.split(",")));
			System.out.println("Words = " + words);
			thisInst.ShutDown();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		thisInst = null;
		
		query = "america";
		sWords = "";
		try {
			thisInst  = new NettyClient();
			writeFuture = thisInst.lookup_nlp_data(query);
			if (writeFuture != null) {
				writeFuture.sync();
			}
			sWords = thisInst.inboundChannel.wait_for_response();
			List<String> words = new ArrayList<String>(Arrays.asList(sWords.split(",")));
			System.out.println("Words = " + words);
			thisInst.ShutDown();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		finally {
//			group.shutdownGracefully();
//		}
		
//		String host = "localhost";
//        int port = 9191;
//        RequestData req = new RequestData();
//        req.setRequestQuery("trump");
//        EventLoopGroup workerGroup = new NioEventLoopGroup();
//
//        try {
//            Bootstrap b = new Bootstrap();
//            b.group(workerGroup);
//            b.channel(NioSocketChannel.class);
//            b.option(ChannelOption.SO_KEEPALIVE, true);
//            b.handler(new ChannelInitializer<SocketChannel>() {
//                @Override
//                public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new RequestDataEncoder(), new ResponseDataDecoder(), 
//                    		new ClientHandler());
//                }
//            });
//
//            ChannelFuture f = b.connect(host, port).sync();
//            f.channel().closeFuture().sync();
//        } catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//            workerGroup.shutdownGracefully();
//        }
		
	}
}
