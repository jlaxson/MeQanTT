package com.albin.mqtt.netty;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.albin.mqtt.message.PingReqMessage;
import com.albin.mqtt.message.PingRespMessage;

public class HeartbeatHandler extends SimpleChannelHandler {
	private static Logger LOGGER = LoggerFactory.getLogger(HeartbeatHandler.class);
	
	private final int interval;
	private final int retries;
	private final int timeout;
	
	private final AtomicInteger retryCount = new AtomicInteger(0);
	
	private final AtomicBoolean connectionDied = new AtomicBoolean(false);
	private CountDownLatch heartbeatLatch;
	
	public HeartbeatHandler() {
		this(5000, 2, 1000);
	}
	
	public HeartbeatHandler(int interval, int retries, int timeout) {
		this.interval = interval;
		this.retries = retries;
		this.timeout = timeout;
	}

	@Override
	public void channelConnected(final ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		new Thread() {
			public void run() {
				while (!connectionDied.get()) {
					try {
						Thread.sleep(interval);
					} catch (InterruptedException e) { }
					
					sendHeartbeat(ctx.getChannel());
					
					try {
						if (heartbeatLatch.await(timeout, TimeUnit.MILLISECONDS)) {
							heartbeatReceived(ctx.getChannel());
						} else {
							heartbeatFailed(ctx.getChannel());
						}
					} catch (InterruptedException e) {
						heartbeatFailed(ctx.getChannel());
					}
				}
			}
		}.start();
		super.channelConnected(ctx, e);
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		connectionDied.set(true);
		super.channelDisconnected(ctx, e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		if (e.getMessage() instanceof PingRespMessage && heartbeatLatch != null) {
			heartbeatLatch.countDown();
		}
		super.messageReceived(ctx, e);
	}
	
	private void sendHeartbeat(Channel channel) {
		PingReqMessage msg = new PingReqMessage();
		
		heartbeatLatch = new CountDownLatch(1);
		
		channel.write(msg);
	}
	
	private void heartbeatReceived(Channel c) {
		LOGGER.debug("Heartbeat received");
		retryCount.set(0);
	}
	
	private void heartbeatFailed(Channel c) {
		int count = retryCount.incrementAndGet();
		LOGGER.warn("Did not receive heartbeat");
		if (count >= this.retries) {
			LOGGER.error("Heartbeat retries exceeded, closing channel");
			c.close();
		}
	}
}
