/*******************************************************************************
 * Copyright 2011 Albin Theander
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.albin.mqtt.netty;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.albin.mqtt.MqttListener;
import com.albin.mqtt.message.ConnAckMessage;
import com.albin.mqtt.message.Message;
import com.albin.mqtt.message.PingReqMessage;
import com.albin.mqtt.message.PingRespMessage;
import com.albin.mqtt.message.PubAckMessage;
import com.albin.mqtt.message.PublishMessage;
import com.albin.mqtt.message.QoS;

public class MqttMessageHandler extends SimpleChannelHandler {
	
	private MqttListener listener;

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		System.out.println("Caught exception: " + e.getCause());
		e.getChannel().close();
	}
	
	@Override
	public void channelDisconnected(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		super.channelDisconnected(ctx, e);
		if (listener != null) {
			listener.disconnected();
		}
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		handleMessage(ctx, (Message) e.getMessage());
	}
	
	private void handleMessage(ChannelHandlerContext ctx, Message msg) {
		if (msg == null) {
			return;
		}
		switch (msg.getType()) {
		case CONNACK:
			handleMessage(ctx, (ConnAckMessage) msg);
			break;
		case PUBLISH:
			handleMessage(ctx, (PublishMessage) msg);
			break;
		case PINGREQ:
			handleMessage(ctx, (PingReqMessage) msg);
			break;
		case PINGRESP:
			handleMessage(ctx, (PingRespMessage) msg);
		default:
			break;
		}
		
		
	}

	private void handleMessage(ChannelHandlerContext ctx, ConnAckMessage msg) {
		if (listener != null) {
			listener.connected();
		}
	}

	private void handleMessage(ChannelHandlerContext ctx, PublishMessage msg) {
		if (listener != null) {
			listener.publishArrived(msg.getTopic(), msg.getData());
			if (msg.getQos() == QoS.AT_LEAST_ONCE) {
				acknowledgeMessage(ctx, msg);
			}
		}
	}
	
	private void handleMessage(ChannelHandlerContext ctx, PingReqMessage msg) {
		PingRespMessage reply = new PingRespMessage();
		ctx.getChannel().write(reply);
	}
	
	private void handleMessage(ChannelHandlerContext ctx, PingRespMessage msg) {
		
	}

	private void acknowledgeMessage(ChannelHandlerContext ctx, PublishMessage msg) {
		PubAckMessage reply = new PubAckMessage(msg.getMessageId());
		ctx.getChannel().write(reply);
	}

	public void setListener(MqttListener listener) {
		this.listener = listener;
	}


}
