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
package com.albin.mqtt;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.albin.mqtt.message.ConnectMessage;
import com.albin.mqtt.message.DisconnectMessage;
import com.albin.mqtt.message.PingReqMessage;
import com.albin.mqtt.message.PublishMessage;
import com.albin.mqtt.message.QoS;
import com.albin.mqtt.message.SubscribeMessage;
import com.albin.mqtt.message.UnsubscribeMessage;
import com.albin.mqtt.netty.MqttMessageDecoder;
import com.albin.mqtt.netty.MqttMessageEncoder;
import com.albin.mqtt.netty.MqttMessageHandler;

public class NettyClient {

	private Channel channel;
	private ClientBootstrap bootstrap;
	private final String id;
	private MqttListener listener;
	private MqttMessageHandler handler;

	public NettyClient(String id) {
		this.id = id;
	}
	
	public void setListener(MqttListener listener) {
		this.listener = listener;
		if (handler != null) {
			handler.setListener(listener);
		}
	}

	/* (non-Javadoc)
	 * @see com.albin.mqtt.MqttClient#connect(java.lang.String, int)
	 */
	public void connect(String host, int port) {
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		handler = new MqttMessageHandler();
		handler.setListener(listener);
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(
						new MqttMessageEncoder(),
						new MqttMessageDecoder(), 
						new HeartbeatHandler(5000, 3, 1000),
						handler);
			}
		});

		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
				port));

		channel = future.awaitUninterruptibly().getChannel();
		if (!future.isSuccess()) {
			future.getCause().printStackTrace();
			bootstrap.releaseExternalResources();
			return;
		}

		channel.write(new ConnectMessage(id, true, 30));
		// TODO: Should probably wait for the ConnAck message
	}

	/* (non-Javadoc)
	 * @see com.albin.mqtt.MqttClient#disconnect()
	 */
	public void disconnect() {
		channel.write(new DisconnectMessage()).awaitUninterruptibly();
		channel.close().awaitUninterruptibly();
		bootstrap.releaseExternalResources();
	}

	/* (non-Javadoc)
	 * @see com.albin.mqtt.MqttClient#subscribe(java.lang.String)
	 */
	public void subscribe(String topic) {
		channel.write(new SubscribeMessage(topic, QoS.AT_MOST_ONCE));
	}

	/* (non-Javadoc)
	 * @see com.albin.mqtt.MqttClient#unsubscribe(java.lang.String)
	 */
	public void unsubscribe(String topic) {
		channel.write(new UnsubscribeMessage(topic));
	}

	/* (non-Javadoc)
	 * @see com.albin.mqtt.MqttClient#publish(java.lang.String, java.lang.String)
	 */
	public void publish(String topic, String msg) {
		channel.write(new PublishMessage(topic, msg));
	}

	/* (non-Javadoc)
	 * @see com.albin.mqtt.MqttClient#ping()
	 */
	public void ping() {
		channel.write(new PingReqMessage());
	}

}
