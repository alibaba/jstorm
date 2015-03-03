/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.message.zeroMq;

import java.nio.ByteBuffer;

import backtype.storm.serialization.KryoTupleDeserializer;

/**
 * virtualport send message
 * 
 * @author yannian/Longda
 * 
 */
public class PacketPair {
	private int port;
	private byte[] message;

	public PacketPair(int port, byte[] message) {
		this.port = port;
		this.message = message;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public byte[] getMessage() {
		return message;
	}

	public void setMessage(byte[] message) {
		this.message = message;
	}

	public static byte[] mk_packet(int virtual_port, byte[] message) {
		ByteBuffer buff = ByteBuffer.allocate((Integer.SIZE / 8)
				+ message.length);
		buff.putInt(virtual_port);
		buff.put(message);
		byte[] rtn = buff.array();
		return rtn;
	}

	public static PacketPair parse_packet(byte[] packet) {
		ByteBuffer buff = ByteBuffer.wrap(packet);
		int port = buff.getInt();

		/**
		 * @@@ Attention please, in order to reduce memory copy
		 * 
		 *     Here directly PacketPair.message use the packet buffer
		 * 
		 *     so need get rid of the target target taskid in
		 *     KryoTupleDeserializer.deserialize
		 * 
		 * 
		 *     The better design should tuple includes targetTaskId
		 */
		byte[] message = null;
		if (KryoTupleDeserializer.USE_RAW_PACKET == true) {
			message = packet;
		} else {
			message = new byte[buff.array().length - (Integer.SIZE / 8)];
			buff.get(message);
		}
		PacketPair pair = new PacketPair(port, message);

		return pair;
	}
}
