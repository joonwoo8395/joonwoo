package com.mobigen.tacs.cms.codec;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.MessageEncoder;

import com.mobigen.tacs.cms.message.BusMessage;

public class SFProtocolMessageEncoder<T extends BusMessage> implements MessageEncoder<T> {

	public void encode(IoSession session, T message, ProtocolEncoderOutput out) throws Exception {
		IoBuffer buffer = IoBuffer.allocate(300);
		buffer.setAutoExpand(false);

		encodeMessage(session, message, buffer);
		buffer.flip();
		out.write(buffer);
	}

	public void encodeMessage(IoSession session, T message, IoBuffer out) {
//		System.out.println("send msg : " + message.toString());
		try {
			byte[] data = message.toString().getBytes("EUC-KR");
			out.put(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
