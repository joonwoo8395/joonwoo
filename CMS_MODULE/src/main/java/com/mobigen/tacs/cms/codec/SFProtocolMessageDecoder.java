package com.mobigen.tacs.cms.codec;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoder;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;

import com.mobigen.tacs.cms.message.BusMessage;

/**
 * A {@link MessageDecoder} which decodes Mobigen SFProtocol Message into
 * {@link SFProtocolMessage}.
 * 
 * @author yoon
 *
 */

public class SFProtocolMessageDecoder implements MessageDecoder {

	private final Logger logger = Logger.getLogger(getClass());
	public static final long HEADER_LENGTH = 13;
	private BusMessage header = null;
	private boolean readHeader = false;

	public MessageDecoderResult decodable(IoSession session, IoBuffer in) {

		if (in.remaining() < HEADER_LENGTH) {
			logger.debug("Need more data(at least 120) to create SMSMessage : " + in.remaining());
			logger.debug(in.getHexDump());
			return MessageDecoderResult.NEED_DATA;
		}

		return MessageDecoderResult.OK;
	}

	public MessageDecoderResult decode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {

		if (!readHeader) {
			decodeHeader(session, in);
			readHeader = true;
		}

		BusMessage message = decodeBody(session, in, header.getLength());
		if (message == null) {
			return MessageDecoderResult.NEED_DATA;
		} else {
			readHeader = false;
		}
		message.setCommand(header.getCommand());
		message.setLength(header.getLength());
		out.write(message);

		return MessageDecoderResult.OK;
	}

	public BusMessage decodeBody(IoSession session, IoBuffer in, long length) {
		BusMessage message = null;

		if (length == 0) {
			return new BusMessage();
		} else {
			if (in.remaining() < length) {
				logger.debug("Need more data(at least " + length + ") to create LinkResponse : " + in.remaining());
			} else {
				message = new BusMessage();
				byte[] b = new byte[((int) length)];
				in.get(b);
				try {
					message.setmValue(new String(b, "EUC-KR"));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return message;
	}

	public void decodeHeader(IoSession session, IoBuffer in) {

		header = new BusMessage();

		byte[] command = new byte[3];
		in.get(command);
		header.setCommand((new String(command)).trim());
		byte[] length = new byte[10];
		in.get(length);
		logger.info("length : " + Integer.valueOf((new String(length))).intValue());
		header.setLength(Integer.valueOf((new String(length))).intValue());
		logger.info("header ok : " + header.toString());
	}

	public void finishDecode(IoSession session, ProtocolDecoderOutput out) {
		// Do nothing.
	}
}
