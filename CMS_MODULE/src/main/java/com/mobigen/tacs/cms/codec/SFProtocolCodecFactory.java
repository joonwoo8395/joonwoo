package com.mobigen.tacs.cms.codec;

import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;

import com.mobigen.tacs.cms.message.BusMessage;

public class SFProtocolCodecFactory extends DemuxingProtocolCodecFactory {

	public SFProtocolCodecFactory(boolean server) {
		if (server) {
			super.addMessageEncoder(BusMessage.class, SFProtocolMessageEncoder.class);
			super.addMessageDecoder(SFProtocolMessageDecoder.class);
		} else {
			super.addMessageEncoder(BusMessage.class, SFProtocolMessageEncoder.class);
			super.addMessageDecoder(SFProtocolMessageDecoder.class);
		}
	}
}
