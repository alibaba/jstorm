package backtype.storm.messaging;

import org.jboss.netty.buffer.ChannelBuffer;

public interface NettyMessage {

    boolean isEmpty();

    int getEncodedLength();

    /**
     * encode the current control message into a channel buffer
     */
    ChannelBuffer buffer() throws Exception;
}
