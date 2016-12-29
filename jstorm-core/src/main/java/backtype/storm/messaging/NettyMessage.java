package backtype.storm.messaging;

import org.jboss.netty.buffer.ChannelBuffer;

public interface NettyMessage {
    
    public boolean isEmpty();
    
    public int getEncodedLength();
    
    /**
     * encode the current Control Message into a channel buffer
     *
     * @throws Exception
     */
    public ChannelBuffer buffer() throws Exception ;
}
