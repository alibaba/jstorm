package com.alibaba.jstorm.yarn.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.login.Configuration;


/**
 * Interface for Thrift Transport plugin
 */
public interface ITransportPlugin {
    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration 
     * @param login_conf login configuration
     */
    void prepare(Map storm_conf, Configuration login_conf);
    
    /**
     * Create a server associated with a given port and service handler
     * @param port listening port
     * @param processor service handler
     * @return server to be binded
     */
    public TServer getServer(int port, TProcessor processor) throws IOException, TTransportException;

    /**
     * Connect to the specified server via framed transport 
     * @param transport The underlying Thrift transport.
     * @param serverHost server host
     */
    public TTransport connect(TTransport transport, String serverHost) throws IOException, TTransportException;
}
