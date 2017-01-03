package com.alibaba.jstorm.elasticsearch.common;

import java.net.UnknownHostException;

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class StormEsClient extends PreBuiltTransportClient {
  public StormEsClient(EsConfig esConfig) throws UnknownHostException {
    super(esConfig.getSettings());
    this.addTransportAddresses(esConfig.getTransportAddresses().toArray(
        new TransportAddress[0]));
  }
}
