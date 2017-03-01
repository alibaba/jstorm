package com.alibaba.jstorm.yarn.server;


import com.alibaba.jstorm.yarn.appmaster.JstormMaster;
import com.alibaba.jstorm.yarn.handler.JstormAMHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import com.alibaba.jstorm.yarn.generated.*;

/**
 * Created by fengjian on 16/4/7.
 * this is thrift server in application master which handle resource management request
 */
public class AMServer {

    private static final Log LOG = LogFactory.getLog(JstormMaster.class);
    private int port;

    public AMServer(int port) {
        this.port = port;
    }

    public JstormAMHandler handler;

    public JstormAM.Processor processor;

    public boolean Start(JstormMaster jm) {
        try {
            handler = new JstormAMHandler(jm);
            processor = new JstormAM.Processor(handler);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };
            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
            return false;
        }
        return true;
    }

    public void simple(JstormAM.Processor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(port);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            LOG.info("Starting the simple server...");

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
