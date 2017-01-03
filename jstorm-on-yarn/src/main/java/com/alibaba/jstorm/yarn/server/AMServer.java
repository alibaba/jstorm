package com.alibaba.jstorm.yarn.server;


import com.alibaba.jstorm.yarn.appmaster.JstormMaster;
import com.alibaba.jstorm.yarn.handler.JstormAMHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

// Generated code
import com.alibaba.jstorm.yarn.generated.*;

//this is thrift server in application master which handle resource management request
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
//            TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

            // Use this for a multithreaded server
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            LOG.info("Starting the simple server...");

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void secure(JstormAM.Processor processor) {
        try {
      /*
       * Use TSSLTransportParameters to setup the required SSL parameters. In this example
       * we are setting the keystore and the keystore password. Other things like algorithms,
       * cipher suites, client auth etc can be set.
       */
            TSSLTransportParameters params = new TSSLTransportParameters();
            // The Keystore contains the private key
            params.setKeyStore("../../lib/java/test/.keystore", "thrift", null, null);

      /*
       * Use any of the TSSLTransportFactory to get a server transport with the appropriate
       * SSL configuration. You can use the default settings if properties are set in the command line.
       * Ex: -Djavax.net.ssl.keyStore=.keystore and -Djavax.net.ssl.keyStorePassword=thrift
       *
       * Note: You need not explicitly call open(). The underlying server socket is bound on return
       * from the factory class.
       */
            TServerTransport serverTransport = TSSLTransportFactory.getServerSocket(9091, 0, null, params);
            TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

            // Use this for a multi threaded server
            // TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the secure server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
