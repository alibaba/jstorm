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
