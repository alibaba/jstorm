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
package backtype.storm;

import backtype.storm.generated.DRPCRequest;
import backtype.storm.utils.ServiceRegistry;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.drpc.Drpc;
import com.alibaba.jstorm.utils.PathUtils;
import java.io.File;
import java.util.Map;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LocalDRPC implements ILocalDRPC {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDRPC.class);

    private final Drpc handler = new Drpc();
    private final Thread thread;
    private final String serviceId;

    public LocalDRPC() {
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                LOG.info("Begin to init local Drpc");
                try {
                    killOldDrpcPids();
                    handler.init();
                } catch (Exception e) {
                    LOG.info("Failed to  start local drpc");
                    System.exit(-1);
                }
                LOG.info("Successfully started local drpc");
            }
        });
        thread.start();

        serviceId = ServiceRegistry.registerService(handler);
    }

    public void killOldDrpcPids() throws Exception {
        Map conf = StormConfig.read_storm_config();
        LOG.info("Configuration is \n" + conf);
        String pidDir = StormConfig.drpcPids(conf);
        File file = new File(pidDir);

        if (!file.exists())
            return;


        String[] existPids = file.list();
        if (existPids == null) {
            return;
        }

        for (String existPid : existPids) {
            try {
                PathUtils.rmpath(pidDir + File.separator + existPid);
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }
    }

    @Override
    public String execute(String functionName, String funcArgs) {
        try {
            return handler.execute(functionName, funcArgs);
        } catch (Exception e) {
            LOG.error("Local DRPC execute error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void result(String id, String result) throws TException {
        handler.result(id, result);
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws TException {
        return handler.fetchRequest(functionName);
    }

    @Override
    public void failRequest(String id) throws TException {
        handler.failRequest(id);
    }

    @Override
    public void shutdown() {
        ServiceRegistry.unregisterService(this.serviceId);
        this.handler.shutdown();
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

}
