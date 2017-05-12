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
package backtype.storm.command;

import backtype.storm.utils.CommandLineUtil;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.util.Map;

/**
 * rollback a topology which is under gray upgrade
 *
 * @author Cody
 * @since 2.3.1
 */
public class rollback_topology {

    public static void usage() {
        System.out.println("rollback a topology which is under gray upgrade, please do as following:");
        System.out.println("rollback_topology topologyName");
    }

    private static void rollbackTopology(String topologyName) {
        Map conf = Utils.readStormConfig();
        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        try {
            // update jar
            client.getClient().rollbackTopology(topologyName);
            CommandLineUtil.success("Successfully submit command rollback_topology " + topologyName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            CommandLineUtil.error("Invalid parameter");
            usage();
            return;
        }
        String topologyName = args[0];
        rollbackTopology(topologyName);
    }
}
