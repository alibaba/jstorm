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
 * gray upgrade a topology
 *
 * @author Cody
 * @since 2.3.1
 */
public class complete_upgrade {

    public static void usage() {
        System.out.println("force to complete upgrading a topology, please do as following:");
        System.out.println("complete_upgrade topologyName");
    }

    private static void completeTopology(String topologyName)
            throws Exception {
        Map conf = Utils.readStormConfig();
        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        try {
            client.getClient().completeUpgrade(topologyName);
            CommandLineUtil.success("Successfully submit command complete_upgrade " + topologyName);
        } catch (Exception ex) {
            CommandLineUtil.error("Failed to perform complete_upgrade: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.out.println("Invalid parameter");
            usage();
            return;
        }
        String topologyName = args[0];
        completeTopology(topologyName);
    }
}
