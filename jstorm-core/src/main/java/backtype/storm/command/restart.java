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

import java.security.InvalidParameterException;
import java.util.Map;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * restart a topology
 *
 * @author basti
 */
public class restart {

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            throw new InvalidParameterException("Please input topology name!");
        }

        String topologyName = args[0];

        NimbusClient client = null;
        try {
            Map conf = Utils.readStormConfig();
            client = NimbusClient.getConfiguredClient(conf);

            System.out.println("It will take 15 ~ 100 seconds to restart, please wait patiently\n");

            if (args.length == 1) {
                client.getClient().restart(topologyName, null);
            } else {
                Map loadConf = Utils.loadConf(args[1]);
                String jsonConf = Utils.to_json(loadConf);
                System.out.println("New configuration:\n" + jsonConf);

                client.getClient().restart(topologyName, jsonConf);
            }

            System.out.println("Successfully submit command restart " + topologyName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

}
