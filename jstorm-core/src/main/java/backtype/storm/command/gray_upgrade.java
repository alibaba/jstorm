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

import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.CommandLineUtil;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

/**
 * gray upgrade a topology
 *
 * @author Cody
 * @since 2.3.1
 */
public class gray_upgrade {

    public static void usage() {
        System.out.println("gray upgrade a topology, please do as following:");
        System.out.println("gray_upgrade topologyName -n workerNum -p component -w workers");
    }

    private static Options buildGeneralOptions(Options opts) {
        Options r = new Options();

        for (Object o : opts.getOptions())
            r.addOption((Option) o);

        Option workerNum = OptionBuilder.withArgName("worker num per batch").hasArg()
                .withDescription("number of workers to upgrade").create("n");
        r.addOption(workerNum);

        Option comp = OptionBuilder.withArgName("component to upgrade").hasArg()
                .withDescription("component id to upgrade, note that only one component is allowed at a time")
                .create("p");
        r.addOption(comp);

        Option workers = OptionBuilder.withArgName("workers to upgrade").hasArg()
                .withDescription("workers to upgrade, in the format: host1:port1,host2:port2,...").create("w");
        r.addOption(workers);

        return r;
    }

    private static void upgradeTopology(String topologyName, String component, List<String> workers, int workerNum)
            throws Exception {
        Map conf = Utils.readStormConfig();
        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        try {
            String topologyId = client.getClient().getTopologyId(topologyName);
            Map stormConf = (Map) Utils.from_json(client.getClient().getTopologyConf(topologyId));
            // check if TM is a separate worker
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topologyId);
            for (TaskSummary taskSummary : topologyInfo.get_tasks()) {
                if (!taskSummary.get_status().equalsIgnoreCase("active")) {
                    CommandLineUtil.error("Some of the tasks are not in ACTIVE state, cannot perform the upgrade!");
                    return;
                }
            }

            if (!ConfigExtension.isTmSingleWorker(stormConf, topologyInfo.get_topology().get_numWorkers())) {
                CommandLineUtil.error("Gray upgrade requires that topology master to be a single worker, " +
                        "cannot perform the upgrade!");
                return;
            }

            client.getClient().grayUpgrade(topologyName, component, workers, workerNum);
            CommandLineUtil.success("Successfully submit command gray_upgrade " + topologyName);
        } catch (Exception ex) {
            CommandLineUtil.error("Failed to perform gray_upgrade: " + ex.getMessage());
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
        String[] str2 = Arrays.copyOfRange(args, 1, args.length);
        CommandLineParser parser = new GnuParser();
        Options r = buildGeneralOptions(new Options());
        CommandLine commandLine = parser.parse(r, str2, true);

        int workerNum = 0;
        String component = null;
        List<String> workers = null;
        if (commandLine.hasOption("n")) {
            workerNum = Integer.valueOf(commandLine.getOptionValue("n"));
        }
        if (commandLine.hasOption("p")) {
            component = commandLine.getOptionValue("p");
        }
        if (commandLine.hasOption("w")) {
            String w = commandLine.getOptionValue("w");
            if (!StringUtils.isBlank(w)) {
                workers = Lists.newArrayList();
                String[] parts = w.split(",");
                for (String part : parts) {
                    if (part.split(":").length == 2) {
                        workers.add(part.trim());
                    }
                }
            }
        }
        upgradeTopology(topologyName, component, workers, workerNum);
    }
}
