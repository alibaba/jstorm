/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.yarn.utils;


import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * a scanner which can take an input string for a range or scan the lot.
 */
public class PortScanner {
    private static Pattern NUMBER_RANGE = Pattern.compile("^(\\d+)\\s*-\\s*(\\d+)$");
    private static Pattern SINGLE_NUMBER = Pattern.compile("^\\d+$");

    private List<Integer> remainingPortsToCheck;

    public PortScanner() {
    }

    int nextPort = 1024;

    public void setPortRange(String input) {
        // first split based on commas
        Set<Integer> inputPorts = new TreeSet<Integer>();
        String[] ranges = input.split(",");
        for (String range : ranges) {
            Matcher m = SINGLE_NUMBER.matcher(range.trim());
            if (m.find()) {
                inputPorts.add(Integer.parseInt(m.group()));
            } else {
                m = NUMBER_RANGE.matcher(range.trim());
                if (m.find()) {
                    String[] boundaryValues = m.group(0).split("-");
                    int start = Integer.parseInt(boundaryValues[0].trim());
                    int end = Integer.parseInt(boundaryValues[1].trim());
                    for (int i = start; i < end + 1; i++) {
                        inputPorts.add(i);
                    }
                }
            }
        }
        this.remainingPortsToCheck = new ArrayList<Integer>(inputPorts);
    }

    public List<Integer> getRemainingPortsToCheck() {
        return remainingPortsToCheck;
    }

    public int getAvailablePort() throws Exception {
        if (remainingPortsToCheck != null) {
            return getAvailablePortViaPortArray();
        } else {
            return getAvailablePortViaCounter();
        }
    }

    private int getAvailablePortViaCounter() throws Exception {
        int port;
        do {
            port = nextPort;
            nextPort++;
        } while (!JstormYarnUtils.isPortAvailable(port));
        return port;
    }

    private int getAvailablePortViaPortArray() throws Exception {
        boolean found = false;
        int availablePort = -1;
        Iterator<Integer> portsToCheck = this.remainingPortsToCheck.iterator();
        while (portsToCheck.hasNext() && !found) {
            int portToCheck = portsToCheck.next();
            found = JstormYarnUtils.isPortAvailable(portToCheck);
            if (found) {
                availablePort = portToCheck;
                portsToCheck.remove();
            }
        }

        if (availablePort < 0) {
            throw new Exception("No available ports found in configured range " + "No available ports found in configured range " +
                    remainingPortsToCheck
            );
        }

        return availablePort;
    }
}
