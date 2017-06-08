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
package com.alibaba.jstorm.container.cgroup.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.SubSystemType;
import com.alibaba.jstorm.container.cgroup.Device;

public class BlkioCore implements CgroupCore {

    public static final String BLKIO_WEIGHT = "/blkio.weight";
    public static final String BLKIO_WEIGHT_DEVICE = "/blkio.weight_device";
    public static final String BLKIO_RESET_STATS = "/blkio.reset_stats";

    public static final String BLKIO_THROTTLE_READ_BPS_DEVICE = "/blkio.throttle.read_bps_device";
    public static final String BLKIO_THROTTLE_WRITE_BPS_DEVICE = "/blkio.throttle.write_bps_device";
    public static final String BLKIO_THROTTLE_READ_IOPS_DEVICE = "/blkio.throttle.read_iops_device";
    public static final String BLKIO_THROTTLE_WRITE_IOPS_DEVICE = "/blkio.throttle.write_iops_device";

    public static final String BLKIO_THROTTLE_IO_SERVICED = "/blkio.throttle.io_serviced";
    public static final String BLKIO_THROTTLE_IO_SERVICE_BYTES = "/blkio.throttle.io_service_bytes";

    public static final String BLKIO_TIME = "/blkio.time";
    public static final String BLKIO_SECTORS = "/blkio.sectors";
    public static final String BLKIO_IO_SERVICED = "/blkio.io_serviced";
    public static final String BLKIO_IO_SERVICE_BYTES = "/blkio.io_service_bytes";
    public static final String BLKIO_IO_SERVICE_TIME = "/blkio.io_service_time";
    public static final String BLKIO_IO_WAIT_TIME = "/blkio.io_wait_time";
    public static final String BLKIO_IO_MERGED = "/blkio.io_merged";
    public static final String BLKIO_IO_QUEUED = "/blkio.io_queued";

    private final String dir;

    public BlkioCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.blkio;
    }

    /* weight: 100-1000 */
    public void setBlkioWeight(int weight) throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_WEIGHT), String.valueOf(weight));
    }

    public int getBlkioWeight() throws IOException {
        return Integer.valueOf(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_WEIGHT)).get(0));
    }

    public void setBlkioWeightDevice(Device device, int weight) throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_WEIGHT_DEVICE), makeContext(device, weight));
    }

    public Map<Device, Integer> getBlkioWeightDevice() throws IOException {
        List<String> strings = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_WEIGHT_DEVICE));
        Map<Device, Integer> result = new HashMap<>();
        for (String string : strings) {
            String[] strArgs = string.split(" ");
            Device device = new Device(strArgs[0]);
            Integer weight = Integer.valueOf(strArgs[1]);
            result.put(device, weight);
        }
        return result;
    }

    public void setReadBps(Device device, long bps) throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_READ_BPS_DEVICE), makeContext(device, bps));
    }

    public Map<Device, Long> getReadBps() throws IOException {
        List<String> strings = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_READ_BPS_DEVICE));
        Map<Device, Long> result = new HashMap<>();
        for (String string : strings) {
            String[] strArgs = string.split(" ");
            Device device = new Device(strArgs[0]);
            Long bps = Long.valueOf(strArgs[1]);
            result.put(device, bps);
        }
        return result;
    }

    public void setWriteBps(Device device, long bps) throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_WRITE_BPS_DEVICE), makeContext(device, bps));
    }

    public Map<Device, Long> getWriteBps() throws IOException {
        List<String> strings = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_WRITE_BPS_DEVICE));
        Map<Device, Long> result = new HashMap<>();
        for (String string : strings) {
            String[] strArgs = string.split(" ");
            Device device = new Device(strArgs[0]);
            Long bps = Long.valueOf(strArgs[1]);
            result.put(device, bps);
        }
        return result;
    }

    public void setReadIOps(Device device, long iops) throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_READ_IOPS_DEVICE), makeContext(device, iops));
    }

    public Map<Device, Long> getReadIOps() throws IOException {
        List<String> strings = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_READ_IOPS_DEVICE));
        Map<Device, Long> result = new HashMap<>();
        for (String string : strings) {
            String[] strArgs = string.split(" ");
            Device device = new Device(strArgs[0]);
            Long iops = Long.valueOf(strArgs[1]);
            result.put(device, iops);
        }
        return result;
    }

    public void setWriteIOps(Device device, long iops) throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_WRITE_IOPS_DEVICE), makeContext(device, iops));
    }

    public Map<Device, Long> getWriteIOps() throws IOException {
        List<String> strings = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_WRITE_IOPS_DEVICE));
        Map<Device, Long> result = new HashMap<>();
        for (String string : strings) {
            String[] strArgs = string.split(" ");
            Device device = new Device(strArgs[0]);
            Long iops = Long.valueOf(strArgs[1]);
            result.put(device, iops);
        }
        return result;
    }

    public Map<Device, Map<RecordType, Long>> getThrottleIOServiced() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_IO_SERVICED)));
    }

    public Map<Device, Map<RecordType, Long>> getThrottleIOServiceByte() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_THROTTLE_IO_SERVICE_BYTES)));
    }

    public Map<Device, Long> getBlkioTime() throws IOException {
        Map<Device, Long> result = new HashMap<>();
        List<String> strs = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_TIME));
        for (String str : strs) {
            String[] strArgs = str.split(" ");
            result.put(new Device(strArgs[0]), Long.parseLong(strArgs[1]));
        }
        return result;
    }

    public Map<Device, Long> getBlkioSectors() throws IOException {
        Map<Device, Long> result = new HashMap<>();
        List<String> strs = CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_SECTORS));
        for (String str : strs) {
            String[] strArgs = str.split(" ");
            result.put(new Device(strArgs[0]), Long.parseLong(strArgs[1]));
        }
        return result;
    }

    public Map<Device, Map<RecordType, Long>> getIOServiced() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_IO_SERVICED)));
    }

    public Map<Device, Map<RecordType, Long>> getIOServiceBytes() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_IO_SERVICE_BYTES)));
    }

    public Map<Device, Map<RecordType, Long>> getIOServiceTime() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_IO_SERVICE_TIME)));
    }

    public Map<Device, Map<RecordType, Long>> getIOWaitTime() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_IO_WAIT_TIME)));
    }

    public Map<Device, Map<RecordType, Long>> getIOMerged() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_IO_MERGED)));
    }

    public Map<Device, Map<RecordType, Long>> getIOQueued() throws IOException {
        return this.analyseRecord(CgroupUtils.readFileByLine(Constants.getDir(this.dir, BLKIO_IO_QUEUED)));
    }

    public void resetStats() throws IOException {
        CgroupUtils.writeFileByLine(Constants.getDir(this.dir, BLKIO_RESET_STATS), "1");
    }

    private String makeContext(Device device, Object data) {
        StringBuilder sb = new StringBuilder();
        sb.append(device.toString()).append(" ").append(data);
        return sb.toString();
    }

    private Map<Device, Map<RecordType, Long>> analyseRecord(List<String> strs) {
        Map<Device, Map<RecordType, Long>> result = new HashMap<>();
        for (String str : strs) {
            String[] strArgs = str.split(" ");
            if (strArgs.length != 3)
                continue;
            Device device = new Device(strArgs[0]);
            RecordType key = RecordType.getType(strArgs[1]);
            Long value = Long.parseLong(strArgs[2]);
            Map<RecordType, Long> record = result.get(device);
            if (record == null) {
                record = new HashMap<>();
                result.put(device, record);
            }
            record.put(key, value);
        }
        return result;
    }

    public enum RecordType {
        read, write, sync, async, total;

        public static RecordType getType(String type) {
            if (type.equals("Read"))
                return read;
            else if (type.equals("Write"))
                return write;
            else if (type.equals("Sync"))
                return sync;
            else if (type.equals("Async"))
                return async;
            else if (type.equals("Total"))
                return total;
            else
                return null;
        }
    }

}
