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
package com.alibaba.jstorm.container;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.container.cgroup.CgroupCommon;
import com.alibaba.jstorm.utils.SystemOperation;

public class CgroupCenter implements CgroupOperation {
    private static Logger LOG = LoggerFactory.getLogger(CgroupCenter.class);

    private static CgroupCenter instance;

    private CgroupCenter() {
    }

    /**
     * Thread unsafe
     */
    public synchronized static CgroupCenter getInstance() {
        if (instance == null)
            instance = new CgroupCenter();
        return CgroupUtils.enabled() ? instance : null;
    }

    @Override
    public List<Hierarchy> getHierarchies() {
        Map<String, Hierarchy> hierarchies = new HashMap<>();
        FileReader reader = null;
        BufferedReader br = null;
        try {
            reader = new FileReader(Constants.MOUNT_STATUS_FILE);
            br = new BufferedReader(reader);
            String str;
            while ((str = br.readLine()) != null) {
                String[] strSplit = str.split(" ");
                if (!strSplit[2].equals("cgroup"))
                    continue;
                String name = strSplit[0];
                String type = strSplit[3];
                String dir = strSplit[1];
                Hierarchy h = new Hierarchy(name, CgroupUtils.analyse(type), dir);
                hierarchies.put(type, h);
            }
            return new ArrayList<>(hierarchies.values());
        } catch (Exception e) {
            LOG.error("Get hierarchies error", e);
        } finally {
            CgroupUtils.close(reader, br);
        }
        return null;
    }

    @Override
    public Set<SubSystem> getSubSystems() {
        Set<SubSystem> subSystems = new HashSet<>();
        FileReader reader = null;
        BufferedReader br = null;
        try {
            reader = new FileReader(Constants.CGROUP_STATUS_FILE);
            br = new BufferedReader(reader);
            String str;
            while ((str = br.readLine()) != null) {
                String[] split = str.split("\t");
                SubSystemType type = SubSystemType.getSubSystem(split[0]);
                if (type == null)
                    continue;
                subSystems.add(new SubSystem(type, Integer.valueOf(split[1]), Integer.valueOf(split[2]),
                        Integer.valueOf(split[3]) == 1));
            }
            return subSystems;
        } catch (Exception e) {
            LOG.error("Get subSystems error ", e);
        } finally {
            CgroupUtils.close(reader, br);
        }
        return null;
    }

    @Override
    public boolean enabled(SubSystemType subsystem) {
        Set<SubSystem> subSystems = this.getSubSystems();
        for (SubSystem subSystem : subSystems) {
            if (subSystem.getType() == subsystem)
                return true;
        }
        return false;
    }

    @Override
    public Hierarchy busy(SubSystemType subsystem) {
        List<Hierarchy> hierarchies = this.getHierarchies();
        for (Hierarchy hierarchy : hierarchies) {
            for (SubSystemType type : hierarchy.getSubSystems()) {
                if (type == subsystem)
                    return hierarchy;
            }
        }
        return null;
    }

    @Override
    public Hierarchy mounted(Hierarchy hierarchy) {
        List<Hierarchy> hierarchies = this.getHierarchies();
        if (CgroupUtils.dirExists(hierarchy.getDir())) {
            for (Hierarchy h : hierarchies) {
                if (h.equals(hierarchy))
                    return h;
            }
        }
        return null;
    }

    @Override
    public void mount(Hierarchy hierarchy) throws IOException {
        if (this.mounted(hierarchy) != null) {
            LOG.error(hierarchy.getDir() + " is mounted");
            return;
        }
        Set<SubSystemType> subsystems = hierarchy.getSubSystems();
        for (SubSystemType type : subsystems) {
            if (this.busy(type) != null) {
                LOG.error("subsystem: " + type.name() + " is busy");
                subsystems.remove(type);
            }
        }
        if (subsystems.size() == 0)
            return;
        if (!CgroupUtils.dirExists(hierarchy.getDir()))
            new File(hierarchy.getDir()).mkdirs();
        String subSystems = CgroupUtils.reAnalyse(subsystems);
        SystemOperation.mount(subSystems, hierarchy.getDir(), "cgroup", subSystems);

    }

    @Override
    public void umount(Hierarchy hierarchy) throws IOException {
        if (this.mounted(hierarchy) != null) {
            hierarchy.getRootCgroups().delete();
            SystemOperation.umount(hierarchy.getDir());
            CgroupUtils.deleteDir(hierarchy.getDir());
        }
    }

    @Override
    public void create(CgroupCommon cgroup) throws SecurityException {
        if (cgroup.isRoot()) {
            LOG.error("You can't create rootCgroup in this function");
            return;
        }
        CgroupCommon parent = cgroup.getParent();
        while (parent != null) {
            if (!CgroupUtils.dirExists(parent.getDir())) {
                LOG.error(parent.getDir() + " does not exist");
                return;
            }
            parent = parent.getParent();
        }
        Hierarchy h = cgroup.getHierarchy();
        if (mounted(h) == null) {
            LOG.error(h.getDir() + " is not mounted");
            return;
        }
        if (CgroupUtils.dirExists(cgroup.getDir())) {
            LOG.error(cgroup.getDir() + " exists");
            return;
        }
        (new File(cgroup.getDir())).mkdir();
    }

    @Override
    public void delete(CgroupCommon cgroup) throws IOException {
        cgroup.delete();
    }

    public static void main(String args[]) {
        System.out.println(CgroupCenter.getInstance().getHierarchies().get(0).getRootCgroups().getChildren().size());
    }

}
