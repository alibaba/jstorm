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
package com.alibaba.jstorm.ui.utils;

import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.WorkerSummary;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.ui.model.*;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIMetricUtils {

    private static final Logger LOG = LoggerFactory.getLogger(UIMetricUtils.class);

    public static final DecimalFormat format = new DecimalFormat(",###.##");


    public static List<String> sortHead(List<? extends UIBasicMetric> list, String[] HEAD) {
        List<String> sortedHead = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        for (UIBasicMetric metric : list) {
            keys.addAll(metric.getMetrics().keySet());
        }
        for (String h : HEAD) {
            if (keys.contains(h)) {
                sortedHead.add(h);
                keys.remove(h);
            }
        }
        sortedHead.addAll(keys);
        return sortedHead;
    }

    public static List<String> sortHead(UIBasicMetric metric, String[] HEAD) {
        List<String> sortedHead = new ArrayList<>();
        if (metric == null) return sortedHead;
        Set<String> keys = new HashSet<>();
        keys.addAll(metric.getMetrics().keySet());
        for (String h : HEAD) {
            if (keys.contains(h)) {
                sortedHead.add(h);
                keys.remove(h);
            }
        }
        sortedHead.addAll(keys);
        return sortedHead;
    }

    /**
     * get MetricSnapshot formatted value string
     */
    public static String getMetricValue(MetricSnapshot snapshot) {
        if (snapshot == null) return null;
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return format(snapshot.get_longValue());
            case GAUGE:
                return format(snapshot.get_doubleValue());
            case METER:
                return format(snapshot.get_m1());
            case HISTOGRAM:
                return format(snapshot.get_mean());
            default:
                return "0";
        }
    }

    public static String getMetricRawValue(MetricSnapshot snapshot) {
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return snapshot.get_longValue() + "";
            case GAUGE:
                return snapshot.get_doubleValue() + "";
            case METER:
                return snapshot.get_m1() + "";
            case HISTOGRAM:
                return snapshot.get_mean() + "";
            default:
                return "0";
        }
    }

    public static Number getMetricNumberValue(MetricSnapshot snapshot){
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return snapshot.get_longValue();
            case GAUGE:
                return snapshot.get_doubleValue();
            case METER:
                return snapshot.get_m1();
            case HISTOGRAM:
                return snapshot.get_mean();
            default:
                return 0;
        }
    }

    public static String format(double value) {
        return format.format(value);
    }

    public static String format(double value, String f){
        DecimalFormat _format = new DecimalFormat(f);
        return _format.format(value);
    }

    public static String format(long value) {
        return format.format(value);
    }


    // Extract compName from 'CC@SequenceTest4-1-1439469823@Merge@0@@sys@Emitted',which is 'Merge'
    public static String extractComponentName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[2];
    }

    // Extract TaskId from 'TH@SequenceTest2-2-1439865106@Split@17@@sys@SerializeTime',which is '17'
    public static String extractTaskId(String[] strs) {
        if (strs.length < 6) return null;
        return strs[3];
    }

    // Extract StreamId from 'SH@SequenceTest2-2-1439865106@SequenceSpout@35@default@sys@ProcessLatency',which is 'default'
    public static String extractStreamId(String[] strs) {
        if (strs.length < 6) return null;
        return strs[4];
    }

    // Extract Group from 'WM@SequenceTest2-2-1439865106@10.218.132.134@6800@sys@MemUsed',which is 'sys'
    public static String extractGroup(String[] strs) {
        if (strs.length < 6) return null;
        return strs[strs.length - 2];
    }

    // Extract MetricName from 'CC@SequenceTest4-1-1439469823@Merge@0@@sys@Emitted',which is 'Emitted'
    public static String extractMetricName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[strs.length - 1];
    }

    public static UISummaryMetric getSummaryMetrics(List<MetricInfo> infos, int window){
        if (infos == null || infos.size() == 0) {
            return null;
        }
        MetricInfo info = infos.get(infos.size() - 1);
        return getSummaryMetrics(info, window);
    }

    public static UISummaryMetric getSummaryMetrics(MetricInfo info, int window) {
        UISummaryMetric summaryMetric = new UISummaryMetric();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);

                if (!metric.getValue().containsKey(window)) {
                    LOG.debug("snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                summaryMetric.setMetricValue(snapshot, metricName);
            }
        }
        return summaryMetric;
    }

    /**
     * get all component metrics in the topology
     * @param info metric info
     * @param window window duration for metrics in seconds
     * @param componentSummaries list of component summaries
     * @param userDefinedMetrics list of user defined metrics for return
     * @return list of component metrics
     */
    public static List<UIComponentMetric> getComponentMetrics(MetricInfo info, int window,
                                                     List<ComponentSummary> componentSummaries,
                                                     List<UIUserDefinedMetric> userDefinedMetrics) {
        Map<String, UIComponentMetric> componentData = new HashMap<>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String compName = UIMetricUtils.extractComponentName(split_name);
                String metricName = UIMetricUtils.extractMetricName(split_name);
                String group = UIMetricUtils.extractGroup(split_name);
                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                if (!metric.getValue().containsKey(window)) {
                    LOG.debug("component snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                if (group != null && group.equals("udf")){
                    UIUserDefinedMetric udm = new UIUserDefinedMetric(metricName, compName);
                    udm.setValue(UIMetricUtils.getMetricValue(snapshot));
                    udm.setType(snapshot.get_metricType());
                    userDefinedMetrics.add(udm);
                }else {
                    UIComponentMetric compMetric;
                    if (componentData.containsKey(compName)) {
                        compMetric = componentData.get(compName);
                    } else {
                        compMetric = new UIComponentMetric(compName);
                        componentData.put(compName, compMetric);
                    }
                    compMetric.setMetricValue(snapshot, parentComp, metricName);
                }
            }
        }
        //merge sub metrics
        for (UIComponentMetric comp : componentData.values()) {
            comp.mergeValue();
        }
        //combine the summary info into metrics
        TreeMap<String, UIComponentMetric> ret = new TreeMap<>();
        for (ComponentSummary summary : componentSummaries) {
            String compName = summary.get_name();
            UIComponentMetric compMetric;
            if (componentData.containsKey(compName)) {
                compMetric = componentData.get(compName);
                compMetric.setParallel(summary.get_parallel());
                compMetric.setType(summary.get_type());
                compMetric.setErrors(summary.get_errors());
            } else {
                compMetric = new UIComponentMetric(compName, summary.get_parallel(), summary.get_type());
                compMetric.setErrors(summary.get_errors());
                componentData.put(compName, compMetric);
            }
            String key = compMetric.getType() + compName;
            if (compName.startsWith("__")) {
                key = "a" + key;
            }
            compMetric.setSortedKey(key);
            ret.put(key, compMetric);
        }
        return new ArrayList<>(ret.descendingMap().values());
    }


    /**
     * get the specific component metric
     * @param info metric info
     * @param window window duration for metrics in seconds
     * @param compName component name
     * @param componentSummaries list of component summaries
     * @return the component metric
     */
    public static UIComponentMetric getComponentMetric(MetricInfo info, int window, String compName,
                                                 List<ComponentSummary> componentSummaries) {
        UIComponentMetric compMetric = new UIComponentMetric(compName);
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String componentName = UIMetricUtils.extractComponentName(split_name);
                if (componentName != null && !componentName.equals(compName)) continue;

                //only handle the specific component
                String metricName = UIMetricUtils.extractMetricName(split_name);
                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                MetricSnapshot snapshot = metric.getValue().get(window);

                compMetric.setMetricValue(snapshot, parentComp, metricName);
            }
        }
        compMetric.mergeValue();
        ComponentSummary summary = null;
        for (ComponentSummary cs : componentSummaries) {
            if (cs.get_name().equals(compName)) {
                summary = cs;
                break;
            }
        }
        if (summary != null) {
            compMetric.setParallel(summary.get_parallel());
            compMetric.setType(summary.get_type());
        }
        return compMetric;
    }

    public static List<UIWorkerMetric> getWorkerMetrics(MetricInfo info, String topology, int window) {
        Map<String, UIWorkerMetric> workerData = new HashMap<>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String host = UIMetricUtils.extractComponentName(split_name);
                String port = UIMetricUtils.extractTaskId(split_name);
                String key = host + ":" + port;
                String metricName = UIMetricUtils.extractMetricName(split_name);


                if (!metric.getValue().containsKey(window)) {
                    LOG.info("worker snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                UIWorkerMetric workerMetric;
                if (workerData.containsKey(key)) {
                    workerMetric = workerData.get(key);
                } else {
                    workerMetric = new UIWorkerMetric(host, port, topology);
                    workerData.put(key, workerMetric);
                }
                workerMetric.setMetricValue(snapshot, metricName);
            }
        }
        return new ArrayList<>(workerData.values());
    }

    public static List<UIWorkerMetric> getWorkerMetrics(Map<String, MetricInfo> workerMetricInfo,
                                                  List<WorkerSummary> workerSummaries, String host, int window) {
        Map<String, UIWorkerMetric> workerMetrics = new HashMap<>();
        for (MetricInfo info : workerMetricInfo.values()) {
            if (info != null) {
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                    String name = metric.getKey();
                    String[] split_name = name.split("@");
                    String _host = UIMetricUtils.extractComponentName(split_name);
                    if (!host.equals(_host)) continue;

                    //only handle the specific host
                    String port = UIMetricUtils.extractTaskId(split_name);
                    String key = host + ":" + port;
                    String metricName = UIMetricUtils.extractMetricName(split_name);
                    MetricSnapshot snapshot = metric.getValue().get(window);

                    UIWorkerMetric workerMetric;
                    if (workerMetrics.containsKey(key)) {
                        workerMetric = workerMetrics.get(key);
                    } else {
                        workerMetric = new UIWorkerMetric(host, port);
                        workerMetrics.put(key, workerMetric);
                    }
                    workerMetric.setMetricValue(snapshot, metricName);
                }
            }
        }

        for (WorkerSummary ws : workerSummaries){
            String worker = host + ":" + ws.get_port();
            if (workerMetrics.containsKey(worker)) {
                workerMetrics.get(worker).setTopology(ws.get_topology());
            }
        }

        return new ArrayList<>(workerMetrics.values());
    }

    /**
     * get all task metrics in the specific component
     * @param info raw metric info
     * @param component component id
     * @param window window duration for metrics in seconds
     * @return the list of task metrics
     */
    public static List<UITaskMetric> getTaskMetrics(MetricInfo info, String component, int window) {
        TreeMap<Integer, UITaskMetric> taskData = new TreeMap<>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                int taskId = JStormUtils.parseInt(UIMetricUtils.extractTaskId(split_name));
                String componentName = UIMetricUtils.extractComponentName(split_name);
                if (componentName != null && !componentName.equals(component)) continue;

                //only handle the tasks belongs to the specific component
                String metricName = UIMetricUtils.extractMetricName(split_name);

                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                if (!metric.getValue().containsKey(window)) {
                    LOG.info("task snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                UITaskMetric taskMetric;
                if (taskData.containsKey(taskId)) {
                    taskMetric = taskData.get(taskId);
                } else {
                    taskMetric = new UITaskMetric(component, taskId);
                    taskData.put(taskId, taskMetric);
                }
                taskMetric.setMetricValue(snapshot, parentComp, metricName);
            }
        }
        for (UITaskMetric t : taskData.values()) {
            t.mergeValue();
        }
        return new ArrayList<>(taskData.values());
    }


    /**
     * get the specific task metric
     * @param taskStreamMetrics raw metric info
     * @param component component name
     * @param id task id
     * @param window window duration for metrics in seconds
     * @return the task metric
     */
    public static UITaskMetric getTaskMetric(List<MetricInfo> taskStreamMetrics, String component,
                                             int id, int window) {
        UITaskMetric taskMetric = new UITaskMetric(component, id);
        if (taskStreamMetrics.size() > 1) {
            MetricInfo info = taskStreamMetrics.get(0);
            if (info != null) {
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                    String name = metric.getKey();
                    String[] split_name = name.split("@");
                    int taskId = JStormUtils.parseInt(UIMetricUtils.extractTaskId(split_name));
                    if (taskId != id) continue;

                    //only handle the specific task
                    String metricName = UIMetricUtils.extractMetricName(split_name);

                    String parentComp = null;
                    if (metricName != null && metricName.contains(".")) {
                        parentComp = metricName.split("\\.")[0];
                        metricName = metricName.split("\\.")[1];
                    }

                    MetricSnapshot snapshot = metric.getValue().get(window);
                    taskMetric.setMetricValue(snapshot, parentComp, metricName);
                }
            }
        }
        taskMetric.mergeValue();
        return taskMetric;
    }


    public static List<UIStreamMetric> getStreamMetrics(List<MetricInfo> taskStreamMetrics, String component,
                                               int id, int window) {
        Map<String, UIStreamMetric> streamData = new HashMap<>();
        if (taskStreamMetrics.size() > 1){
            MetricInfo info = taskStreamMetrics.get(1);
            if (info != null) {
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                    String name = metric.getKey();
                    String[] split_name = name.split("@");
                    int taskId = JStormUtils.parseInt(UIMetricUtils.extractTaskId(split_name));
                    if (taskId != id) continue;

                    //only handle the specific task
                    String metricName = UIMetricUtils.extractMetricName(split_name);
                    String streamId = UIMetricUtils.extractStreamId(split_name);

                    String parentComp = null;
                    if (metricName != null && metricName.contains(".")) {
                        parentComp = metricName.split("\\.")[0];
                        metricName = metricName.split("\\.")[1];
                    }

                    MetricSnapshot snapshot = metric.getValue().get(window);

                    UIStreamMetric streamMetric;
                    if (streamData.containsKey(streamId)) {
                        streamMetric = streamData.get(streamId);
                    } else {
                        streamMetric = new UIStreamMetric(component, streamId);
                        streamData.put(streamId, streamMetric);
                    }
                    streamMetric.setMetricValue(snapshot, parentComp, metricName);
                }
            }
        }
        for (UIStreamMetric stream : streamData.values()) {
            stream.mergeValue();
        }
        return new ArrayList<>(streamData.values());
    }



}
