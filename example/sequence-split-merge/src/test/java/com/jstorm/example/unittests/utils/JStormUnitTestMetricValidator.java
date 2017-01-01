package com.jstorm.example.unittests.utils;

import backtype.storm.Config;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by binyang.dby on 2016/7/8.
 *
 * used for validate if a unit test result should pass by user define metrics. The metrics name should
 * be put in a set, and the value of it will be the key of the metric value.
 */
public abstract class JStormUnitTestMetricValidator implements JStormUnitTestValidator
{
    private Set<String> userDefineMetrics;

    /**
     * pass a set of metrics keys, these keys will be passed as a parameter in
     * callback validateMetrics().
     * @param userDefineMetrics
     */
    public JStormUnitTestMetricValidator(Set<String> userDefineMetrics) {
        this.userDefineMetrics = userDefineMetrics;
    }

    @Override
    public boolean validate(Map config) {
        return validateMetrics(getUserDefineMetrics(config));
    }

    /**
     * Validate is pass a unit test or not by the values of User define metrics.
     *
     * NOTE:this method provides only MetaType.COMPONENT metrics at window of 10 minutes.
     * This is because the value of 1 minute window will be auto cleared every minute.
     * Since the unit test will not last more than 10 minutes, it can fit the current needs.
     *
     * Use assert in the body of this method since the return value of it doesn't
     * determine the test result of the unit test, it is just passed to the return
     * value of JStormUnitTestRunner.submitTopology()
     *
     * @param metrics a map contains the user define metric values
     * @return the return value will be pass to the return value of JStormUnitTestRunner.submitTopology()
     */
    public abstract boolean validateMetrics(Map<String, Double> metrics);

    /**
     * get the metrics value which is defined by the user. the value will be 0 but not null
     * if a metric is defined but could not find in the result of JStormUtils.getMetrics()
     *
     * NOTE:this method provides only MetaType.COMPONENT metrics at window of 10 minutes.
     * This is because the value of 1 minute window will be auto cleared every minute.
     * Since the unit test will not last more than 10 minutes, it can fit the current needs.
     *
     * @param config the config pass in the JStormUnitTestRunner.submitTopology()
     * @return the metric values map
     */
    private Map<String, Double> getUserDefineMetrics(Map config)
    {
        String topologyName;
        if(config.containsKey(Config.TOPOLOGY_NAME))
            topologyName = (String)config.get(Config.TOPOLOGY_NAME);
        else
            topologyName = "JStormUnitTestTopology";

        Map<String, Double> rawMap = JStormUtils.getMetrics(config, topologyName, MetaType.COMPONENT, AsmWindow.M10_WINDOW);
        Map<String, Double> resultMap = new HashMap<String, Double>();

        //pre-set the user define metric to default 0 in order to avoid NullPointerException
        for(String key : userDefineMetrics)
            resultMap.put(key, 0.0);

        //foreach in the all metric map, just remain the metrics which the user has defined
        for(Map.Entry<String, Double> entry : rawMap.entrySet())
        {
            String key = entry.getKey();
            for(String userDefineKey : userDefineMetrics) {
                if (key.contains(userDefineKey))
                    resultMap.put(userDefineKey, entry.getValue());
            }
        }

        return resultMap;
    }
}
