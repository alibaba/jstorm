package com.jstorm.example.unittests.utils;

import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/8.
 *
 * This interface is used for check if a unit test should pass after running. The abstract classes which
 * implements this interface (like JStormUnitTestMetricValidator etc.) is recommended.
 */
public interface JStormUnitTestValidator
{
    /**
     * Validate is pass a unit test or not. Use assert in the body of this method since the return
     * value of it doesn't determine the test result of the unit test, it is just passed to the
     * return value of JStormUnitTestRunner.submitTopology()
     *
     * @param config the config which is passed in JStormUnitTestRunner.submitTopology()
     * @return the return value will be pass to the return value of JStormUnitTestRunner.submitTopology()
     */
    public boolean validate(Map config);
}
