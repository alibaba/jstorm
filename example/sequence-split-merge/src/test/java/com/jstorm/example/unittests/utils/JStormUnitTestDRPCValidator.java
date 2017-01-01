package com.jstorm.example.unittests.utils;

import backtype.storm.LocalDRPC;

/**
 * Created by binyang.dby on 2016/7/21.
 *
 * used for validate if a unit test result should pass by execute local DRPC.
 */
public abstract class JStormUnitTestDRPCValidator implements JStormUnitTestValidator
{
    private LocalDRPC localDRPC;

    /**
     * pass a localDRPC object to execute when you call executeLocalDRPC() method.
     * @param localDRPC the localDRPC
     */
    public JStormUnitTestDRPCValidator(LocalDRPC localDRPC)
    {
        this.localDRPC = localDRPC;
    }

    /**
     * execute local DRPC and generate a result
     * @param function the name of function
     * @param args the args
     * @return the execute result
     */
    protected String executeLocalDRPC(String function, String args) {
        return localDRPC.execute(function, args);
    }
}
