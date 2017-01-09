package org.apache.storm.starter;

import org.apache.storm.starter.trident.TridentMapExample;
import org.apache.storm.starter.trident.TridentMinMaxOfDevicesTopology;
import org.apache.storm.starter.trident.TridentMinMaxOfVehiclesTopology;
import org.apache.storm.starter.trident.TridentReach;
import org.apache.storm.starter.trident.TridentWindowingInmemoryStoreTopology;
import org.apache.storm.starter.trident.TridentWordCount;
import org.junit.Test;

import com.alipay.dw.jstorm.example.batch.SimpleBatchTopology;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology;
import com.alipay.dw.jstorm.example.update.topology.TestBackpressure;
import com.alipay.dw.jstorm.example.window.RollingTopWords;
import com.alipay.dw.jstorm.example.window.SkewedRollingTopWords;
import com.alipay.dw.jstorm.example.window.SlidingTupleTsTopology;
import com.alipay.dw.jstorm.example.window.SlidingWindowTopology;
import com.alipay.dw.jstorm.transcation.TransactionalGlobalCount;
import com.alipay.dw.jstorm.transcation.TransactionalWords;

/**
 * keep this unit test case
 * maybe we should add some assert.
 */
public class TestDrpc {

    /**
     * replaced
     */
    //@Test
    public void testBasicDRPCTopology() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        BasicDRPCTopology.testDrpc();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced
     */
    //@Test
    public void testManualDRPC() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        ManualDRPC.testDrpc();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced by unit test in package performance
     */
    //@Test
    public void testFastWordCountTopology() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        FastWordCountTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }
    
}
