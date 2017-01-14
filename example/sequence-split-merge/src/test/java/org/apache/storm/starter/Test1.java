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

public class Test1 {

    /**
     * TestBackpressure was changed, is it OK to be tested now?
     *
     */
    //@Test
    public void testUpdateConf() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TestBackpressure.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * what is the usage of the method commit and revert in both spout and bolt?
     * i could not invoke any of them in any way.
     */
    //@Test
    public void testJStormBatch() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SimpleBatchTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }
    
    
////    @Test
////    public void testTaskInDifferentNode() throws Exception {
////        TaskInDifferentNodeTopology.test();
////    }
////    
////    @Test
////    public void testUserDefinedHosts() {
////        UserDefinedHostsTopology.test();
////    }
////    
////    @Test
////    public void testUserDefinedWorkerTopology() {
////        UserDefinedWorkerTopology.test();
////    }


    /**
     * has been replaced by a unit test
     */
    //@Test
    public void testInOrderDelivery() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        InOrderDeliveryTest.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }
    
    
}
