package org.apache.storm.starter;

import org.junit.Test;

import com.alipay.dw.jstorm.example.window.RollingTopWords;
import com.alipay.dw.jstorm.example.window.SkewedRollingTopWords;
import com.alipay.dw.jstorm.example.window.SlidingTupleTsTopology;
import com.alipay.dw.jstorm.example.window.SlidingWindowTopology;

public class TestRollingWindow {
    /**
     * replaced by unit test
     */
    //@Test
    public void testRollingTopWords() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        RollingTopWords.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    /**
     * replaced
     */
    //@Test
    public void testSkewedRollingTopWords() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SkewedRollingTopWords.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    /**
     * replaced
     */
    //@Test
    public void testSlidingTupleTs() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SlidingTupleTsTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    /**
     * replaced by unit test
     */
    //@Test
    public void testSlidingWindowTopology() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SlidingWindowTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }
    
}
