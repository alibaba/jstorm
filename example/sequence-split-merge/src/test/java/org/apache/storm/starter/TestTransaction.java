package org.apache.storm.starter;

import org.junit.Test;

import com.alipay.dw.jstorm.transcation.TransactionalGlobalCount;
import com.alipay.dw.jstorm.transcation.TransactionalWords;

public class TestTransaction {
    /**
     * has been replaced by a unit test. Old API , use trident instead.
     */
    //@Test
    public void testTransactionGlobalCount() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TransactionalGlobalCount.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * unit test still has some problem(exist in the origin example too)
     */
    //@Test
    public void testTransactionWords() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TransactionalWords.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }
}
