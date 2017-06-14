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
package com.alibaba.jstorm.utils;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/***
 * Catching signal in java is very dangerous,
 * Please do as less as possible
 */
public class JStormSignalHandler implements SignalHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JStormSignalHandler.class);

    // It must be single instance
    protected static JStormSignalHandler instance = null;

    protected Map<Integer, String> signalMap = new HashMap<>();
    protected Map<Integer, Runnable> signalHandlers = new HashMap<>();
    protected Map<Integer, SignalHandler> oldSignalHandlers = new HashMap<>();
    protected Thread signalThread;
    protected LinkedBlockingDeque<Signal> waitingSignals = new LinkedBlockingDeque<>();

    protected boolean isRunning = true;

    public static JStormSignalHandler getInstance() {
        synchronized (JStormSignalHandler.class) {
            if (instance == null) {
                instance = new JStormSignalHandler();
            }
        }
        return instance;
    }

    protected JStormSignalHandler() {
        initSignalMap();
        initSignalThread();
    }

    protected void initSignalMap() {
        signalMap.put(1, "HUP");
        signalMap.put(2, "INT");
        signalMap.put(3, "QUIT");
        signalMap.put(4, "ILL");
        signalMap.put(5, "TRAP");
        signalMap.put(6, "ABRT");
        signalMap.put(6, "IOT");
        signalMap.put(7, "BUS");
        signalMap.put(8, "FPE");
        signalMap.put(9, "KILL");
        signalMap.put(10, "USR1");
        signalMap.put(11, "SEGV");
        signalMap.put(12, "USR2");
        signalMap.put(13, "PIPE");
        signalMap.put(14, "ALRM");
        signalMap.put(15, "TERM");
        signalMap.put(16, "STKFLT");
        signalMap.put(17, "CHLD");
        signalMap.put(18, "CONT");
        signalMap.put(19, "STOP");
        signalMap.put(20, "TSTP");
        signalMap.put(21, "TTIN");
        signalMap.put(22, "TTOU");
        signalMap.put(23, "URG");
        signalMap.put(24, "XCPU");
        signalMap.put(25, "XFSZ");
        signalMap.put(26, "VTALRM");
        signalMap.put(27, "PROF");
        signalMap.put(28, "WINCH");
        signalMap.put(29, "IO");
        signalMap.put(30, "PWR");
        signalMap.put(31, "SYS");
        signalMap.put(32, "UNUSED");
    }


    protected void initSignalThread() {
        signalThread = new Thread(new SignalRunnable());
        signalThread.setDaemon(true);
        signalThread.setName("SignalRunnable");
        signalThread.start();
    }

    /**
     * Register signal to system
     * if callback is null, then the current process will ignore this signal
     */
    public synchronized void registerSignal(int signalNumber, Runnable callback, boolean replace) {
        String signalName = signalMap.get(signalNumber);
        if (signalName == null) {
            LOG.warn("Invalid signalNumber " + signalNumber);
            return;
        }

        LOG.info("Begin to register signal of {}", signalName);
        try {
            SignalHandler oldHandler = Signal.handle(new Signal(signalName), this);
            LOG.info("Successfully register {} handler", signalName);

            Runnable old = signalHandlers.put(signalNumber, callback);
            if (old != null) {
                if (!replace) {
                    oldSignalHandlers.put(signalNumber, oldHandler);

                } else {
                    LOG.info("Successfully old {} handler will be replaced", signalName);
                }

            }
            LOG.info("Successfully register signal of {}", signalName);
        } catch (Exception e) {
            LOG.error("Failed to register " + signalName + ":" + signalNumber + ", Signal already used by VM or OS: SIGILL");
        }

    }

    protected class SignalRunnable implements Runnable {
        @Override
        public void run() {
            LOG.info("Start");

            while (isRunning) {
                Signal signal;
                try {
                    signal = waitingSignals.take();
                    if (signal != null) {
                        handle(signal);
                    }
                } catch (Throwable e) {
                    LOG.error("Failed to handle " + e.getCause(), e);
                }
            }
            LOG.info("End");
        }

        public void handle(Signal signal) {
            LOG.info("Receive singal " + signal.getName() + " " + signal.getNumber());

            Runnable runner = signalHandlers.get(signal.getNumber());
            try {
                if (runner == null) {
                    LOG.info("Skip JStorm register handler of signal: {}", signal.getName());
                } else {

                    LOG.info("Begin to handle signal of {}", signal.getName());
                    runner.run();
                    LOG.info("Successfully handle signal of {}", signal.getName());

                }
            } catch (Throwable e) {
                LOG.error("Failed to handle signal of " + signal.getName()
                        + ":" + e.getCause(), e);
            }

            try {
                SignalHandler oldHandler = oldSignalHandlers.get(signal.getNumber());
                if (oldHandler != null) {
                    LOG.info("Begin to run the old singleHandler");
                    oldHandler.handle(signal);
                    // Force to sleep one second to avoid competition
                    JStormUtils.sleepMs(1000);
                    LOG.info("Successfully run the old singleHandler");
                }
            } catch (Throwable e) {
                LOG.error("Failed to run old SignalHandler of  signal: " + signal.getName()
                        + ":" + e.getCause(), e);
            }
        }
    }

    public void handle(Signal signal) {
        waitingSignals.offer(signal);
    }

    /**
     * General "clean up" method which is called when we receive a TERM
     * signal
     * This will likely be superseeded by specific cleanup code
     **/
    public boolean cleanUp() {
        LOG.info("Cleaning up!");
        isRunning = false;
        return true;
    }

    public static void registerJStormSignalHandler() {
        JStormSignalHandler instance = JStormSignalHandler.getInstance();
        int[] signals = {
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                10,
                11,
                12,
                14,
                16,
        };

        for (int signal : signals) {
            instance.registerSignal(signal, null, false);
        }
    }

    public static void main(final String[] args) {
        JStormSignalHandler handler = JStormSignalHandler.getInstance();
        registerJStormSignalHandler();
        while (true) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
