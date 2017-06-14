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
package com.alibaba.jstorm.drpc;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.jstorm.utils.DefaultUncaughtExceptionHandler;
import com.alibaba.jstorm.utils.JStormServerUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.DistributedRPCInvocations;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * @author yannian
 */
public class Drpc implements DistributedRPC.Iface, DistributedRPCInvocations.Iface, Shutdownable {
    private static final Logger LOG = LoggerFactory.getLogger(Drpc.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Begin to start drpc server");
        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());
        final Drpc service = new Drpc();
        service.init();
    }

    private Map conf;

    private THsHaServer handlerServer;

    private THsHaServer invokeServer;

    private AsyncLoopThread clearThread;

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    private THsHaServer initHandlerServer(Map conf, final Drpc service) throws Exception {
        int port = JStormUtils.parseInt(conf.get(Config.DRPC_PORT));
        int workerThreadNum = JStormUtils.parseInt(conf.get(Config.DRPC_WORKER_THREADS));
        int queueSize = JStormUtils.parseInt(conf.get(Config.DRPC_QUEUE_SIZE));

        LOG.info("Begin to init DRPC handler server at port: " + port);

        TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        THsHaServer.Args targs = new THsHaServer.Args(socket);
        targs.workerThreads(64);
        targs.protocolFactory(new TBinaryProtocol.Factory());
        targs.processor(new DistributedRPC.Processor<DistributedRPC.Iface>(service));

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workerThreadNum, workerThreadNum, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize));
        targs.executorService(executor);

        THsHaServer handlerServer = new THsHaServer(targs);
        LOG.info("Successfully inited DRPC handler server at port: " + port);

        return handlerServer;
    }

    private THsHaServer initInvokeServer(Map conf, final Drpc service) throws Exception {
        int port = JStormUtils.parseInt(conf.get(Config.DRPC_INVOCATIONS_PORT));

        LOG.info("Begin to init DRPC invoke server at port: " + port);

        TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        THsHaServer.Args targsInvoke = new THsHaServer.Args(socket);
        targsInvoke.workerThreads(64);
        targsInvoke.protocolFactory(new TBinaryProtocol.Factory());
        targsInvoke.processor(new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(service));

        THsHaServer invokeServer = new THsHaServer(targsInvoke);

        LOG.info("Successfully inited DRPC invoke server at port: " + port);
        return invokeServer;
    }

    private void initThrift() throws Exception {
        handlerServer = initHandlerServer(conf, this);
        invokeServer = initInvokeServer(conf, this);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                Drpc.this.shutdown();
                handlerServer.stop();
                invokeServer.stop();
            }

        });

        LOG.info("Starting distributed RPC servers...");
        new Thread(new Runnable() {

            @Override
            public void run() {
                invokeServer.serve();
            }
        }).start();
        handlerServer.serve();
    }

    private void initClearThread() {
        clearThread = new AsyncLoopThread(new ClearThread(this));
        LOG.info("Successfully started clear thread");
    }

    private void createPid(Map conf) throws Exception {
        String pidDir = StormConfig.drpcPids(conf);
        JStormServerUtils.createPid(pidDir);
    }

    public void init() throws Exception {
        conf = StormConfig.read_storm_config();
        LOG.info("Configuration is \n" + conf);

        createPid(conf);
        initClearThread();
        initThrift();
    }

    public Drpc() {
    }

    @Override
    public void shutdown() {
        if (shutdown.getAndSet(true)) {
            LOG.info("Notify to quit drpc");
            return;
        }

        LOG.info("Begin to shutdown drpc");
        AsyncLoopRunnable.getShutdown().set(true);

        clearThread.interrupt();

        try {
            clearThread.join();
        } catch (InterruptedException ignored) {
        }
        LOG.info("Successfully cleaned up clear thread");

        invokeServer.stop();
        LOG.info("Successfully stopped drpc invoke server");

        handlerServer.stop();
        LOG.info("Successfully stopped drpc handler server");
    }

    private AtomicInteger ctr = new AtomicInteger(0);
    private ConcurrentHashMap<String, Semaphore> idToSem = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Object> idToResult = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> idToStart = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> idToFunction = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, DRPCRequest> idToRequest = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<>();

    public void cleanup(String id) {
        LOG.info("clean id " + id + " @ " + (System.currentTimeMillis()));
        idToSem.remove(id);
        idToResult.remove(id);
        idToStart.remove(id);
        idToFunction.remove(id);
        idToRequest.remove(id);
    }

    @Override
    public String execute(String function, String args) throws TException {
        LOG.info("Received DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));
        int idInc = this.ctr.incrementAndGet();
        int maxvalue = 1000000000;
        int newId = idInc % maxvalue;
        if (idInc != newId) {
            this.ctr.compareAndSet(idInc, newId);
        }

        String strId = String.valueOf(newId);
        Semaphore sem = new Semaphore(0);

        DRPCRequest req = new DRPCRequest(args, strId);
        this.idToStart.put(strId, TimeUtils.current_time_secs());
        this.idToSem.put(strId, sem);
        this.idToFunction.put(strId, function);
        this.idToRequest.put(strId, req);
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(function);
        queue.add(req);
        LOG.info("Waiting for DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            LOG.error("acquire fail ", e);
        }
        LOG.info("Acquired for DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));

        Object result = this.idToResult.get(strId);
        if (!this.idToResult.containsKey(strId)) {
            result = new DRPCExecutionException("Request timed out");   // this request has timed out
        }
        LOG.info("Returning for DRPC request for " + function + " " + args + " at " + (System.currentTimeMillis()));

        this.cleanup(strId);

        if (result instanceof DRPCExecutionException) {
            throw (DRPCExecutionException) result;
        }
        return String.valueOf(result);
    }

    @Override
    public void result(String id, String result) throws TException {
        Semaphore sem = this.idToSem.get(id);
        LOG.info("Received result " + result + " for id " + id + " at " + (System.currentTimeMillis()));
        if (sem != null) {
            this.idToResult.put(id, result);
            sem.release();
        }
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws TException {
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        DRPCRequest req = queue.poll();
        if (req != null) {
            LOG.info("Fetched request for " + functionName + " at " + (System.currentTimeMillis()));
            return req;
        } else {
            return new DRPCRequest("", "");
        }
    }

    @Override
    public void failRequest(String id) throws TException {
        Semaphore sem = this.idToSem.get(id);
        LOG.info("failRequest result  for id " + id + " at " + (System.currentTimeMillis()));
        if (sem != null) {
            this.idToResult.put(id, new DRPCExecutionException("Request failed"));
            sem.release();
        }
    }

    protected ConcurrentLinkedQueue<DRPCRequest> acquireQueue(String function) {
        ConcurrentLinkedQueue<DRPCRequest> reqQueue = requestQueues.get(function);
        if (reqQueue == null) {
            reqQueue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<DRPCRequest> tmp = requestQueues.putIfAbsent(function, reqQueue);
            if (tmp != null) {
                reqQueue = tmp;
            }
        }
        return reqQueue;
    }

    public ConcurrentHashMap<String, Semaphore> getIdToSem() {
        return idToSem;
    }

    public ConcurrentHashMap<String, Object> getIdToResult() {
        return idToResult;
    }

    public ConcurrentHashMap<String, Integer> getIdToStart() {
        return idToStart;
    }

    public ConcurrentHashMap<String, String> getIdToFunction() {
        return idToFunction;
    }

    public ConcurrentHashMap<String, DRPCRequest> getIdToRequest() {
        return idToRequest;
    }

    public AtomicBoolean isShutdown() {
        return shutdown;
    }

    public Map getConf() {
        return conf;
    }

}
