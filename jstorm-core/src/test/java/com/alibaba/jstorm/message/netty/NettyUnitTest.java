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
package com.alibaba.jstorm.message.netty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.Config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.Flusher;
import com.alibaba.jstorm.daemon.worker.FlusherPool;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

public class NettyUnitTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettyUnitTest.class);

    private static int port = 6700;
    private static int task = 1;
    private static Lock lock = new ReentrantLock();
    private static Condition clientClose = lock.newCondition();
    private static Condition contextClose = lock.newCondition();

    private static Map storm_conf = new HashMap<Object, Object>();
    private static IContext context = null;
    

    @BeforeClass
    public static void setup() {
        storm_conf = Utils.readDefaultConfig();
        ConfigExtension.setLocalWorkerPort(storm_conf, port);
        ConfigExtension.setNettyASyncBlock(storm_conf, false);
        storm_conf.put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, false);

        // Check whether context can be reused or not
        context = TransportFactory.makeContext(storm_conf);
        FlusherPool flusherPool = new FlusherPool(1, 100, 30, TimeUnit.SECONDS);
        Flusher.setFlusherPool(flusherPool);
    }

    private IConnection initNettyServer() {
        return initNettyServer(port);
    }

    private IConnection initNettyServer(int port) {
        ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues = new ConcurrentHashMap<Integer, DisruptorQueue>();
        //ConcurrentHashMap<Integer, DisruptorQueue> deserializeCtrlQueues = new ConcurrentHashMap<Integer, DisruptorQueue>();

        WaitStrategy wait = (WaitStrategy)Utils.newInstance("com.lmax.disruptor.TimeoutBlockingWaitStrategy", 5, TimeUnit.MILLISECONDS);
        DisruptorQueue recvControlQueue = DisruptorQueue.mkInstance("Dispatch-control", ProducerType.MULTI,
                256, wait, false, 0, 0);
        Set<Integer> taskSet = new HashSet<Integer>();
        taskSet.add(1);
        IConnection server = context.bind(null, port, deserializeQueues, recvControlQueue, true, taskSet);

        WaitStrategy waitStrategy = new BlockingWaitStrategy();
        DisruptorQueue recvQueue = DisruptorQueue.mkInstance("NettyUnitTest", ProducerType.SINGLE, 1024, waitStrategy, false, 0, 0);
        server.registerQueue(task, recvQueue);

        return server;
    }
    
    @Test
    public void test_pending_read_server() {
    	System.out.println("!!!!!!!! Start test_pending_read_server !!!!!!!!!!!");
        final String req_msg = "Aloha is the most Hawaiian word.";

        final IConnection server;
        final IConnection client;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);
        JStormUtils.sleepMs(1000);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        NettyServer nServer = (NettyServer) server;
        StormChannelGroup channleGroup = nServer.getChannelGroup();
        String remoteAddress = channleGroup.getAllRemoteAddress().iterator().next();
        System.out.println("!!!!!!!!!!!!!!!!!! All remoteAddress: " + channleGroup.getAllRemoteAddress() + " !!!!!!!!!!!!!!!!!!");
        channleGroup.suspendChannel(remoteAddress);
        System.out.println("!!!!!!!!!!!!!!!!!! Suspend netty channel=" + remoteAddress + " !!!!!!!!!!!!!!!!!!");
        client.send(message);

        final List<String> recvMsg = new ArrayList<String>(); 
        Thread thread = new Thread(new Runnable() {
        	@Override
        	public void run() {
        		System.out.println("!!!!!!!!!!!!!!!!!! Start to receive msg !!!!!!!!!!!!!!!!!");
        		byte[] recv = (byte[]) server.recv(task, 0);
                Assert.assertEquals(req_msg, new String(recv));
                recvMsg.add(new String(recv));
                System.out.println("!!!!!!!!!!!!!!!!!! Finish to receive msg !!!!!!!!!!!!!!!!!!");
        	}
        });
        thread.start();
        
        JStormUtils.sleepMs(1000);
        Assert.assertEquals(true, recvMsg.size() == 0);
        System.out.println("!!!!!!!!!!!!!!!!!! Resume channel=" + remoteAddress + " !!!!!!!!!!!!!!!!!!");
        channleGroup.resumeChannel(remoteAddress);
        JStormUtils.sleepMs(1000);
        
        try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        System.out.println("!!!!!!!!!!!!!!!!!! Recv Msg=" + recvMsg + " !!!!!!!!!!!!!!!!!!");
        Assert.assertEquals(true, recvMsg.size() == 1);
        Assert.assertEquals(req_msg, recvMsg.get(0));

        server.close();
        client.close();

        System.out.println("!!!!!!!!!!!! End test_pending_read_server !!!!!!!!!!!!!");
    }

    @Test
    public void test_small_message() {
        System.out.println("!!!!!!!!Start test_small_message !!!!!!!!!!!");
        String req_msg = "Aloha is the most Hawaiian word.";

        IConnection server = null;
        IConnection client = null;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        client.send(message);

        byte[] recv = (byte[]) server.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv));

        System.out.println("!!!!!!!!!!!!!!!!!!Test one time!!!!!!!!!!!!!!!!!");

        server.close();
        client.close();

        System.out.println("!!!!!!!!!!!!End test_small_message!!!!!!!!!!!!!");
    }

    public String setupLargMsg() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Short.MAX_VALUE * 64; i++) {
            sb.append("Aloha is the most Hawaiian word.");
        }

        return sb.toString();
    }

    @Test
    public void test_large_msg() {
        System.out.println("!!!!!!!!!!start large message test!!!!!!!!");
        String req_msg = setupLargMsg();
        System.out.println("!!!!Finish batch data, size:" + req_msg.length() + "!!!!");

        IConnection server = null;
        IConnection client = null;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        LOG.info("Client send data");
        client.send(message);

        byte[] recv = (byte[]) server.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv));

        client.close();
        server.close();
        System.out.println("!!!!!!!!!!End larget message test!!!!!!!!");
    }

    @Test
    public void test_server_delay()  {
        System.out.println("!!!!!!!!!!Start delay message test!!!!!!!!");
        String req_msg = setupLargMsg();

        IConnection server = null;
        IConnection client = null;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        LOG.info("Client send data");
        client.send(message);
        JStormUtils.sleepMs(1000);

        byte[] recv = (byte[]) server.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv));

        server.close();
        client.close();
        System.out.println("!!!!!!!!!!End delay message test!!!!!!!!");
    }

    @Test
    public void test_first_client()  {
        System.out.println("!!!!!!!!Start test_first_client !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);

        new Thread(new Runnable() {

            @Override
            public void run() {

                lock.lock();
                IConnection client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);
                System.out.println("!!Client has sent data");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        IConnection server = null;

        JStormUtils.sleepMs(1000);
        System.out.println("!!server begin start!!!!!");

        server = initNettyServer();
        JStormUtils.sleepMs(5000);

        System.out.println("Begin to receive message");
        byte[] recv = (byte[]) server.recv(task, 1);
        Assert.assertEquals(req_msg, new String(recv));

        System.out.println("Finished to receive message");

        lock.lock();
		try {
			clientClose.signal();
			server.close();
			try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.term();
		} finally {
			lock.unlock();
		}

        System.out.println("!!!!!!!!!!!!End test_first_client!!!!!!!!!!!!!");
    }

    @Test
    public void test_msg_buffer_timeout() {
        System.out.println("!!!!!!!!Start test_msg_buffer_timeout !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        ConfigExtension.setNettyPendingBufferTimeout(storm_conf, 5 * 1000l);
        final IContext context = TransportFactory.makeContext(storm_conf);

        new Thread(new Runnable() {

            @Override
            public void run() {

                lock.lock();
                IConnection client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);
                System.out.println("!!Client has sent data");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        IConnection server = null;

        JStormUtils.sleepMs(7000);
        System.out.println("!!server begin start!!!!!");

        server = initNettyServer();
        JStormUtils.sleepMs(5000);

        byte[] recv = (byte[]) server.recv(task, 1);
        System.out.println("Begin to receive message. recv message size: " + (recv == null ? 0 : recv.length));
        Assert.assertEquals(null, recv);

        System.out.println("Pending message was timouout:" + (recv == null));

		lock.lock();
		try {
			clientClose.signal();
			server.close();
			try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.term();
		} finally {
			lock.unlock();
		}

		ConfigExtension.setNettyPendingBufferTimeout(storm_conf, 60 * 1000l);
        System.out.println("!!!!!!!!!!!!End test_msg_buffer_timeout!!!!!!!!!!!!!");
    }

    @Test
    public void test_batch()  {
        System.out.println("!!!!!!!!!!Start batch message test!!!!!!!!");
        final int base = 100000;

        final IContext context = TransportFactory.makeContext(storm_conf);
        final IConnection server = initNettyServer();

        new Thread(new Runnable() {

            public void send() {
                final IConnection client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();

                for (int i = 1; i < Short.MAX_VALUE; i++) {

                    String req_msg = String.valueOf(i + base);

                    TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                    list.add(message);
                }

                client.send(list);

                System.out.println("Finish Send ");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();

            }

            @Override
            public void run() {
                lock.lock();
                try {
                    send();
                } finally {
                    lock.unlock();
                }
            }
        }).start();

        for (int i = 1; i < Short.MAX_VALUE; i++) {
            byte[] message = (byte[]) server.recv(task, 0);

            Assert.assertEquals(String.valueOf(i + base), new String(message));

            if (i % 1000 == 0) {
                // System.out.println("Receive " + new String(message));
            }
        }

        System.out.println("Finish Receive ");

        lock.lock();
        try {
	        clientClose.signal();
	        server.close();
	        try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				
			}
	        context.term();
        }finally {
        	lock.unlock();
        }
        System.out.println("!!!!!!!!!!End batch message test!!!!!!!!");
    }

    @Test
    public void test_slow_receive()  {
        System.out.println("!!!!!!!!!!Start test_slow_receive message test!!!!!!!!");
        final int base = 100000;

        final IContext context = TransportFactory.makeContext(storm_conf);
        final IConnection server = initNettyServer();

        new Thread(new Runnable() {

            @Override
            public void run() {
                lock.lock();

                IConnection client = null;

                client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();

                for (int i = 1; i < Short.MAX_VALUE; i++) {

                    String req_msg = String.valueOf(i + base);

                    TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                    list.add(message);

                    if (i % 1000 == 0) {
                        System.out.println("send " + i);
                        client.send(list);
                        list = new ArrayList<TaskMessage>();
                    }

                }

                client.send(list);

                System.out.println("Finish Send ");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();
            }
        }).start();

        for (int i = 1; i < Short.MAX_VALUE; i++) {
            byte[] message = (byte[]) server.recv(task, 0);
            JStormUtils.sleepMs(1);

            Assert.assertEquals(String.valueOf(i + base), new String(message));

            if (i % 1000 == 0) {
                // System.out.println("Receive " + new String(message));
            }
        }

        System.out.println("Finish Receive ");

        lock.lock();
		try {
			clientClose.signal();
			server.close();
			try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.term();
		} finally {
			lock.unlock();
		}
        System.out.println("!!!!!!!!!!End test_slow_receive message test!!!!!!!!");
    }

    @Test
    public void test_slow_receive_big() {
        System.out.println("!!!!!!!!!!Start test_slow_receive_big message test!!!!!!!!");
        final int base = 100;
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);
        final IConnection server = initNettyServer();

        new Thread(new Runnable() {

            @Override
            public void run() {
                final IConnection client = context.connect(null, "localhost", port);

                lock.lock();
                for (int i = 1; i < base; i++) {

                    TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                    System.out.println("send " + i);
                    client.send(message);

                }

                System.out.println("Finish Send ");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        for (int i = 1; i < base; i++) {
            byte[] message = (byte[]) server.recv(task, 0);
            JStormUtils.sleepMs(100);

            Assert.assertEquals(req_msg, new String(message));
            System.out.println("receive msg-" + i);

        }

        System.out.println("Finish Receive ");

        lock.lock();
		try {
			clientClose.signal();
			server.close();
			try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.term();
		} finally {
			lock.unlock();
		}
        System.out.println("!!!!!!!!!!End test_slow_receive_big message test!!!!!!!!");
    }

    @Test
    public void test_client_reboot() {
        System.out.println("!!!!!!!!!!Start client reboot test!!!!!!!!");
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);

        new Thread(new Runnable() {

            @Override
            public void run() {

                IConnection client = context.connect(null, "localhost", port);

                lock.lock();

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);

                System.out.println("Send first");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                client.close();

                IConnection client2 = context.connect(null, "localhost", port);
                System.out.println("!!!!!!! restart client !!!!!!!!!!");

                client2.send(message);
                System.out.println("Send second");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client2.close();
                contextClose.signal();
                lock.unlock();
            }
        }).start();

        IConnection server = initNettyServer();

        byte[] recv = (byte[]) server.recv(task, 0);
        System.out.println("Sever receive first");
        Assert.assertEquals(req_msg, new String(recv));

        try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        byte[] recv2 = (byte[]) server.recv(task, 0);
        System.out.println("Sever receive second");
        Assert.assertEquals(req_msg, new String(recv2));

		lock.lock();
		try {
			clientClose.signal();
			server.close();
			try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.term();
		} finally {
			lock.unlock();
		}
        System.out.println("!!!!!!!!!!End client reboot test!!!!!!!!");
    }

    @Test
    public void test_server_reboot() {
        System.out.println("!!!!!!!!!!Start server reboot test!!!!!!!!");
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);
        IConnection server = null;

        new Thread(new Runnable() {

            @Override
            public void run() {
                final IConnection client = context.connect(null, "localhost", port);

                lock.lock();

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);

                System.out.println("Send first");

                JStormUtils.sleepMs(10000);

                System.out.println("Begin to Send second");
                client.send(message);
                System.out.println("Send second");

                JStormUtils.sleepMs(15000);
                client.send(message);
                System.out.println("Send third time");

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        server = initNettyServer();

        byte[] recv = (byte[]) server.recv(task, 0);
        System.out.println("Receive first");
        Assert.assertEquals(req_msg, new String(recv));

        server.close();

        System.out.println("!!shutdow server and sleep 30s, please wait!!");
        try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        IConnection server2 = server = initNettyServer();
        System.out.println("!!!!!!!!!!!!!!!!!!!! restart server !!!!!!!!!!!");

        byte[] recv2 = (byte[]) server2.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv2));

        lock.lock();
		try {
			clientClose.signal();
			server2.close();
			try {
				contextClose.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.term();
		} finally {
			lock.unlock();
		}
        System.out.println("!!!!!!!!!!End server reboot test!!!!!!!!");
    }

    /**
     * Due to there is only one client to one server in one jvm It can't do this test
     * 
     * @throws InterruptedException
     */
    public void test_multiple_client()  {
        System.out.println("!!!!!!!!Start test_multiple_client !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        final int clientNum = 3;
        final AtomicLong received = new AtomicLong(clientNum);

        for (int i = 0; i < clientNum; i++) {

            new Thread(new Runnable() {

                @Override
                public void run() {

                    IConnection client = context.connect(null, "localhost", port);

                    List<TaskMessage> list = new ArrayList<TaskMessage>();
                    TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                    list.add(message);

                    client.send(message);
                    System.out.println("!!Client has sent data");

                    while (received.get() != 0) {
                        JStormUtils.sleepMs(1000);
                    }

                    client.close();

                }
            }).start();
        }

        IConnection server = null;

        JStormUtils.sleepMs(1000);
        System.out.println("!!server begin start!!!!!");

        server = initNettyServer();

        for (int i = 0; i < clientNum; i++) {
            byte[] recv = (byte[]) server.recv(task, 0);
            Assert.assertEquals(req_msg, new String(recv));
            received.decrementAndGet();
        }

        server.close();

        System.out.println("!!!!!!!!!!!!End test_multiple_client!!!!!!!!!!!!!");
    }

    @Test
    public void test_multiple_server()  {
        System.out.println("!!!!!!!!Start test_multiple_server !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        final int clientNum = 3;
        final AtomicLong received = new AtomicLong(clientNum);

        for (int i = 0; i < clientNum; i++) {
            final int realPort = port + i;

            new Thread(new Runnable() {

                @Override
                public void run() {

                    IConnection server = null;

                    JStormUtils.sleepMs(1000);
                    System.out.println("!!server begin start!!!!!");

                    server = initNettyServer(realPort);

                    byte[] recv = (byte[]) server.recv(task, 0);
                    Assert.assertEquals(req_msg, new String(recv));
                    received.decrementAndGet();
                    System.out.println("!!server received !!!!!" + realPort);

                    server.close();

                }
            }).start();
        }

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        List<IConnection> clients = new ArrayList<IConnection>();

        for (int i = 0; i < clientNum; i++) {
            final int realPort = port + i;

            IConnection client = context.connect(null, "localhost", realPort);
            clients.add(client);

            client.send(message);
            System.out.println("!!Client has sent data to " + realPort);
        }

        while (received.get() != 0) {
            JStormUtils.sleepMs(1000);
        }

        for (int i = 0; i < clientNum; i++) {
            clients.get(i).close();
        }

        System.out.println("!!!!!!!!!!!!End test_multiple_server!!!!!!!!!!!!!");
    }
}
