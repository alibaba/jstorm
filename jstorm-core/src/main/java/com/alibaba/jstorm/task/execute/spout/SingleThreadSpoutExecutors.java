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
package com.alibaba.jstorm.task.execute.spout;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * spout executor
 * <p/>
 * All spout actions will be done here
 * 
 * @author yannian/Longda
 */
public class SingleThreadSpoutExecutors extends SpoutExecutors {
    private static Logger LOG = LoggerFactory.getLogger(SingleThreadSpoutExecutors.class);

    public SingleThreadSpoutExecutors(Task task) {
        super(task);

    }
    
    @Override
    public void mkPending() {
    	// sending Tuple's TimeCacheMap
        if (ConfigExtension.isTaskBatchTuple(storm_conf)) {
            pending = new RotatingMap<Long, TupleInfo>(Acker.TIMEOUT_BUCKET_NUM, null, false);
        }else {
            pending = new RotatingMap<Long, TupleInfo>(Acker.TIMEOUT_BUCKET_NUM, null, true);
        }
    }

    @Override
    public String getThreadName() {
        return idStr + "-" + SingleThreadSpoutExecutors.class.getSimpleName();
    }

    @Override
    public void run() {
    	if (checkTopologyFinishInit == false ) {
    		initWrapper();
            int delayRun = ConfigExtension.getSpoutDelayRunSeconds(storm_conf);
            long now = System.currentTimeMillis();
            while (!checkTopologyFinishInit){
                // wait other bolt is ready, but the spout can handle the received message
                executeEvent();
                controlQueue.consumeBatch(this);
                if (System.currentTimeMillis() - now > delayRun *  1000){
                    executorStatus.setStatus(TaskStatus.RUN);
                    this.checkTopologyFinishInit = true;
                    LOG.info("wait {} timeout, begin operate nextTuple", delayRun);
                    break;
                }
            }
            while (true){
                JStormUtils.sleepMs(10);
                if (taskStatus.isRun()){
                    this.spout.activate();
                    break;
                }else if (taskStatus.isPause()){
                    this.spout.deactivate();
                    break;
                }
            }
            LOG.info(idStr + " is ready, due to the topology finish init. ");
    	}
    	
        executeEvent();
        controlQueue.consumeBatch(this);

        super.nextTuple();

    }

    private void executeEvent() {
        try {
            exeQueue.consumeBatch(this);

        } catch (Exception e) {
            if (taskStatus.isShutdown() == false) {
                LOG.error("Actor occur unknow exception ", e);
            }
        }

    }

}
