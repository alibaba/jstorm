package com.alibaba.jstorm.daemon.worker;

import java.util.Map;
import java.util.Set;

/**
 * Created by yunfan on 2017/2/28.
 */
public interface IWorkerReportError {

    public void init(Map conf, Object obj);

    public void report(String topology_id, Integer worker_port,
                       Set<Integer> tasks, String error, int errorCode);
}
