package com.alibaba.jstorm.daemon.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class FlusherPool {
    private Timer _timer = new Timer("flush-trigger", true);
    private ThreadPoolExecutor _exec;
    private HashMap<Long, ArrayList<Flusher>> _pendingFlush = new HashMap<>();
    private HashMap<Long, TimerTask> _tt = new HashMap<>();

    public FlusherPool(int corePoolSize,
                       int maximumPoolSize,
                       long keepAliveTime,
                       TimeUnit unit) {
        this._exec = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, new ArrayBlockingQueue<Runnable>(1024), new ThreadPoolExecutor.DiscardPolicy());
    }

    public synchronized void start(Flusher flusher, final long flushInterval) {
        ArrayList<Flusher> pending = _pendingFlush.get(flushInterval);
        if (pending == null) {
            pending = new ArrayList<>();
            TimerTask t = new TimerTask() {
                @Override
                public void run() {
                    invokeAll(flushInterval);
                }
            };
            _pendingFlush.put(flushInterval, pending);
            _timer.schedule(t, flushInterval, flushInterval);
            _tt.put(flushInterval, t);
        }
        pending.add(flusher);
    }

    private synchronized void invokeAll(long flushInterval) {
        ArrayList<Flusher> tasks = _pendingFlush.get(flushInterval);
        if (tasks != null) {
            for (Flusher f : tasks) {
                _exec.submit(f);
            }
        }
    }

    public synchronized void stop(Flusher flusher, long flushInterval) {
        ArrayList<Flusher> pending = _pendingFlush.get(flushInterval);
        pending.remove(flusher);
        if (pending.size() == 0) {
            _pendingFlush.remove(flushInterval);
            _tt.remove(flushInterval).cancel();
        }
    }

    public Future<?> submit(Runnable runnable){
        return _exec.submit(runnable);
    }

    public void shutdown() {
        if (this._exec != null) {
            this._exec.shutdown();
            
        }
        
        this._timer.cancel();
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (this._exec != null) {
            this._exec.awaitTermination(timeout, unit);
        }
    }

}