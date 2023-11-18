package com.antgroup.geaflow.Util;

import sun.awt.Mutex;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadCounter {
    private Lock lock ;
    private Condition condition;
    private int runningThread;

    public ThreadCounter(int n){
        runningThread = n;
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    public void finish(){
        lock.lock();
        runningThread --;
        condition.signalAll();
        lock.unlock();
    }

    public void check() {
        lock.lock();
        try {
            while (runningThread > 0) {
                condition.await();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }
}

