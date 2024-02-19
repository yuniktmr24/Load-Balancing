package csx55.threads;

import java.nio.file.AtomicMoveNotSupportedException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadPoolThread implements Runnable {
   // private AtomicBoolean execute;
    private Thread thread =  Thread.currentThread();
    private BlockingQueue taskQueue;

    private AtomicBoolean stopped = new AtomicBoolean(false);

    public ThreadPoolThread(String threadName, BlockingQueue queue) {
        thread.setName(threadName);
        this.taskQueue = queue;
    }

    @Override
    public void run() {
        try {
            while (!stopped.get()) {
                Runnable task = (Runnable) taskQueue.take();
                task.run();
            }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
    }

    public synchronized void doStop () {
        this.stopped.set(true);
        this.thread.interrupt();
    }

    public AtomicBoolean isStopped() {
        return this.stopped;
    }


}
