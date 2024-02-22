package csx55.threads;

import java.nio.file.AtomicMoveNotSupportedException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadPoolThread implements Runnable {
   // private AtomicBoolean execute;
    private Thread thread =  Thread.currentThread();
    private BlockingQueue taskQueue;

    private AtomicBoolean stopped = new AtomicBoolean(false);

    private final CountDownLatch taskCompletionLatch;

    public ThreadPoolThread(String threadName, BlockingQueue queue, CountDownLatch latch) {
        thread.setName(threadName);
        this.taskQueue = queue;
        this.taskCompletionLatch = latch;
    }

    @Override
    public void run() {
        try {
            while (!stopped.get()) {
                Runnable task = (Runnable) taskQueue.take();
                task.run();
                taskCompletionLatch.countDown();
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

    public boolean awaitCompletion() throws InterruptedException {
        taskCompletionLatch.await();
        System.out.println("All tasks complete");
        return true; // All tasks have completed
    }

}
