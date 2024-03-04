package csx55.threads;

import csx55.hashing.Task;

import java.nio.file.AtomicMoveNotSupportedException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadPoolThread implements Runnable {
   // private AtomicBoolean execute;
    private final Thread thread =  new Thread();
    private BlockingQueue taskQueue;

    private AtomicBoolean stopped = new AtomicBoolean(false);

    private CountDownLatch taskCompletionLatch = null;

    public ThreadPoolThread(String threadName, BlockingQueue queue, CountDownLatch latch) {
        try {
            thread.setName(threadName);
            this.taskQueue = queue;
            this.taskCompletionLatch = latch;
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            while (!stopped.get()) {
                Object taskObj = taskQueue.take();
                Runnable task = (Runnable) taskObj;
                task.run();
                //System.out.println(((Task)taskObj).toString()); //well java throws a cast exception
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
