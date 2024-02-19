package csx55.threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPool {
    private BlockingQueue taskQueue; //queue of runnables
    private List<ThreadPoolThread> threadList = new ArrayList<>();

    private AtomicBoolean stopped = new AtomicBoolean(false);

    public ThreadPool(int numThreads, int maxNumberOfTasks) {
        taskQueue = new ArrayBlockingQueue(maxNumberOfTasks);

        for (int i = 0; i < numThreads; i++){
            ThreadPoolThread thread = new ThreadPoolThread("thread "+ i, taskQueue);
            threadList.add(thread);
        }
        System.out.println("Thread pool created with # of threads "+ numThreads);

        for (ThreadPoolThread runnable: threadList) {
            new Thread(runnable).start();
        }

    }

    public synchronized void submit (Runnable task) {
        if (stopped.get()) throw new IllegalStateException("Threadpoool stopped");
        this.taskQueue.offer(task);
    }

    public synchronized void stop () {
        this.stopped.set(true);
        for (ThreadPoolThread thread: threadList) {
            thread.doStop();
        }
    }

    public int getNumThreads() {
        return this.threadList.size();
    }

}
