package csx55.domain;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StatsEngine {
    private AtomicInteger generatedTasks;

    private AtomicInteger pulledTasks;

    private AtomicInteger pushedTasks;

    private AtomicInteger completedTasks;


    public StatsEngine() {
        this.generatedTasks = new AtomicInteger( 0 );
        this.pulledTasks = new AtomicInteger( 0 );
        this.pushedTasks = new AtomicInteger( 0 );
        this.completedTasks = new AtomicInteger( 0 );
    }

    public void incrementPullCount() {
        this.pulledTasks.getAndIncrement();
    }

    public void incrementPushCount() {
        this.pushedTasks.getAndIncrement();
    }

    public void incrementCompletedCount() {
        this.completedTasks.getAndIncrement();
    }

    public void setGeneratedCount(int val) {
        this.generatedTasks.set(val);
    }



    public void reset() {
        this.generatedTasks.set( 0 );
        this.completedTasks.set( 0 );
        this.pushedTasks.set( 0 );
        this.pulledTasks.set( 0 );
    }

    @Override
    public String toString() {
        return "StatsEngine{" +
                "generatedTasks=" + generatedTasks +
                ", pulledTasks=" + pulledTasks +
                ", pushedTasks=" + pushedTasks +
                ", completedTasks=" + completedTasks +
                '}';
    }

    public AtomicInteger getGeneratedTasks() {
        return generatedTasks;
    }

    public AtomicInteger getPulledTasks() {
        return pulledTasks;
    }

    public AtomicInteger getPushedTasks() {
        return pushedTasks;
    }

    public AtomicInteger getCompletedTasks() {
        return completedTasks;
    }
}
