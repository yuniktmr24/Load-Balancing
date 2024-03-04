package csx55.domain;

public class CollatedTrafficStats {
    private String nodeIP;

    private int nodePort;
    private int generatedTotal;

    private int pulledTotal;

    private int pushedTotal;

    private int completedTotal;

    private double percentCompleted;

    private int globalTotal;

    private int totalPulled;

    private int totalPushed;

    private int totalCompleted;



    public CollatedTrafficStats(TaskCompleteResponse stats) {
        nodeIP = stats.getNodeIP();
        nodePort = stats.getNodePort();
        this.generatedTotal = stats.getGeneratedTasks();
        this.pulledTotal = stats.getPulledTasks();
        this.pushedTotal = stats.getPushedTasks();
        this.completedTotal = stats.getCompletedTasks();
    }

    public void addStats (TaskCompleteResponse stats) {
        this.generatedTotal += stats.getGeneratedTasks();
        this.pulledTotal += stats.getPulledTasks();
        this.pushedTotal += stats.getPushedTasks();
        this.completedTotal += stats.getCompletedTasks();
    }

    public void setGlobalTotal(int globalTotal) {
        this.globalTotal = globalTotal;
        this.percentCompleted = ((double) this.completedTotal / this.globalTotal) * 100;
    }

    public String toString() {
            return String.format( "%1$-20s %2$-20s %3$-20s %4$-20s %5$-20s %6$-20s",
                    nodeIP + ":" + Integer.toString( nodePort ), Integer.toString( generatedTotal ),
                    Integer.toString( pulledTotal ), Integer.toString( pushedTotal ),
                    Integer.toString( completedTotal ), Double.toString(percentCompleted) );
        }

    public String getNodeIP() {
        return nodeIP;
    }

    public int getNodePort() {
        return nodePort;
    }

    public int getGeneratedTotal() {
        return generatedTotal;
    }

    public int getPulledTotal() {
        return pulledTotal;
    }

    public int getPushedTotal() {
        return pushedTotal;
    }

    public int getCompletedTotal() {
        return completedTotal;
    }

    public double getPercentCompleted() {
        return percentCompleted;
    }

    public void setPulledTotal(int pulledTotal) {
        this.pulledTotal = pulledTotal;
    }

    public void setPushedTotal(int pushedTotal) {
        this.pushedTotal = pushedTotal;
    }

    public void setCompletedTotal(int completedTotal) {
        this.completedTotal = completedTotal;
    }
}
