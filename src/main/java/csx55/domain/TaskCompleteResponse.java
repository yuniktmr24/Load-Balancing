package csx55.domain;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskCompleteResponse {
    private final int type = Protocol.TASK_COMPLETE;
    private String nodeIP;

    private int nodePort;

    private int generatedTasks;

    private int pulledTasks;

    private int pushedTasks;

    private int completedTasks;

    public TaskCompleteResponse(String host, int port,
                                  StatsEngine stats) {
        this.nodeIP = host;
        this.nodePort = port;
        this.generatedTasks = stats.getGeneratedTasks().get();
        this.pulledTasks = stats.getPulledTasks().get();
        this.pushedTasks = stats.getPushedTasks().get();
        this.completedTasks = stats.getCompletedTasks().get();
    }

    public TaskCompleteResponse() {

    }

    public int getGeneratedTasks() {
        return generatedTasks;
    }

    public int getPulledTasks() {
        return pulledTasks;
    }

    public int getPushedTasks() {
        return pushedTasks;
    }

    public int getCompletedTasks() {
        return completedTasks;
    }

    public byte[] marshal() throws IOException {
        byte[] marshalledBytes = null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout =
                new DataOutputStream( new BufferedOutputStream( outputStream ) );

        dout.writeInt( type );

        byte[] hostBytes = nodeIP.getBytes();
        dout.writeInt( hostBytes.length );
        dout.write( hostBytes );

        dout.writeInt( nodePort );
        dout.writeInt( generatedTasks );
        dout.writeInt( pulledTasks );
        dout.writeInt( pushedTasks );
        dout.writeInt( completedTasks );

        dout.flush();
        marshalledBytes = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public TaskCompleteResponse unmarshal(byte [] marshalledBytes) throws IOException {
        ByteArrayInputStream inputStream =
                new ByteArrayInputStream( marshalledBytes );
        DataInputStream din =
                new DataInputStream( new BufferedInputStream( inputStream ) );

        din.readInt();

        int len = din.readInt();
        byte[] hostBytes = new byte[ len ];
        din.readFully( hostBytes );

        this.nodeIP = new String( hostBytes );

        this.nodePort = din.readInt();
        this.generatedTasks = din.readInt();
        this.pulledTasks = din.readInt();
        this.pushedTasks = din.readInt();
        this.completedTasks = din.readInt();

        inputStream.close();
        din.close();

        return this;
    }


    // Convert the statistics summary response to a readable text format.
    public String toString() {
        return String.format( "%1$-20s %2$-20s %3$-20s %4$-20s %5$-20s",
                nodeIP + ":" + Integer.toString( nodePort ), Integer.toString( generatedTasks ),
                Integer.toString( pulledTasks ), Integer.toString( pushedTasks ),
                Integer.toString( completedTasks ) );
    }

    public String getNodeIP() {
        return nodeIP;
    }

    public int getNodePort() {
        return nodePort;
    }
}
