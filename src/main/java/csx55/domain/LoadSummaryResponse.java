package csx55.domain;

import java.io.*;

public class LoadSummaryResponse implements Marshallable{

        private final int type = Protocol.LOAD_SUMMARY;

        private String nodeIP;

        private int nodePort;

        private int currentTasks;

        private int generatedTasks;

        private int completedTasks;

        private int pushedTasks;

        private int pulledTasks;

        public LoadSummaryResponse(String host, int port,
                                   StatsEngine stats) {
            this.nodeIP = host;
            this.nodePort = port;
            this.currentTasks = stats.getCurrentTasks().get();
            this.completedTasks = stats.getCompletedTasks().get();
            this.generatedTasks = stats.getGeneratedTasks().get();
            this.pushedTasks = stats.getPushedTasks().get();
            this.pulledTasks = stats.getPulledTasks().get();
        }

    public LoadSummaryResponse() {

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
        dout.writeInt(currentTasks);
        dout.writeInt(generatedTasks);
        dout.writeInt(completedTasks);
        dout.writeInt(pushedTasks);
        dout.writeInt(pulledTasks);

        dout.flush();
        marshalledBytes = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledBytes;
    }

    public LoadSummaryResponse unmarshal(byte [] marshalledBytes) throws IOException {
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
        this.currentTasks = din.readInt();
        this.generatedTasks = din.readInt();
        this.completedTasks = din.readInt();
        this.pushedTasks = din.readInt();
        this.pulledTasks = din.readInt();

        inputStream.close();
        din.close();

        return this;
    }


     // Convert the statistics summary response to a readable text format.
    public String toString() {
        return String.format( "%1$-20s %2$-20s %3$-20s %4$-20s %5$-20s %6$-20s",
                nodeIP + ":" + Integer.toString( nodePort ), Integer.toString( generatedTasks),
                Integer.toString( pulledTasks ), Integer.toString( pushedTasks ),
                Integer.toString( completedTasks ), Double.toString( ((double) completedTasks /generatedTasks) * 100 ) );
    }

    public int getGeneratedTasks() {
        return generatedTasks;
    }

    public int getCompletedTasks() {
        return completedTasks;
    }

    public int getPushedTasks() {
        return pushedTasks;
    }

    public int getPulledTasks() {
        return pulledTasks;
    }

    public String getNodeIP() {
        return nodeIP;
    }

    public int getNodePort() {
        return nodePort;
    }

    public int getCurrentTasks() {
        return currentTasks;
    }
}

