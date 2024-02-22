package csx55.domain;

import java.io.*;

public class TopologyInfo {
    private final int type = Protocol.TOPOLOGY_INFO;

    private String nodeRingInfo; //csv

    private int numThreads;

    public TopologyInfo(String nodeRingInfo, int numThreads) {
        this.nodeRingInfo = nodeRingInfo;
        this.numThreads = numThreads;
    }

    public TopologyInfo() {
    }

    public byte [] marshal () throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        byte[] infoBytes = this.nodeRingInfo.getBytes();
        int infoLength = infoBytes.length;
        dout.writeInt(infoLength);
        dout.write(infoBytes);
        dout.writeInt(numThreads);
        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }

    public TopologyInfo unmarshal(byte [] marshalledBytes) throws IOException {
        if (marshalledBytes != null) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));
            din.readInt();
            int infoLength = din.readInt();
            byte[] infoBytes = new byte[infoLength];
            din.readFully(infoBytes);
            String nodeInfo = new String(infoBytes);
            int numThreads = din.readInt();
            byteArrayInputStream.close();
            din.close();

            return new TopologyInfo(nodeInfo, numThreads);
        }
        return null;
    }

    @Override
    public String toString() {
        return "TopologyInfo: " +
                "Elements in the ring = '" + nodeRingInfo;
    }

    public String getNodeRingInfo() {
        return nodeRingInfo;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }
}
