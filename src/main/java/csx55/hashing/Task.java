package csx55.hashing;

import csx55.domain.ClientConnection;
import csx55.domain.Marshallable;
import csx55.domain.Protocol;
import csx55.domain.RequestType;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Task implements Marshallable {
    private final int type = Protocol.TASK;
    private final String ip;
    private final int port;
    private final int roundNumber;
    private final int payload;
    private long timestamp;
    private long threadId;
    private int nonce;

    public Task(String ip, int port, int roundNumber, int payload) {
        this.ip = ip;
        this.port = port;
        this.roundNumber = roundNumber;
        this.payload = payload;
        this.timestamp = 0L;
        this.threadId = 0L;
        this.nonce = 0;
    }

    public Task() {
        //dummy inits to make the compiler happy. to be overwritten.
        this.ip = "";
        this.port = 0;
        this.roundNumber = 0;
        this.payload = 0;
    }

    public void setTimestamp() {
        this.timestamp = System.currentTimeMillis();
    }

    public void setThreadId() {
        this.threadId = Thread.currentThread().getId();
    }

    public void setNonce(int nonce) {
        this.nonce = nonce;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getRoundNumber() {
        return roundNumber;
    }

    public int getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getNonce() {
        return nonce;
    }

    public String toString() {
        return ip + ":" + port + ":" + roundNumber + ":" + payload + ":" + timestamp + ":" + threadId + ":" + nonce;
    }

    public byte[] toBytes() {
        return toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Task unmarshal(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));
        din.readInt();

        // private final String ip;
        //    private final int port;
        //    private final int roundNumber;
        //    private final int payload;
        //    private long timestamp;
        //    private long threadId;
        //    private int nonce;
        // Read the length first
        int ipAddressLength = din.readInt();
        // Allocate the byte array for the IP address
        byte[] ipAddressBytes = new byte[ipAddressLength];
        // Read the data into the byte array
        din.readFully(ipAddressBytes);

        String ip = new String(ipAddressBytes);
        int portV = din.readInt();
        int roundNum = din.readInt();
        int payloadV = din.readInt();
        Task task = new Task(ip, portV, roundNum, payloadV);
        long timestamp = din.readLong();
        long threadId = din.readLong();
        int nonce = din.readInt();

        task.setNonce(nonce);

        byteArrayInputStream.close();
        din.close();
        return task;
    }

    @Override
    public byte[] marshal() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        byte[] ipAddressBytesBytes = this.ip.getBytes();
        int ipAddressLength = ipAddressBytesBytes.length;
        dout.writeInt(ipAddressLength);
        dout.write(ipAddressBytesBytes);
        dout.writeInt(port);
        dout.writeInt(roundNumber);
        dout.writeInt(payload);
        dout.writeLong(timestamp);
        dout.writeLong(threadId);
        dout.writeInt(nonce);

        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }
}