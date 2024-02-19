package csx55.hashing;

import java.nio.charset.StandardCharsets;

public class Task {
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
}