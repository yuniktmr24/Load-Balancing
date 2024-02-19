package csx55.domain;

import java.io.*;
import java.util.Objects;


public class ClientConnection implements Marshallable<ClientConnection> {

    private final int type = Protocol.CLIENT_CONNECTION;
    private String ipAddress;
    private RequestType requestType;
    private Integer port;


    public ClientConnection(String ipAddress, RequestType requestType, Integer port) {
        this.ipAddress = ipAddress;
        this.requestType = requestType;
        this.port = port;
    }

    public ClientConnection() {

    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    @Override
    public String toString() {
        return "Node " +
                "ipAddress='" + ipAddress + '\'' +
                ", port=" + port + requestType.toString() +
                '}';
    }

    @Override
    public ClientConnection unmarshal(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));
        din.readInt();
        // Read the length first
        int ipAddressLength = din.readInt();

        // Allocate the byte array for the IP address
        byte[] ipAddressBytes = new byte[ipAddressLength];
        // Read the data into the byte array
        din.readFully(ipAddressBytes);

        String ip = new String(ipAddressBytes);
        RequestType reqType = RequestType.values()[din.readInt()];
        int p = din.readInt();
        byteArrayInputStream.close();
        din.close();
        return new ClientConnection(ip, reqType, p);
    }

    @Override
    public byte[] marshal() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        byte[] ipAddressBytesBytes = this.ipAddress.getBytes();
        int ipAddressLength = ipAddressBytesBytes.length;
        dout.writeInt(ipAddressLength);
        dout.write(ipAddressBytesBytes);
        dout.writeInt(requestType.ordinal());
        dout.writeInt(this.port);

        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }

    //custom logic to prevent duplicates -> ip plus port based duplication constraint
    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ClientConnection other = (ClientConnection) obj;
        return Objects.equals(ipAddress, other.ipAddress) &&
                Objects.equals(port, other.port);
    }
}
