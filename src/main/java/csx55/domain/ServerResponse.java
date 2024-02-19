package csx55.domain;

import java.io.*;

public class ServerResponse implements Marshallable <ServerResponse>{
    private final int type = Protocol.SERVER_RESPONSE;
    public RequestType requestType;
    public StatusCode statusCode;
    public String additionalInfo;

    public ServerResponse(RequestType requestType, StatusCode statusCode, String additionalInfo) {
        this.requestType = requestType;
        this.statusCode = statusCode;
        this.additionalInfo = additionalInfo;
    }

    public ServerResponse() {

    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(StatusCode statusCode) {
        this.statusCode = statusCode;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    @Override
    public String toString() {
        return "ServerResponse{" +
                "requestType=" + requestType +
                ", statusCode=" + statusCode +
                ", additionalInfo='" + additionalInfo + '\'' +
                '}';
    }

    @Override
    public ServerResponse unmarshal(byte[] marshalledBytes) throws IOException {
        if (marshalledBytes != null) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
            DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));
            din.readInt();
            int infoLength = din.readInt();
            byte[] infoBytes = new byte[infoLength];
            din.readFully(infoBytes);
            String info = new String(infoBytes);
            RequestType reqType = RequestType.values()[din.readInt()];
            StatusCode st = StatusCode.values()[din.readInt()];
            byteArrayInputStream.close();
            din.close();

            return new ServerResponse(reqType, st, info);
        }
        return null;
    }

    @Override
    public byte[] marshal() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        byte[] infoBytes = this.additionalInfo.getBytes();
        int infoLength = infoBytes.length;
        dout.writeInt(infoLength);
        dout.write(infoBytes);
        dout.writeInt(requestType.ordinal());
        dout.writeInt(statusCode.ordinal());
        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }

    public int getType() {
        return type;
    }
}
