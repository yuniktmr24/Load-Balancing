package csx55.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TCPSenderThread implements Runnable {
    protected DataOutputStream dout;

    private byte [] payload;
    private boolean terminated = true;


    public TCPSenderThread(Socket socket) throws IOException {
        this.dout = new DataOutputStream(socket.getOutputStream());
    }

    public void terminateSender() throws IOException {
        this.dout.close();
        this.terminated = true;
    }

    public void sendData (final byte [] payload) throws InterruptedException {
        this.payload = payload;
        if (this.terminated) {
            run();
        }
    }
    @Override
    public void run() {
        int bytesSent = 0;
        try {
            this.terminated = false;
            while (payload != null && bytesSent < payload.length) {
            //while (true) {
                int remainingBytes = payload.length - bytesSent;
                int chunkSize = Math.min(remainingBytes, 1000000);
                //byte[] data = queue.take();
                //byte [] data = this.payload;
                // int dataLength = data.length;
                //System.out.println("len" + dataLength);
                dout.writeInt(chunkSize);
                dout.write(payload, bytesSent, chunkSize);
                bytesSent += chunkSize;
                dout.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            this.terminated = true;
            try {
                dout.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        try {
            this.dout.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
