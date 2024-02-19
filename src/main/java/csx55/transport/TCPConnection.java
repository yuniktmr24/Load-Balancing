package csx55.transport;

import csx55.node.Node;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class TCPConnection {
    private Socket socket;

    private Node node;
    private TCPSenderThread senderThread;
    private TCPReceiverThread receiverThread;

    public TCPConnection(Node node, Socket socket) throws IOException {
        this.node = node;
        this.socket = socket;
        this.senderThread = new TCPSenderThread(socket);
        this.receiverThread = new TCPReceiverThread(node, socket, this);

    }

    public TCPReceiverThread getReceiverThread() {
        return receiverThread;
    }

    public TCPSenderThread getSenderThread() {
        return senderThread;
    }

    public void startConnection() {
        new Thread(this.senderThread).start();
        new Thread(this.receiverThread).start();
    }

    public void closeConnection() throws IOException, InterruptedException {
        TimeUnit.SECONDS.sleep( 1 );
        this.socket.close();
        this.senderThread.terminateSender();
        this.receiverThread.terminateReceiver();
    }

    public void setNode(Node node) {
        this.node = node;
    }

    @Override
    public String toString() {
        return "TCPConnection{" +
                "socket=" + socket +
                ", node=" + node +
                '}';
    }

    public Socket getSocket() {
        return socket;
    }
}
