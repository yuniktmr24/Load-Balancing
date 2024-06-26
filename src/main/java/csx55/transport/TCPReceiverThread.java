package csx55.transport;

import csx55.domain.*;
import csx55.hashing.Task;
import csx55.threads.ComputeNode;
import csx55.threads.Node;
import csx55.threads.Registry;

import java.io.*;
import java.net.Socket;

public class TCPReceiverThread implements Runnable{
    private Socket messageSource;
    private Node node;
    private DataInputStream din;

    private byte[] receivedPayload;

    private TCPConnection connection;

    private boolean terminated = false;

    public TCPReceiverThread (Node node, Socket socket, TCPConnection connection) throws IOException {
        messageSource = socket;
        this.node = node;
        din = new DataInputStream(socket.getInputStream());
        this.connection = connection;
    }

    public void terminateReceiver(){
        terminated = true;
    }

    public byte[] getReceivedPayload() {
        return receivedPayload;
    }

    private void setReceivedPayload(byte[] receivedPayload) throws IOException {
        this.receivedPayload = receivedPayload;
    }

    @Override
    public void run() {
        //keep listening until socket is open
        while (!messageSource.isClosed()) {
            try {
                int dataLength = din.readInt();
                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);
                setReceivedPayload(data);
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));
                int domainType = din.readInt();

                if (node instanceof Registry) {
                    try {
                        if (domainType == Protocol.CLIENT_CONNECTION) { //could refactor these numbers to a protocol class
                            ClientConnection conn = new ClientConnection().unmarshal(data);
                            ((Registry) node).handleClient(messageSource, conn, connection);
                        }
                        else if (domainType == Protocol.TASK_COMPLETE) {
                            TaskCompleteResponse complete = new TaskCompleteResponse().unmarshal(data);
                            ((Registry) node).recordCompletedTaskFromMessagingNode(complete);
                        }
//                        else if (domainType == 6) {
//                            TrafficSummaryResponse traffic = new TrafficSummaryResponse().unmarshal(data);
//                            ((Registry) node).handleTrafficSummaryResponse(traffic);
//                        }
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                else if (node instanceof ComputeNode) {
                    //can be peer to
                    try {
                        if (domainType == Protocol.CLIENT_CONNECTION) {
                            ClientConnection peerConnection = new ClientConnection().unmarshal(data);
                            if (peerConnection.getRequestType().equals(RequestType.REQUEST_TOTAL_TASK_INFO)) {
                                ((ComputeNode) node).sendTaskInfo(peerConnection, connection);
                            }
                        }
                        else if (domainType == Protocol.RING_MESSAGE) {
                            RingMessage ring = new RingMessage().unmarshal(data);
                            ((ComputeNode)node).receiveMessageRoundViaRing(ring);
                        }
                        else if (domainType == Protocol.TOPOLOGY_INFO) {
                            TopologyInfo info = new TopologyInfo().unmarshal(data);
                            ((ComputeNode)node).initThreadCount(info);
                        }
                        else if (domainType == Protocol.LOAD_SUMMARY) {
                            LoadSummaryResponse traffic = new LoadSummaryResponse().unmarshal(data);
                            ((ComputeNode) node).handleLoadSummaryResponse(traffic);
                        }
                        else if (domainType == Protocol.TASK) {
                            Task task = new Task().unmarshal(data);
                            ((ComputeNode) node).pullSingleTask(task);
                        }
                        else if (domainType == Protocol.TASK_LIST) {
                            TaskList taskList = new TaskList().unmarshal(data);
                            ((ComputeNode) node).handleTaskMigrations(taskList);
                        }
                        else if (domainType == Protocol.BALANCED_NODES) {
                            BalancedNodes balanced = new BalancedNodes().unmarshal(data);
                            ((ComputeNode) node).copyStaticBalancedNodesInfoToLocal(balanced);
                        }

//                        else if (domainType == 3) {
//                            TaskInitiate startTask = new TaskInitiate().unmarshal(data);
//                            ((ComputeNode)node).initiateMessageRounds(startTask);
//                        }
//                        else if (domainType == 5) {
//                            Message msg = new Message().unmarshal(data);
//                            ((ComputeNode)node).handleMessageRounds(msg);
//                        }
                        else {
                            ServerResponse res = new ServerResponse().unmarshal(data);
                            ((ComputeNode)node).receiveServerData(res);
                        }
                    }
                    catch (Exception ex) {
                        //yeah well move on to the other "exceptional" payload type
                       ex.printStackTrace();
                       this.close();
                        //deserializeBytes(data);
                    }

                }
            } catch (Exception e) {
                //System.out.println("Error in node "+ node.toString());
                if (node instanceof Registry) {
                    System.out.println("Node exited connection");
                }
                this.close();
                //e.printStackTrace();
            }
        }
    }


    public void close() {
        try {
            this.din.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

