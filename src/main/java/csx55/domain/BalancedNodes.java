package csx55.domain;

import java.io.*;

public class BalancedNodes implements Marshallable{
    private final int type = Protocol.BALANCED_NODES;
    private static String[] balancedNodes;

    // Static initializer to initialize the array
    static {
        balancedNodes = new String[0]; // Initialize empty
    }

    private String balancedNodeInfo; //node:load format
    // Method to add a new node to the balancedNodes array in a thread-safe manner
    public static synchronized void addBalancedNode(String newNode) {
        // Create a new array with increased size to accommodate the new node
        String[] newBalancedNodes = new String[balancedNodes.length + 1];

        // Copy existing elements to new array
        System.arraycopy(balancedNodes, 0, newBalancedNodes, 0, balancedNodes.length);

        // Add the new node to the end of the new array
        newBalancedNodes[balancedNodes.length] = newNode;

        balancedNodes = newBalancedNodes;
    }

    // Method to get the current list of balancedNodes
    public static String[] getBalancedNodes() {
        return balancedNodes;
    }

    public static synchronized void setBalancedNodes(String[] balancedNodes) {
        BalancedNodes.balancedNodes = balancedNodes;
    }

    public String getBalancedNodeInfo() {
        return balancedNodeInfo;
    }

    public synchronized void setBalancedNodeInfo(String balancedNodeInfo) {
        this.balancedNodeInfo = balancedNodeInfo;
    }

    public BalancedNodes unmarshal(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        din.readInt(); // Read and discard the type

        int numNodes = din.readInt(); // Read the number of nodes
        String[] nodes = new String[numNodes];

        for (int i = 0; i < numNodes; i++) {
            int nodeLength = din.readInt(); // Read length of node bytes
            byte[] nodeBytes = new byte[nodeLength];
            din.readFully(nodeBytes); // Read node bytes

            String node = new String(nodeBytes);
            nodes[i] = node; // Store each node
        }
        int balancedNodeInfoLength = din.readInt(); // Read length of node bytes
        byte[] balancedNodeInfoBytes = new byte[balancedNodeInfoLength];
        din.readFully(balancedNodeInfoBytes); // Read node bytes

        String balancedNodeInfo = new String(balancedNodeInfoBytes);
         // Store each node

        BalancedNodes balancedNodes = new BalancedNodes();
        setBalancedNodes(nodes);
        balancedNodes.setBalancedNodeInfo(balancedNodeInfo);


        byteArrayInputStream.close();
        din.close();

        return balancedNodes;
    }

    public byte[] marshal() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        dout.writeInt(balancedNodes.length);

        for (String node : balancedNodes) {
            byte[] nodeBytes = node.getBytes(); // Convert node to bytes
            dout.writeInt(nodeBytes.length); // Write length of node bytes
            dout.write(nodeBytes); // Write node bytes
        }
        byte[] balancedNodeInfoBytes = balancedNodeInfo.getBytes(); // Convert node to bytes
        dout.writeInt(balancedNodeInfoBytes.length); // Write length of node bytes
        dout.write(balancedNodeInfoBytes);

        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }
}
