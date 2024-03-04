package csx55.domain;


import csx55.hashing.Task;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

//domain class for messages sent around the ring
public class RingMessage {
    private final int type = Protocol.RING_MESSAGE;
    private String [] elements;

    private String origin; //node which started the message; node with token
    private LinkedList<String> ring = new LinkedList<>();

    private String loadDesc = "";

    public RingMessage(String ringToString, String source) {
        String [] ringEl = ringToString.split("->");
        elements = ringEl;
        origin = source;

        ring.addAll(Arrays.asList(ringEl));

        //link from last to first element
        //if no cycle yet, then create one
        if (!Objects.equals(ring.getFirst(), ring.getLast())) {
            ring.addLast(ringEl[0]);
        }
    }

    public RingMessage() {

    }

    public int getCurrentIndex (String currentNode) {
        int currentIndex = -1;
        int idx = 0;
        for (String el: elements) {
            if (currentNode.equals(el) || el.contains(currentNode)) { //el.contains because we may have 127.0.0.1=234 due to the attached laods
                currentIndex = idx;
                break;
            }
            idx++;
        }
        return currentIndex;
    }

    public String next(String currentNode) {
        int currentIndex = getCurrentIndex(currentNode);
        if (currentIndex == -1) {
            throw new RuntimeException("Element lookup failed in RingMessage.next()");
        }
        int nextIndex = (currentIndex + 1) % ring.size();
        //get next element in ring
        return ring.get(nextIndex);
    }

    public RingMessage unmarshal (byte [] marshalledBytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        din.readInt(); // Read and discard the type

        int ringSize = din.readInt(); // Read the string array size
        String [] ring = new String[ringSize];

        String ringString = "";

        for (int i = 0; i < ringSize; i++) {
            int elementBytesLen = din.readInt(); // Read length of el bytes
            byte[] elBytes = new byte[elementBytesLen];
            din.readFully(elBytes); // Read element bytes

            String el = new String(elBytes);
            if (i != ringSize - 1) {
                ringString = ringString.concat(el + "->");
            }
            else {
                ringString = ringString.concat(el);
            }
        }
        //this.elements = ring;

        int sourceBytesLen = din.readInt();
        byte [] sourceBytes = new byte[sourceBytesLen];
        din.readFully(sourceBytes);

        int loadBytesLen = din.readInt();
        byte [] loadBytes = new byte[loadBytesLen];
        din.readFully(loadBytes);


        String source = new String(sourceBytes);
        String loadDesc = new String(loadBytes);

        RingMessage msg = new RingMessage(ringString, source);
        msg.setLoadDesc(loadDesc);

        byteArrayInputStream.close();
        din.close();

        return msg;
    }

    public byte [] marshal () throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        dout.writeInt(this.ring.size());

        for (String el : this.ring) {
            byte [] elementBytes = el.getBytes();
            dout.writeInt(elementBytes.length); // Write length of element bytes
            dout.write(elementBytes); // Write element bytes
        }

        byte [] sourceBytes = origin.getBytes();
        dout.writeInt(sourceBytes.length);
        dout.write(sourceBytes);

        byte [] loadBytes = loadDesc.getBytes();
        dout.writeInt(loadBytes.length);
        dout.write(loadBytes);

        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }

    public void setLoad(String nodeInfo, int load) {
        if (loadDesc.isEmpty()) {
            this.loadDesc = nodeInfo + "=" + load;
        }
        else {
            //add new Els as csv
            this.loadDesc += "," + nodeInfo + "=" + load;
        }
    }

    public String getLoadDesc () {
        return loadDesc;
    }

    public String toString() {
        StringBuilder out = new StringBuilder();
        int idx = 0;
        for (String el: elements) {
            if (idx != elements.length - 1) {
                out.append(el).append("->");
            }
            else {
                out.append(el);
            }
            idx++;
        }
        return out.toString();
    }

    public void setLoadDesc(String desc) {
        this.loadDesc = desc;
    }

 }
