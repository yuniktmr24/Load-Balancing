package csx55.overlay;

import csx55.transport.TCPConnection;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class OverlayNode {
    private TCPConnection connection;

    private List<OverlayNode> neighbors = new ArrayList<>();

    private String descriptor;

    public OverlayNode(TCPConnection connection, String descriptor) {
        this.connection = connection;
        this.descriptor = descriptor;
    }

    public TCPConnection getConnection() {
        return connection;
    }

    public List<OverlayNode> getNeighbors() {
        return new ArrayList<>(neighbors);
    }


    public String getNeighborsList () {
        return this.getNeighbors().stream().map(OverlayNode::getDescriptor).collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        if (connection.getSocket() != null) {
            return "OverlayNode : {" +
                    "Connection to messaging node with IP " + ((InetSocketAddress)connection.getSocket().getRemoteSocketAddress()).getAddress().getHostAddress() +
                    "port=" + ((InetSocketAddress)connection.getSocket().getRemoteSocketAddress()).getPort() +
                    " using registry local IP "+ connection.getSocket().getInetAddress().getHostAddress() + " port: " + connection.getSocket().getLocalPort() +
                    '}';
        }
        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OverlayNode that = (OverlayNode) o;
        return Objects.equals(connection, that.connection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connection);
    }

    public String getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }

    public void setNeighbors(List<OverlayNode> neighbors) {
        this.neighbors = neighbors;
    }

    public void addNeighbors (OverlayNode neighbor) {
        this.neighbors.add(neighbor);
    }

    public void clearGraph() {
        this.neighbors.clear();
    }
}
