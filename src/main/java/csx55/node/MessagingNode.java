package csx55.node;

import csx55.domain.*;
import csx55.overlay.OverlayNode;
import csx55.threads.ThreadPool;
import csx55.transport.TCPConnection;
import csx55.transport.TCPServerThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class MessagingNode implements Node{
    private static final Logger logger = Logger.getLogger(MessagingNode.class.getName());
    private TCPConnection registryConnection;

    private String nodeIP;
    private int nodePort;
    private final ThreadPool threadPool = new ThreadPool(new Random().nextInt(15) + 2, 10);

    private List <String> neighbors = new ArrayList<>();

    private Map<String, TCPConnection> peerConnections = new ConcurrentHashMap<>();

    private Map <String, Integer> loadMap = new HashMap<>();

    private StatsEngine stats = new StatsEngine();

    public String getNodeIP() {
        return nodeIP;
    }

    public void setNodeIP(String nodeIP) {
        this.nodeIP = nodeIP;
    }

    public int getNodePort() {
        return nodePort;
    }

    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }



    public static void main(String[] args) throws IOException {
        //try (Socket socketToRegistry = new Socket(args[0], Integer.parseInt(args[1]));
             try (Socket socketToRegistry = new Socket("localhost", 12349);
             ServerSocket peerServer = new ServerSocket(0);
        ) {
            System.out.println("Connecting to server...");

            MessagingNode node = new MessagingNode();
            node.setNodeIP(InetAddress.getLocalHost().getHostAddress());
            node.setNodePort(peerServer.getLocalPort());

            Thread messageNodeServerThread = new Thread(new TCPServerThread(node, peerServer));
            messageNodeServerThread.start();

            System.out.println("Thread pool size # " + node.getThreadPoolSize());

            node.registerNode(node, socketToRegistry);
            node.submitTasks();

            Thread userThread = new Thread(() -> node.userInput(node));
            userThread.start();

            while (true) {

            }
        } catch (IOException e) {
            logger.severe("Error in main thread" + e.getMessage());
        }
    }

    private void submitTasks () {
        Random rand = new Random();
        int maxTasks = 10; //1000;
        int minTasks = 1;
        int numTasks = rand.nextInt(maxTasks - 1) + minTasks;
        stats.setGeneratedCount(numTasks);

        //TODO : before submitting, we perform load balancing maneuvers.
        for (int i = 0; i < numTasks; i++) {
            int taskNo = i;

            //submit task //dummy example
            getThreadPool().submit(() -> {
                String msg = Thread.currentThread().getName() + " : Task " + taskNo;
                System.out.println(msg);
                stats.incrementCompletedCount();
            });
        }
    }

    public void registerNode (MessagingNode node, Socket socketToRegistry) {
        try {
            ClientConnection conn = new ClientConnection(node.getNodeIP(), RequestType.REGISTER, node.getNodePort());
            TCPConnection connection = new TCPConnection(node, socketToRegistry);
            //logger.info("Connecting to registry server "+ socketToRegistry.getInetAddress().getHostAddress() + ":"  + socketToRegistry.getPort() +
            //" using "+ socketToRegistry.getLocalAddress().getHostAddress() + ":" + socketToRegistry.getLocalPort());
            connection.getSenderThread().sendData(conn.marshal());
            connection.startConnection();
            this.registryConnection = connection;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void userInput (MessagingNode node) {
        try {
            boolean running = true;
            while (running) {
                // Scanner scan = new Scanner(System.in);
                System.out.println("***************************************");
                System.out.println("[Messaging Node] Enter your Message Node command");
                System.out.println(UserCommands.messageNodeCommandsToString());
                //String userInput = scan.nextLine();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                String userInput = inputReader.readLine();
                System.out.println("User input detected " + userInput);
                if (userInput.equals(UserCommands.EXIT_OVERLAY.getCmd()) || userInput.equals(String.valueOf(UserCommands.EXIT_OVERLAY.getCmdId()))) {
                    //exit everything
                    running = false;
                    System.out.println("[Messaging Node] Exiting Overlay...");
                    ClientConnection conn2 = new ClientConnection(node.getNodeIP(), RequestType.DEREGISTER, node.getNodePort());
                    byte[] dataToSend2 = conn2.marshal();
                    this.registryConnection.getSenderThread().sendData(dataToSend2);
                    //  TimeUnit.SECONDS.sleep(3);
                    // this.registryConnection.closeConnection();
                } else if (userInput.equals(UserCommands.PRINT_NEIGHBOR.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_NEIGHBOR.getCmdId()))) {
                    node.getPeerConnections().forEach((k, v) -> {
                        System.out.println(k + " :  " + v);
                    });
                    System.out.println("Total neighbor Connections # " +node.getPeerConnections().size());
                }
                else if (userInput.equals(UserCommands.MESSAGE_NEIGHBOR.getCmd()) || userInput.equals(String.valueOf(UserCommands.MESSAGE_NEIGHBOR.getCmdId()))) {
                    talkToNeighbor();
                }
                else if (userInput.equals(UserCommands.PRINT_LOADS_ACROSS_NETWORK.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_LOADS_ACROSS_NETWORK.getCmdId()))) {
                    System.out.println("Load Statistics : \n" );
                    this.loadMap.entrySet().forEach((entry) -> {
                        System.out.println(entry.getKey() + "\t" + entry.getValue());
                    });
                    System.out.println(nodeIP+":"+nodePort+ " (self) "+"\t"+ stats.getGeneratedTasks().get());

                }
                else if (userInput.equals(UserCommands.PRINT_NUM_COMPLETED_TASKS.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_NUM_COMPLETED_TASKS.getCmdId()))) {
                    getTotalTasks();
                    System.out.println("Number of completed tasks : " + stats.getCompletedTasks());
                }
            }

        } catch (IOException ex) {

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, TCPConnection> getPeerConnections() {
        return peerConnections;
    }

    public void setNeighbors(Map<String, TCPConnection> neighborConns) {
        this.peerConnections = neighborConns;
    }

    private void talkToNeighbor() {
        ServerResponse resp = (new ServerResponse(RequestType.PEER_MESSAGE, StatusCode.SUCCESS, "Your peer contacted you"));
        this.peerConnections.forEach((k, v) -> {
            try {
                v.getSenderThread().sendData(resp.marshal());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                logger.severe("Error talking to neighbor");
                throw new RuntimeException(e);
            }
        });
    }

    public synchronized void receiveServerData(ServerResponse res) {
        System.out.println("****** New Message round from Registry server ******");
        if (res.getRequestType() != null && res.getRequestType().equals(RequestType.REGISTER)) {
            System.out.println(res.getAdditionalInfo());
        }
        else if (res.getRequestType() != null && res.getRequestType().equals(RequestType.DEREGISTER)) {
            try {
                System.out.println(res.getAdditionalInfo());
                this.registryConnection.closeConnection();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int getThreadPoolSize () {
        return this.threadPool.getNumThreads();
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    private int getTotalTasks() {
        OverlayNode self = null;
//        for (OverlayNode node: overlayNodes) {
//            if (((InetSocketAddress)node.getConnection()
//                    .getSocket().getRemoteSocketAddress()).
//                    getPort() == (this.nodePort)){
//                self = node;
//            }
//        }
        return -1;
    }

    public synchronized void receiveTopologyInfo(TopologyInfo info) {
        System.out.println("Received topology info "+ info);

        String[] elements = info.getNodeRingInfo().split("->");

        // Add each element to the linked list
        neighbors.addAll(Arrays.asList(elements));
        String currentNodeDesc = nodeIP+":"+nodePort;
        //remove self from list of neighbors
        neighbors.removeIf(currentNodeDesc::equals);
        //TODO: after this, we ping each element in the ring and get total tasks
        System.out.println("The list of neighbors : "+ neighbors);

        for (String peer: neighbors) {
            String peerIp = peer.split(":")[0].trim();
            int peerPort = Integer.parseInt(peer.split(":")[1].trim());
            try {
                Socket peerSocket = new Socket(peerIp, peerPort);
               TCPConnection connection = new TCPConnection(this, peerSocket);
               peerConnections.put(peer, connection);

               ClientConnection conn = new ClientConnection(this.getNodeIP(), RequestType.REQUEST_TOTAL_TASK_INFO, this.getNodePort());
               connection.getSenderThread().sendData(conn.marshal());
               connection.startConnection();

            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //send task info to peer that requests it.
    public void sendTaskInfo(ClientConnection peerConnection, TCPConnection connection) {
        String peer = peerConnection.getIpAddress()+":"+peerConnection.getPort();

        TCPConnection peerConn = this.peerConnections.get(peer);

        LoadSummaryResponse load = new LoadSummaryResponse(nodeIP, nodePort, stats);
        try {
            peerConn.getSenderThread().sendData(load.marshal());
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    //record task load info that peer sent as result of the sendTaskInfo function
    public void handleLoadSummaryResponse(LoadSummaryResponse traffic) {
        loadMap.put(traffic.getNodeIP()+":"+traffic.getNodePort(), traffic.getGeneratedTasks());
    }
}
