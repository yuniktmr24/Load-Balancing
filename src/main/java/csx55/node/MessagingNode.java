package csx55.node;

import csx55.domain.*;
import csx55.hashing.Miner;
import csx55.hashing.Task;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class MessagingNode implements Node{
    private static final Logger logger = Logger.getLogger(MessagingNode.class.getName());
    private TCPConnection registryConnection;

    private String nodeIP;
    private int nodePort;
    private final ThreadPool threadPool = new ThreadPool(new Random().nextInt(15) + 2, 10);

    private List <String> neighbors = new ArrayList<>();

    private Map<String, TCPConnection> peerConnections = new ConcurrentHashMap<>();

    //contains loads from all peers (not from self though)
    private Map <String, Integer> loadMap = new ConcurrentHashMap<>();

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

    private List <Task> currentTasks = Collections.synchronizedList(new ArrayList<>());



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

            //TODO : might do this when the actual message rounds begin
            node.generateTasks();

            Thread userThread = new Thread(() -> node.userInput(node));
            userThread.start();

            while (true) {

            }
        } catch (IOException e) {
            logger.severe("Error in main thread" + e.getMessage());
        }
    }

    private void generateTasks() {
        Random rand = new Random();
        int maxTasks = 10; //1000;
        int minTasks = 1;
        int numTasks = rand.nextInt(maxTasks - 1) + minTasks;
        for (int idx = 0; idx < numTasks; idx++) {
            Task task = new Task("192.168.0.1", 1234, 1, new Random().nextInt());
            currentTasks.add(task);
        }
        stats.setGeneratedCount(numTasks);
        stats.setCurrentTasks(numTasks);
    }

    private void loadBalance() {
        int maxLoadInNetwork = this.loadMap.values().stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElse(Integer.MIN_VALUE);



    }

    private void submitTasks (int numTasks) {


        //TODO : before submitting, we perform load balancing maneuvers.
        //TODO: maybe use a task list.size() because the task list content will change after load balancing
        for (int i = 0; i < numTasks; i++) {
            int taskNo = i;

            //submit task
            getThreadPool().submit(() -> {
                //dummy example
                //String msg = Thread.currentThread().getName() + " : Task " + taskNo;
                //System.out.println(msg);
                System.out.println(Thread.currentThread().getName() + " : Task " + taskNo);
                Miner miner = Miner.getInstance();

                Task task = this.currentTasks.get(taskNo);
                miner.mine(task);
                System.out.println(task.toString());

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
                //for testing
                else if (userInput.equals(UserCommands.SEND_TEST_WIRE.getCmd()) || userInput.equals(String.valueOf(UserCommands.SEND_TEST_WIRE.getCmdId()))) {
                    //Task task = new Task("192.168.0.1", 1234, 1, new Random().nextInt());
                    //send most recent node over the wire
                    Task task = this.currentTasks.get(this.currentTasks.size() - 1);

                    this.peerConnections.forEach((k, v) -> {
                        try {
                            //send only one task for test
                            if (task.getOriginNode().isEmpty()) {
                                task.setOriginNode(nodeIP + ":" + nodePort);
                                pushTask(task, v); //v=connection
                            }
                            else {
                                System.out.println("Omitted single push because of oscillation " + task.toString());
                                //send second last then
                                this.currentTasks.get(this.currentTasks.size() - 2).setOriginNode(nodeIP + ":" + nodePort);
                                pushTask(this.currentTasks.get(this.currentTasks.size() - 2), v); //v=connection
                            }
                        } catch (Exception e) {
                            logger.severe("Error talking to neighbor");
                            throw new RuntimeException(e);
                        }
                    });
                }
                //for testing
                else if (userInput.equals(UserCommands.SEND_TEST_WIRE_BULK.getCmd()) || userInput.equals(String.valueOf(UserCommands.SEND_TEST_WIRE_BULK.getCmdId()))) {
                    /* dummy tasks for test
                    Task task = new Task("192.168.0.1", 1234, 1, new Random().nextInt());
                    Task task2 = new Task("192.168.0.1", 1234, 1, new Random().nextInt());
                    Task task3 = new Task("192.168.0.1", 1234, 1, new Random().nextInt());
                    */
                    //send only one for test
                    int oscillatingTasks = 0;
                    for (int i = 0; i < this.currentTasks.size(); i++) {
                        if (this.currentTasks.get(i).getOriginNode().isEmpty()) {
                            oscillatingTasks++;
                        }

                    }
                    Random rand = new Random();
                    int randomTasksToSend = rand.nextInt(this.currentTasks.size() - oscillatingTasks) + 1;
                    Task [] tasklist = new Task[randomTasksToSend];
                    for (int idx = 0; idx < randomTasksToSend; idx++) {
                        if (this.currentTasks.get(idx).getOriginNode().isEmpty()) {
                            tasklist[idx] = this.currentTasks.get(idx);
                            this.currentTasks.get(idx).setOriginNode(nodeIP+":"+nodePort);
                        }
                    }
                    System.out.println("Picked "+ randomTasksToSend + " to send over the wire randomly"+ " Omitted "+ oscillatingTasks + " oscillating tasks ");

                    this.peerConnections.forEach((k, v) -> {
                        try {
                            TaskList taskList = new TaskList(tasklist);
                            pushTasks(taskList, v, k); //v=connection

                        } catch (Exception e) {
                            logger.severe("Error talking to neighbor");
                            throw new RuntimeException(e);
                        }
                    });
                }
                else if (userInput.equals(UserCommands.PRINT_LOADS_ACROSS_NETWORK.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_LOADS_ACROSS_NETWORK.getCmdId()))) {
                    System.out.println("Load Statistics : \n" );
                    this.loadMap.entrySet().forEach((entry) -> {
                        System.out.println(entry.getKey() + "\t" + entry.getValue());
                    });
                    System.out.println(nodeIP+":"+nodePort+ " (self) "+"\t Generated: "+ stats.getGeneratedTasks().get() + "\t pulled : "
                            + stats.getPulledTasks() + "\t pushed : "+ stats.getPushedTasks());

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

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //send task info to peer that requests it.
    public synchronized void sendTaskInfo(ClientConnection peerConnection, TCPConnection connection) {
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
    public synchronized void handleLoadSummaryResponse(LoadSummaryResponse traffic) {
        //loadMap.clear();
        if (loadMap.containsKey(traffic.getNodeIP()+":"+traffic.getNodePort())) {
            loadMap.replace(traffic.getNodeIP()+":"+traffic.getNodePort(), traffic.getCurrentTasks());
        }
        else {
            loadMap.put(traffic.getNodeIP() + ":" + traffic.getNodePort(), traffic.getCurrentTasks());
        }

    }

    public synchronized void pushTask(Task task, TCPConnection targetNode) {
        try {
            targetNode.getSenderThread().sendData(task.marshal());
            stats.incrementPushCount();
            stats.decrementCurrentTasks();
            currentTasks.removeIf(el -> el.getPayload() == task.getPayload());
            System.out.println("Pushed "+ task.toString());
            printCurrentTasks("After single push");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void pushTasks(TaskList taskList, TCPConnection targetNode, String targetNodeInfo) {
        try {
            if (taskList.getTasks().length == 0) {
                System.out.println("Nothing to push ");
                return;
            }
            targetNode.getSenderThread().sendData(taskList.marshal());
            for (int i = 0; i < taskList.getTasks().length; i++) {
                Task pushedTask = taskList.getTasks()[i];
                stats.incrementPushCount();
                stats.decrementCurrentTasks();
                currentTasks.removeIf(el -> el.getPayload() == pushedTask.getPayload());
            }
            printCurrentTasks("(After bulk push)");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void pullTasks(TaskList taskList) {
        System.out.println("Pulled tasks # "+ taskList.getTasks().length);
        for (Task task: taskList.getTasks()) {
            System.out.println(task.toString());
            currentTasks.add(task);
            stats.incrementCurrentTasks();
            stats.incrementPullCount();
        }
        printCurrentTasks("(After bulk pull)");
    }

    public synchronized void pullSingleTask(Task task) {
        System.out.println("Pulled task "+ task.toString());
        currentTasks.add(task);
        stats.incrementCurrentTasks();
        printCurrentTasks("(After Single pull)");
        stats.incrementPullCount();
    }

    private void printCurrentTasks(String appendum) {
        System.out.println("Current tasks list elements : " + appendum);
        for (Task tsk: currentTasks) {
            System.out.println(tsk.toString() + " origin node "+tsk.getOriginNode());
        }
        System.out.println("Number of current tasks "+ currentTasks.size());
        System.out.println("Number of current tasks as per statsEngine "+ stats.getCurrentTasks());
    }
}
