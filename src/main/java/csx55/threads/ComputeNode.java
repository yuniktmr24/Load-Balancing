package csx55.threads;

import csx55.domain.*;
import csx55.hashing.Miner;
import csx55.hashing.Task;
import csx55.overlay.OverlayNode;
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
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ComputeNode implements Node{
    private static final Logger logger = Logger.getLogger(ComputeNode.class.getName());
    private TCPConnection registryConnection;

    private String nodeIP;
    private int nodePort;

    //threadpool size comes in from registry TODO
    private ThreadPool threadPool = null;

    private List <String> neighbors = new ArrayList<>();

    private List <String> originalRing = new ArrayList<>();

    private Map<String, TCPConnection> peerConnections = new ConcurrentHashMap<>();

    //contains loads from all peers (not from self though)
    private Map <String, Integer> loadMap = new ConcurrentHashMap<>();

    private StatsEngine stats = new StatsEngine();
    private int threadCount;

    private CountDownLatch taskGeneratedCounter;

    private CountDownLatch loadStatisticsMessageAroundRingCounter;

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

    private List <String> balancedNodesLocalCopy = Collections.synchronizedList(new ArrayList<>());

    private int taskRounds;

    private boolean loadBalanceOriginTokenReceived = false;

    private CountDownLatch pullCounter;

    public static void main(String[] args) throws IOException {
        //try (Socket socketToRegistry = new Socket(args[0], Integer.parseInt(args[1]));
             try (Socket socketToRegistry = new Socket("localhost", 12331);
             ServerSocket peerServer = new ServerSocket(0);
        ) {
            logger.info("Connecting to server...");

            ComputeNode node = new ComputeNode();
            node.setNodeIP(InetAddress.getLocalHost().getHostAddress());
            node.setNodePort(peerServer.getLocalPort());

            Thread messageNodeServerThread = new Thread(new TCPServerThread(node, peerServer));
            messageNodeServerThread.start();

            node.registerNode(node, socketToRegistry);

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
        int maxTasks = 1000; //1000;
        int minTasks = 1;
        int numTasks = rand.nextInt(maxTasks - 1) + minTasks;
        taskGeneratedCounter = new CountDownLatch(numTasks);
        for (int idx = 0; idx < numTasks; idx++) {
            Task task = new Task(nodeIP, nodePort, 1, new Random().nextInt());
            currentTasks.add(task);
            taskGeneratedCounter.countDown();
        }
        try {
            taskGeneratedCounter.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Generated "+ numTasks + " tasks");
        stats.setGeneratedCount(numTasks);
        stats.setCurrentTasks(numTasks);
        int selfLoad = stats.getCurrentTasks().get();
        this.loadMap.put(this.nodeIP+":"+this.nodePort, selfLoad);
        //fresh set of tasks. fresh loads.
        //TODO: peer loads. Replace with message rounds going aroundf the ring
        //logger.info("Latch of size "+ this.neighbors.size());
        /*direct channel approach
        loadStatisticsMessageAroundRingCounter = new CountDownLatch(this.neighbors.size());
        directChannelToNeighbor();
        */

        //msg round start
        connectToNeighbor();
        loadStatisticsMessageAroundRingCounter = new CountDownLatch(1);
        initiateMessageRound();
    }

    //called via receiver thread
    public void receiveMessageRoundViaRing (RingMessage ringMessage) {
        //back at the source node where the message started
        if (loadBalanceOriginTokenReceived) {
            //loadMap descs are csv
            String loadMap = ringMessage.getLoadDesc();
            if (!loadMap.contains(",")) {
                String[] nodeInfo = loadMap.split("=");
                this.loadMap.put(nodeInfo[0], Integer.parseInt(nodeInfo[1]));
            }
            for (String node: loadMap.split(",")) {
                //don't worry about own load, only concerned about peers here
                if (!node.equals(nodeIP+":"+nodePort)) {
                    String[] nodeInfo = node.split("=");
                    this.loadMap.put(nodeInfo[0], Integer.parseInt(nodeInfo[1]));
                }
            }
            //wait for message round to complete before load balancing
            loadStatisticsMessageAroundRingCounter.countDown();
        }
        //if not the source node, then please put your current local load info here
        else {
            try {
                //wait for all tasks to be generated
                taskGeneratedCounter.await();
                ringMessage.setLoad(nodeIP + ":" + nodePort, currentTasks.size());
                String nextNode = ringMessage.next(nodeIP + ":" + nodePort);
                TCPConnection nextNodeConn = this.peerConnections.get(nextNode);
                nextNodeConn.getSenderThread().sendData(ringMessage.marshal());
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //at source aka token holder node we start the ring message to get total
    private void initiateMessageRound() {
        if (loadBalanceOriginTokenReceived) {
            String originalRingString = originalRing.stream().map(String::trim).collect(Collectors.joining("->"));

            RingMessage ringMessage = new RingMessage(originalRingString, nodeIP+":"+nodePort);
            String nextNode = ringMessage.next(nodeIP+":"+nodePort);
            //System.out.println(ringMessage.toString());
            TCPConnection nextNodeConn = this.peerConnections.get(nextNode);
            try {
                nextNodeConn.getSenderThread().sendData(ringMessage.marshal());
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void connectToNeighbor () {
        for (String peer: neighbors) {
            String peerIp = peer.split(":")[0].trim();
            int peerPort = Integer.parseInt(peer.split(":")[1].trim());
            try {
                Socket peerSocket = new Socket(peerIp, peerPort);
                TCPConnection connection = new TCPConnection(this, peerSocket);
                connection.startConnection();
                peerConnections.put(peer, connection);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    @Deprecated
    private void directChannelToNeighbor() {
        for (String peer: neighbors) {
            String peerIp = peer.split(":")[0].trim();
            int peerPort = Integer.parseInt(peer.split(":")[1].trim());
            try {
                Socket peerSocket = new Socket(peerIp, peerPort);
                TCPConnection connection = new TCPConnection(this, peerSocket);
                connection.startConnection();
                peerConnections.put(peer, connection);

                if (loadBalanceOriginTokenReceived) {
                    ClientConnection conn = new ClientConnection(this.getNodeIP(), RequestType.REQUEST_TOTAL_TASK_INFO, this.getNodePort());
                    //start round to request load info from the peers
                    connection.getSenderThread().sendData(conn.marshal());
                    //connection.startConnection();
                }

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void loadBalanceInitThreadPoolAndSubmitTasks() throws IOException, InterruptedException {
        if (loadBalanceOriginTokenReceived) {
            //logger.info("Waiting for latch release. current elements in peer map : "+ this.loadMap.keySet() + " latch size : "+ loadStatisticsMessageAroundRingCounter.getCount());
            loadStatisticsMessageAroundRingCounter.await();
            //logger.info("Latch released : loadmap SIZE "+ this.loadMap.size());
            //lets add the current nodes load to the loadMap which has peer loads
            logger.info("Starting load balancing");
            int selfLoad = stats.getCurrentTasks().get();
//        this.loadMap.put(this.nodeIP+":"+this.nodePort, selfLoad);

            //exclude balanced nodes from compute
            Optional<Map.Entry<String, Integer>> entryWithMaxValue = getMaxLoadEntrySet();
            Optional<Map.Entry<String, Integer>> entryWithMinValue = getMinLoadEntrySet();

            Map.Entry<String, Integer> maxEntry = entryWithMaxValue.get();
            int maxLoadInNetwork = maxEntry.getValue();
            String nodeWithMaxLoad = maxEntry.getKey();

            Map.Entry<String, Integer> minEntry = entryWithMinValue.get();
            int minLoadInNetwork = minEntry.getValue();
            String nodeWithMinLoad = minEntry.getKey();

            //this means there is only one unbalanced node, (which is self). So, we terminate
            //as other nodes have already attained equilibrium.
            //but wait
            if (nodeWithMaxLoad.equals(nodeWithMinLoad)) {
                logger.info("All other nodes balanced. So no other node available for" +
                        " further balance ops. So terminating");
                //printCurrentLoadMap();
                //TODO: maybe send registry a message saying load balance done
                return;
            }

            //target load for convergence operations
            int targetLoadInNetwork = (int) Math.floor(this.loadMap.values().stream()
                    .mapToInt(Integer::intValue)
                    .average()
                    .orElseThrow());

            int currentAverage = targetLoadInNetwork;
            int currentMaxLoad;
            int currentMinLoad;

            logger.info("Global target load size after balance " + targetLoadInNetwork);
            logger.info("Initial load size in this node " + selfLoad);
            while (selfLoad != targetLoadInNetwork) {
                if (selfLoad < targetLoadInNetwork) {
                    Optional<Map.Entry<String, Integer>> entryWithMaxValueTemp = getMaxLoadEntrySet();
                    Map.Entry<String, Integer> maxEntryTemp = entryWithMaxValueTemp.get();
                    currentMaxLoad = maxEntryTemp.getValue();
                    nodeWithMaxLoad = maxEntryTemp.getKey();

                    currentAverage = (currentMaxLoad + selfLoad) / 2;
                    int difference = Math.abs(currentAverage - selfLoad);
                    selfLoad = selfLoad + difference;
                    //pull tasks; [TODO fix NPE here]
                    if (difference != 0) {
                        TaskList pullTask = new TaskList(difference, nodeIP + ":" + nodePort);
                        this.peerConnections.get(nodeWithMaxLoad).getSenderThread().sendData(pullTask.marshal());

                        //sleep so that the currentTaskList has time to update //or create a latch TODO
                        pullCounter = new CountDownLatch(difference);
                        //logger.info("Awaiting all tasks to be pulled. Latch count "+ pullCounter);
                        pullCounter.await();
                    }

                    //TimeUnit.SECONDS.sleep(10);
                    //logger.info("Updated self load after balance substeps = " + selfLoad);
                    currentMaxLoad = currentMaxLoad - difference;

                    //update own load map with intermediate values
                    this.loadMap.put(nodeWithMaxLoad, currentMaxLoad);
                    this.loadMap.put(nodeIP + ":" + nodePort, selfLoad);
                    //TODO: ALso need to send/receive tasks over the wire
                    //propagate this change to the maxLoad keynode
                    ServerResponse res = new ServerResponse(RequestType.LOAD_UPDATE, StatusCode.SUCCESS, nodeWithMaxLoad + "->" + String.valueOf(currentMaxLoad));
                    //send task to only specific node, but inform all other nodes about the update todo
                    this.peerConnections.forEach((k, v) -> {
                        try {
                            v.getSenderThread().sendData(res.marshal());
                        } catch (InterruptedException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    //this.peerConnections.get(nodeWithMaxLoad).getSenderThread().sendData(res.marshal());
                } else {
                    Optional<Map.Entry<String, Integer>> entryWithMinValueTemp = getMinLoadEntrySet();
                    Map.Entry<String, Integer> minEntryTemp = entryWithMinValueTemp.get();
                    currentMinLoad = minEntryTemp.getValue();
                    nodeWithMinLoad = minEntryTemp.getKey();

                    currentAverage = (currentMinLoad + selfLoad) / 2;
                    int difference = Math.abs(selfLoad - currentAverage);
                    selfLoad = selfLoad - difference;
                    //TODO L fix npe

                    //lets push actual tasks now
                    if (difference != 0) {
                        pushNTasks(difference, nodeWithMinLoad);
                    }
                    //logger.info("Updated self load after balance substeps = " + selfLoad);
                    currentMinLoad = currentMinLoad + difference;
                    //update own load map with intermediate values
                    this.loadMap.put(nodeWithMinLoad, currentMinLoad);
                    this.loadMap.put(nodeIP + ":" + nodePort, selfLoad);
                    //propagate this change to the maxLoad keynode
                    ServerResponse res = new ServerResponse(RequestType.LOAD_UPDATE, StatusCode.SUCCESS, nodeWithMinLoad + "->" + String.valueOf(currentMinLoad));
                    this.peerConnections.forEach((k, v) -> {
                        try {
                            v.getSenderThread().sendData(res.marshal());
                        } catch (InterruptedException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    //this.peerConnections.get(nodeWithMinLoad).getSenderThread().sendData(res.marshal());
                }
            }
            //this node has balanced load. we're done here. broadcast that this
            //node is balanced and should not be factored into load balancing
            //TODO
            logger.info("Final self load after balance completed = " + selfLoad);
            this.loadMap.replace(nodeIP + ":" + nodePort, selfLoad);

            balancedNodesLocalCopy.add(this.nodeIP + ":" + this.nodePort);

            //inform peers about balanced node update as well
            BalancedNodes staticBalancerInfo = new BalancedNodes();
            staticBalancerInfo.setBalancedNodeInfo(this.nodeIP + ":" + this.nodePort + "->" + selfLoad);
            BalancedNodes.addBalancedNode(this.nodeIP + ":" + this.nodePort);

            //create balanced node message payload TODO
            //send balanced node message to all peers
            peerConnections.forEach((k, v) -> {
                try {
                    v.getSenderThread().sendData(staticBalancerInfo.marshal());
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            });
            logger.info("Load balancing done");
            //printCurrentLoadMap();
            if (threadPool != null) {
                logger.info("Cleaning up old thread pool instance");
                threadPool.stop(); //clean up before starting new threadpool instance
            }
            threadPool = new ThreadPool(this.threadCount, 1000, stats.getCurrentTasks().get());
            submitAndWaitUntilTasksComplete();
        }
        else {
            logger.info("Need load balancer token to start");
        }
    }

    private void submitAndWaitUntilTasksComplete() {
        long startTime = System.nanoTime();
        System.out.println("*********COMPLETED TASKS***********");
        System.out.println();
        submitTasks();
        try {
            boolean taskComplete = threadPool.awaitCompletion();
            if (taskComplete) {
                //well one round is complete
                long endTime = System.nanoTime();
                long executionTime = endTime - startTime;
                long numSecs = executionTime/1_000_000_000;
                System.out.println("**********************");
                System.out.println("************METRICS**********");
                System.out.println("All tasks completed in "+ numSecs + " secs");
                //System.out.println(stats.getCurrentTasks().get());
                stats.setCompletedTasks(stats.getCurrentTasks().get());
                if (numSecs != 0) {
                    System.out.println("Task completion rate (per sec) = " + stats.getCurrentTasks().get() / numSecs);
                }
                System.out.println("**********************");
                //inform registry about task completion

                TaskCompleteResponse resp = new TaskCompleteResponse(nodeIP, nodePort, stats);
                this.registryConnection.getSenderThread().sendData(resp.marshal());

                this.loadBalanceOriginTokenReceived = false; //reset token for new round
                stats.reset();
                //System.out.println("Stats at end of round "+ stats.toString());
                //generateNewRoundOfTasks();
                //logger.info("New round of tasks being generated");
            }
            else {
                logger.severe("Some tasks not completed. ERROR");
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<Map.Entry<String, Integer>> getMaxLoadEntrySet () {
        Optional<Map.Entry<String, Integer>> entryWithMaxValue = loadMap.entrySet().stream()
                .filter(entry -> !balancedNodesLocalCopy.contains(entry.getKey()))
                .max(Map.Entry.comparingByValue());
        return entryWithMaxValue;
    }

    private Optional<Map.Entry<String, Integer>> getMinLoadEntrySet () {
        Optional<Map.Entry<String, Integer>> entryWithMinValue = loadMap.entrySet().stream()
                .filter(entry -> !balancedNodesLocalCopy.contains(entry.getKey()))
                .min(Map.Entry.comparingByValue());
        return entryWithMinValue;
    }



    private void submitTasks () {
        //TODO : before submitting, we perform load balancing maneuvers.
        //TODO: maybe use a task list.size() because the task list content will change after load balancing
        for (int i = 0; i < currentTasks.size(); i++) {
            int taskNo = i;

            //submit task
            getThreadPool().submit(() -> {
                //dummy example
                //String msg = Thread.currentThread().getName() + " : Task " + taskNo;
                //System.out.println(msg);
                //System.out.println(Thread.currentThread().getName() + " : Task " + taskNo);
                Miner miner = Miner.getInstance();

                Task task = this.currentTasks.get(taskNo);
                miner.mine(task);
                System.out.println(task.toString());

                //stats.incrementCompletedCount();
            });
        }
    }

    public void registerNode (ComputeNode node, Socket socketToRegistry) {
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

    private void userInput (ComputeNode node) {
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
                    System.exit(0);
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
                            TaskList taskList = new TaskList(tasklist, TaskOperations.PUSH);
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
                            + stats.getPulledTasks() + "\t pushed : "+ stats.getPushedTasks() + "\t completed : "+stats.getCompletedTasks());

                }
                else if (userInput.equals(UserCommands.PRINT_NUM_COMPLETED_TASKS.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_NUM_COMPLETED_TASKS.getCmdId()))) {
                    getTotalTasks();
                    System.out.println("Number of completed tasks : " + stats.getCompletedTasks());
                }
                else if (userInput.equals(UserCommands.PRINT_NUM_CURRENT_TASKS.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_NUM_CURRENT_TASKS.getCmdId()))) {
                    System.out.println("Number of current tasks : " + stats.getCurrentTasks());
                }
                else if (userInput.equals(UserCommands.COUNTDOWN_LATCH_DEBUG.getCmd()) || userInput.equals(String.valueOf(UserCommands.COUNTDOWN_LATCH_DEBUG.getCmdId()))) {
                    System.out.println("Count down latch : " + threadPool.getTaskCompletionLatch().getCount());
                }
                else if (userInput.equals(UserCommands.MANUAL_LOAD_BALANCE.getCmd()) || userInput.equals(String.valueOf(UserCommands.MANUAL_LOAD_BALANCE.getCmdId()))) {
                    Thread loadBalanceThread = new Thread(() -> {
                        try {
                            loadBalanceOriginTokenReceived = true;
                            node.loadBalanceInitThreadPoolAndSubmitTasks();
                        } catch (IOException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    loadBalanceThread.start();
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
        if (res.getRequestType() != null && res.getRequestType().equals(RequestType.REGISTER)) {
            logger.info(res.getAdditionalInfo());
        }
        else if (res.getRequestType() != null && res.getRequestType().equals(RequestType.DEREGISTER)) {
            try {
                logger.info(res.getAdditionalInfo());
                this.registryConnection.closeConnection();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        else if (res.getRequestType() != null && res.getRequestType().equals(RequestType.LOAD_UPDATE)) {
            logger.info("****** New comms from Peer server ******");
            //TODO TOPLAN: Might need to implement the token based approach. Say node x modifies the load count
            //for this node, but if this node already did its compute with the old value, then we have a problem
            //System.out.println("Old load map");
            //printCurrentLoadMap();
            //System.out.println("[LOAD BALANCING] Updated load received from peer node");
            String [] updatedLoadInfo = res.getAdditionalInfo().split("->");
            this.loadMap.replace(updatedLoadInfo[0], Integer.parseInt(updatedLoadInfo[1]));
            //printCurrentLoadMap();
        }
        else if (res.getRequestType() != null && res.getRequestType().equals(RequestType.MESSAGE_ROUND_INITIATE)) {
            logger.info("****** New Message round instruction received from  server ******");
            //start the load balance and the task rounds after lb
            if (res.getAdditionalInfo().contains("TOKEN")) {
                loadBalanceOriginTokenReceived = true;
            }
            startNewRoundOfTasks();
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

    //this is basically our init
    public synchronized void initThreadCount(TopologyInfo info) {
        logger.info("Received topology info "+ info);

        String[] elements = info.getNodeRingInfo().split("->");

        //first setup threadpool
        if (threadPool != null) {
            logger.info("Cleaning up old thread pool instance");
            threadPool.stop(); //clean up before starting new threadpool instance
        }
        stats.reset();
        neighbors.clear();
        this.threadCount = info.getNumThreads();

        // Add each element to the linked list
        neighbors.clear();
        neighbors.addAll(Arrays.asList(elements));
        //preserve the original ring composition here.
        originalRing.clear();
        originalRing.addAll(Arrays.asList(elements));
        String currentNodeDesc = nodeIP+":"+nodePort;
        //remove self from list of neighbors
        neighbors.removeIf(currentNodeDesc::equals);
        //System.out.println("The list of neighbors : "+ neighbors);

        //initial round of tasks
        generateNewRoundOfTasks();
    }

    private synchronized void startNewRoundOfTasks () {
        generateNewRoundOfTasks();
        try {
             //wait for total loads across the network to be collected before load balancing
            taskGeneratedCounter.await();
            //new thread TODO
            new Thread(() -> {
                try {
                    loadBalanceInitThreadPoolAndSubmitTasks();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateNewRoundOfTasks () {
        this.loadMap.clear();
        this.balancedNodesLocalCopy.clear();
        this.currentTasks.clear();

        generateTasks();
        try {
            taskGeneratedCounter.await();
            logger.info("All tasks generated");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    //send task info to peer that requests it.
    public synchronized void sendTaskInfo(ClientConnection peerConnection, TCPConnection connection) {
        logger.info("Received task load request from source node");
        String peer = peerConnection.getIpAddress()+":"+peerConnection.getPort();

        TCPConnection peerConn = this.peerConnections.get(peer);

        LoadSummaryResponse load = new LoadSummaryResponse(nodeIP, nodePort, stats);
        try {
            peerConn.getSenderThread().sendData(load.marshal());
            //peerConn.startConnection();
            logger.info("Sent stats to "+ peer);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    //record task load info that peer sent as result of the sendTaskInfo function
    public synchronized void handleLoadSummaryResponse(LoadSummaryResponse traffic) {
        //loadMap.clear();
        int oldSize = loadMap.size();
        if (loadMap.containsKey(traffic.getNodeIP() + ":" + traffic.getNodePort())) {
            loadMap.replace(traffic.getNodeIP() + ":" + traffic.getNodePort(), traffic.getCurrentTasks());
        } else {
            loadMap.put(traffic.getNodeIP() + ":" + traffic.getNodePort(), traffic.getCurrentTasks());
        }
        //System.out.println("Received load summary from neighbor "+ traffic.getNodeIP() + ":" + traffic.getNodePort());
        loadStatisticsMessageAroundRingCounter.countDown();

        //logger.info("New latch size after handling load "+ loadStatisticsMessageAroundRingCounter.getCount());
//        System.out.println("New load statistics latch val " + loadStatisticsMessageAroundRingCounter.getCount());
//        printCurrentLoadMap();
//        if (loadStatisticsMessageAroundRingCounter.getCount() == 0 && this.loadBalanceOriginTokenReceived) {
//            try {
//                loadBalanceInitThreadPoolAndSubmitTasks();
//            } catch (IOException | InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }

    }

    public synchronized void pushTask(Task task, TCPConnection targetNode) {
        try {
            targetNode.getSenderThread().sendData(task.marshal());
            stats.incrementPushCount();
            stats.decrementCurrentTasks();
            currentTasks.removeIf(el -> el.getPayload() == task.getPayload());
            //System.out.println("Pushed "+ task.toString());
            //printCurrentTasks("After single push");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void pushTasks(TaskList taskList, TCPConnection targetNode, String targetNodeInfo) {
        try {
            if (taskList.getTasks().length == 0) {
                //System.out.println("Nothing to push ");
                return;
            }
            targetNode.getSenderThread().sendData(taskList.marshal());
            for (int i = 0; i < taskList.getTasks().length; i++) {
                Task pushedTask = taskList.getTasks()[i];
                stats.incrementPushCount();
                stats.decrementCurrentTasks();
                currentTasks.removeIf(el -> el.getPayload() == pushedTask.getPayload());
            }
           // printCurrentTasks("(After bulk push)");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    //remember this is happening in receiver node. Not sender node. So the operations enum
    //reflect the original command in the sender. For pull in receiver, the sender will
    //have a push operation, which is what the payload will have
    //don't sync this. else deadlock arises because the first thread will need the pull counter latch to update
    //to proceed, but in order to update the latch this method needs to be executed. However, the first
    //thread hasn't released the lock on this, but is waiting for this method to return, which will not happen
    //DEADLOCK!!!!!!! If sync'ed
    //UPDATE: well I was wrong. No deadlock becuase of a sync. Sync this!!!
    public synchronized void handleTaskMigrations(TaskList taskList) {
        //System.out.println("Handling task migration");
//        Thread migrationThread = new Thread(() -> {
            if (taskList.getOperations().equals(TaskOperations.PUSH)) {
                //System.out.println("Pulled tasks # " + taskList.getTasks().length);
                for (Task task : taskList.getTasks()) {
                    currentTasks.add(task);
                    stats.incrementCurrentTasks();
                    stats.incrementPullCount();
                    if (pullCounter != null) {
                        pullCounter.countDown();
                        //logger.info("PullCounter latch val # "+ pullCounter.getCount());
                    }
                }
            }
            //pull
            else {
                //System.out.println("Pushing tasks # " + taskList.getTasks().length);
                pushNTasks(taskList.getRequestedTaskNum(), taskList.getRequestingNode());

            }

//        migrationThread.start();
        //printCurrentTasks("(After bulk pull)");
    }

    public synchronized void pullSingleTask(Task task) {
        //System.out.println("Pulled task "+ task.toString());
        currentTasks.add(task);
        stats.incrementCurrentTasks();
        //printCurrentTasks("(After Single pull)");
        stats.incrementPullCount();
    }

    private void printCurrentTasks(String appendum) {
        //System.out.println("Current tasks list elements : " + appendum);
        for (Task tsk: currentTasks) {
            System.out.println(tsk.toString() + " origin node "+tsk.getOriginNode());
        }
        //System.out.println("Number of current tasks "+ currentTasks.size());
        //System.out.println("Number of current tasks as per statsEngine "+ stats.getCurrentTasks());
    }

    public synchronized void copyStaticBalancedNodesInfoToLocal(BalancedNodes balanced) {
        this.balancedNodesLocalCopy.addAll(Arrays.asList(BalancedNodes.getBalancedNodes()));
        //System.out.println("Updated balanced nodes list in this node.");
        logger.info("Origin node balanced. Starting tasks in the destination nodes");

        String balanceNodeId = balanced.getBalancedNodeInfo().split("->")[0];
        int balancedNodeLoad = Integer.parseInt(balanced.getBalancedNodeInfo().split("->")[1]);
        this.loadMap.replace(balanceNodeId, balancedNodeLoad);

        //well as per experiments, looks like the network is balanced after lb operation
        // at the self node , so lets run the tasks in the target nodes.
        //not the node with balance token
        if (threadPool != null) {
            logger.info("Cleaning up old thread pool instance");
            threadPool.stop(); //clean up before starting new threadpool instance
        }
        threadPool = new ThreadPool(this.threadCount, 1000, stats.getCurrentTasks().get());
        submitAndWaitUntilTasksComplete();

        //printBalancedNodesInfo();
        //printCurrentLoadMap();
    }

    private void printBalancedNodesInfo() {
        System.out.println("Currently balanced nodes local info: ");
        for (String blNode: balancedNodesLocalCopy) {
            System.out.println(blNode);
        }
        System.out.println("Currently balanced nodes static info  ");
        for (String blNode: BalancedNodes.getBalancedNodes()) {
            System.out.println(blNode);
        }
    }

    private void printCurrentLoadMap() {
        System.out.println("Current load map in this node ");
        for (Map.Entry<String, Integer> entry: loadMap.entrySet()) {
            String self = (nodeIP+":"+nodePort).equals(entry.getKey()) ? "self" : "";
            System.out.println("Node: "+ entry.getKey()+ " | Current Load : "+ entry.getValue() + self);
        }
    }

    private synchronized void pushNTasks (int n, String targetNode) {
        //oscillation damping logic
        /*
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
        */
        Task [] tasklist = new Task[n];
        for (int idx = 0; idx < n; idx++) {
            tasklist[idx] = this.currentTasks.get(idx);
            //this.currentTasks.get(idx).setOriginNode(nodeIP+":"+nodePort);
        }
        TCPConnection targetConn = this.peerConnections.get(targetNode);
        try {
            TaskList taskList = new TaskList(tasklist, TaskOperations.PUSH);
            pushTasks(taskList, targetConn, targetNode); //v=connection

        } catch (Exception e) {
            logger.severe("Error pushing load");
            throw new RuntimeException(e);
        }
    }
}
