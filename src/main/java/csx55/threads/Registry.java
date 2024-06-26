package csx55.threads;

import csx55.domain.*;
import csx55.overlay.OverlayCreator;
import csx55.overlay.OverlayNode;
import csx55.transport.TCPConnection;
import csx55.transport.TCPServerThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Registry implements Node{
    private static final Logger logger = Logger.getLogger(Registry.class.getName());
    private static final Registry instance = new Registry();

    public static Registry getInstance() {
        return instance;
    }

    private static final Set<ClientConnection> registeredNodes = new HashSet<>();
    //copy of recently deregistered nodes, not registered currently
    private static final List<ClientConnection> deregisteredNodes = new ArrayList<>();

    private static final Set <OverlayNode> overlayNodes = new HashSet<>();

    private static TCPServerThread registryServerThread;

    private CountDownLatch singleRoundTaskCompleteCounter;

    private Map <String, CollatedTrafficStats> collatedStatsMap = new ConcurrentHashMap<>();

    public static void main (String[] args) {
        int registryPort = args.length >= 1 ? Integer.parseInt(args[0]) : 12331;
        try (ServerSocket serverSocket = new ServerSocket(registryPort)) {
            System.out.println("Server listening on port " + registryPort + "...");
            Registry registry = Registry.getInstance();
            (new Thread(registryServerThread = new TCPServerThread(registry, serverSocket))).start();
            registry.startUserInputThread(serverSocket);
        } catch (IOException e) {
            logger.severe("Error in the serverSocket communication channel" + e);
        }
    }

    private void startUserInputThread(ServerSocket socket) {
        BufferedReader userInputReader = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.println("***************************************");
                System.out.println("[REGISTRY] Enter your Registry command");
                System.out.println(UserCommands.userRegistryCommandsToString());
                String userInput = userInputReader.readLine();
                boolean containsSpace = false, validOverlayCreatorCmd = false, validStartMessagingCmd = false;
                if (userInput.contains(" ")) {
                    containsSpace = true;
                    if (userInput.startsWith(UserCommands.SETUP_OVERLAY_NUMBER_OF_CONNECTIONS.getCmd()) ||
                            userInput.toUpperCase().contains("SETUP-OVERLAY") ||
                            userInput.startsWith(String.valueOf(UserCommands.SETUP_OVERLAY_NUMBER_OF_CONNECTIONS.getCmdId()))) {
                        validOverlayCreatorCmd = true;
                    }
                    else if (userInput.startsWith(UserCommands.START_NUMBER_OF_ROUNDS.getCmd()) ||
                            userInput.toUpperCase().contains("START") ||
                            userInput.startsWith(String.valueOf(UserCommands.START_NUMBER_OF_ROUNDS.getCmdId()))) {
                        validStartMessagingCmd = true;
                    }
                }
                if (userInput.equals(UserCommands.EXIT.getCmd()) || userInput.equals(String.valueOf(UserCommands.EXIT.getCmdId()))) {
                    //exit everything
                    socket.close();
                    throw new RuntimeException("Server terminated");
                }
                else if (userInput.equals(UserCommands.LIST_MESSAGING_NODES.getCmd())
                        || userInput.equals(String.valueOf(UserCommands.LIST_MESSAGING_NODES.getCmdId()))) {
                    getRegisteredNodes().forEach(registeredNode -> {
                        System.out.println(registeredNode.toString());
                    });
                }
                else if (containsSpace && validOverlayCreatorCmd) {
                    System.out.println("[REGISTRY ] Setting up overlay");
                    new OverlayCreator().constructRing(overlayNodes);
                    //create a simplified string representation of the overlayNodes struct
                    List <OverlayNode> overlayNodeList = new ArrayList<>(overlayNodes);
                    String [] nodeRing = new String[overlayNodeList.size()];
                    nodeRing[0] = overlayNodeList.get(0).getDescriptor();
                    for (int idx = 0; idx < overlayNodeList.size() - 1; idx++) {
                        //node.getConnection().getSenderThread().sendData(overlayNodes.);
                        OverlayNode node = overlayNodeList.get(idx);
                        nodeRing[idx + 1] = node.getNeighbors().get(0).getDescriptor();
                    }
                    int numThreads = Integer.parseInt(userInput.split(" ")[1]);
                    TopologyInfo topologyInfo = new TopologyInfo(String.join("->", nodeRing), numThreads);
                    System.out.println("Num of threads specified by user "+ numThreads);
                    //send the NodeRing payload over the wire
                    for (OverlayNode node: overlayNodes) {
                        node.getConnection().getSenderThread().sendData(topologyInfo.marshal());
                    }
                    logger.info("Ring created with # of nodes : "+ overlayNodes.size());
                }

                else if (containsSpace && validStartMessagingCmd) {
                    singleRoundTaskCompleteCounter = new CountDownLatch(overlayNodes.size());
                    //collatedTrafficStats = new CollatedTrafficStats();
                    int rounds = Integer.parseInt(userInput.split(" ")[1]);
                    //System.out.println("Sending "+ rounds + " round of messages");
                    //System.out.println("****************************");
                    //TODO loop this
                    for (int i = 0; i < rounds; i++) {
                        ServerResponse res = new ServerResponse(RequestType.MESSAGE_ROUND_INITIATE, StatusCode.SUCCESS, "TOKEN_BALANCE_RECEIVER");
                        //only send to node with token (source Node for load balancing)
                        Random randomNodeForBalanceStart = new Random();
                        List<OverlayNode> overlayNodeList = new ArrayList<>(overlayNodes);
                        int sourceNode = randomNodeForBalanceStart.nextInt(overlayNodes.size() - 1);
                        OverlayNode tokenBearer = overlayNodeList.get(sourceNode);
                        //System.out.println("Token bearer node " + tokenBearer.getDescriptor());
                        tokenBearer.getConnection().getSenderThread().sendData(res.marshal());
                        //also the other needs to geneerate rando, number of tasks
                        int idx = 0;
                        for (OverlayNode node: overlayNodeList) {
                            ServerResponse res2 = new ServerResponse(RequestType.MESSAGE_ROUND_INITIATE, StatusCode.SUCCESS, "");
                            if (idx != sourceNode) {
                                node.getConnection().getSenderThread().sendData(res2.marshal());
                            }
                            idx++;
                        }
                        singleRoundTaskCompleteCounter.await();
                        //System.out.println("All tasks completed for given round");
                        //System.out.println("****************************");
                        //reinit the latch for next round of msg
                        singleRoundTaskCompleteCounter = new CountDownLatch(overlayNodes.size());
                    }
                    //get global total
                    int totalTasks = 0;
                    int totalPushed = 0;
                    int totalPulled = 0;
                    int totalCompleted = 0;
                    for (CollatedTrafficStats collated: collatedStatsMap.values()) {
                        totalTasks += collated.getGeneratedTotal();
                        totalPushed += collated.getPushedTotal();
                        totalPulled += collated.getPulledTotal();
                        totalCompleted += collated.getCompletedTotal();
                    }
                   // System.out.println("****************************");
                   // System.out.println("Final stats after "+ rounds + " rounds");
                    System.out.printf(String.format( "%1$-25s %2$-25s %3$-25s %4$-25s %5$-25s %6$-25s",
                            "", "Number of generated tasks", "Number of pulled tasks", "Number of pushed tasks"
                                    ,"Number of completed tasks", "Percent of total tasks performed"));
                    System.out.println("");
                    for (CollatedTrafficStats collated: collatedStatsMap.values()) {
                        collated.setGlobalTotal(totalTasks);
                        System.out.println(collated.toString());
                    }
                    System.out.printf(String.format( "%1$-25s %2$-25s %3$-25s %4$-25s %5$-25s %6$-25s",
                            "Total",
                            Integer.toString( totalTasks ),
                            Integer.toString( totalPulled ), Integer.toString( totalPushed ),
                            Integer.toString( totalCompleted ), Double.toString(100)));
                    System.out.println();
                    //System.out.println("****************************");
                }
            }
        } catch (Exception e) {
            logger.severe("Error encountered while running user command "+ e);
            try {
                TimeUnit.SECONDS.sleep( 4 );
                //cleanup();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public synchronized void recordCompletedTaskFromMessagingNode(TaskCompleteResponse complete) {
        //System.out.println("Received completed tasks stats from " + complete.getNodeIP()+":"+complete.getNodePort());
        //System.out.println(complete.toString());

        String nodeDescriptor = complete.getNodeIP()+":"+complete.getNodePort();
        if (collatedStatsMap.containsKey(nodeDescriptor)) {
            //add to old stats
            CollatedTrafficStats stats = collatedStatsMap.get(nodeDescriptor);
            stats.addStats(complete);
        }
        else{
            //add stats for a node for the first time
            CollatedTrafficStats stats = new CollatedTrafficStats(complete);
            collatedStatsMap.put(nodeDescriptor, stats);
        }
        if (singleRoundTaskCompleteCounter != null) {
            singleRoundTaskCompleteCounter.countDown();
        }
        //decrement the number of messaging nodes yet to return result for given round
    }

    public synchronized void handleClient(Socket clientSocket, ClientConnection conn, TCPConnection connection) {
        try {
            String additionalProcessingInfo = "";
            boolean success = conn.getRequestType().equals(RequestType.REGISTER) ? Registry.registerNode(clientSocket, conn) : Registry.deregisterNode(clientSocket, conn);
            boolean modifiedOverlay = false;
            if (conn.getRequestType().equals(RequestType.REGISTER)) {
                additionalProcessingInfo = "Registration Request ";
                modifiedOverlay = overlayNodes.add(new OverlayNode(connection, conn.getIpAddress() + ":" + conn.getPort()));
            } else if (conn.getRequestType().equals(RequestType.DEREGISTER)) {
                additionalProcessingInfo = "De-registration.";
                overlayNodes.removeIf(i -> {
                    if (i.getConnection().getSocket() != null && connection.getSocket() != null) {
                        return i.getConnection().getSocket().getPort() == connection.getSocket().getPort();
                    }
                    return false;
                });
            }
            additionalProcessingInfo += (success ? "Successful. " :"Failed") +
                    String.format("The number of messaging nodes currently constituting the overlay is %d ", registeredNodes.size());
            ServerResponse serverResponse = new ServerResponse(conn.getRequestType(), success ? StatusCode.SUCCESS : StatusCode.FAILURE, additionalProcessingInfo);

            /*
            for (OverlayNode node: overlayNodes) {
                System.out.println(node.toString());
            }
            */

            if (modifiedOverlay) {
                connection.getSenderThread().sendData(serverResponse.marshal());
            }
        } catch (Exception e) {
            logger.severe("Error handling client in registry "+ e);
        }
    }

    private static boolean validateNode (Socket clientSocket, ClientConnection conn) {
        if (!((InetSocketAddress)clientSocket.getRemoteSocketAddress()).getAddress().getHostAddress().equals(conn.getIpAddress())) {
            logger.severe("Invalid Node Registration : Payloads must match : ClientSocket Addr : "+ ((InetSocketAddress)clientSocket.getRemoteSocketAddress()).getAddress().getHostAddress() + "VS Connection payload addr: "+ conn.getIpAddress());
            return false;
        }
//        else if (registeredNodes.stream().anyMatch(registered -> registered.getIpAddress()
//                .equals(conn.getIpAddress())
//                && Objects.equals(registered.getPort(), conn.getPort())) && conn.getRequestType().equals(RequestType.REGISTER)) {
//            logger.severe("Node already registered: "+ conn.getIpAddress() + ":" + conn.getPort());
//            return false;
//        }
        return true;
    }

    public static boolean registerNode(Socket clientSocket, ClientConnection conn) {
        try {
            logger.info("Registering Message node...");
            if (!validateNode(clientSocket, conn)) {
                throw new RuntimeException("Invalid messaging node registration. Exiting");
            }
            //todo add more logic (?) //validate first. check duplicate registrations etc
            deregisteredNodes.removeIf(node -> node.getIpAddress().equals(conn.getIpAddress()) && Objects.equals(node.getPort(), conn.getPort()));
            registeredNodes.add(conn);
            //System.out.println("Total number of registered nodes is " + registeredNodes.size());
            return true;
        }
        catch (Exception ex) {
            //log it
            logger.severe("Error registering node "+ ex);
            return false;
        }
    }

    public static boolean deregisterNode(Socket clientSocket, ClientConnection conn) {
        try {
            logger.info("[REGISTRY] De-registering message node....");
            if (!validateNode(clientSocket, conn)) {
                throw new RuntimeException("Invalid messaging node de-registration. Exiting");
            }
            boolean success = registeredNodes.removeIf(node -> node.getIpAddress().equals(conn.getIpAddress()) && Objects.equals(node.getPort(), conn.getPort()));
            deregisteredNodes.add(conn);
            logger.info("De-registration" + (success ? " successful" : "failed"));
            return success;
        }
        catch (Exception ex) {
            //log it
            logger.severe("De-registration failed "+ ex.getMessage());
            return false;
        }
    }

    public static OverlayNode[] getRegisteredOverlayNodes () {
        return overlayNodes.toArray(new OverlayNode[0]);
    }


    public static Set<ClientConnection> getRegisteredNodes() {
        return registeredNodes;
    }

    public static List<ClientConnection> getDeregisteredNodes() {
        return deregisteredNodes;
    }

}
