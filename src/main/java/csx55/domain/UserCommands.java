package csx55.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public enum UserCommands {
    LIST_MESSAGING_NODES("list-messaging-nodes", 1, Collections.singletonList(NodeType.REGISTRY)),
    SETUP_OVERLAY_NUMBER_OF_CONNECTIONS("setup-overlay number-of-connections", 3, Collections.singletonList(NodeType.REGISTRY)),
    START_NUMBER_OF_ROUNDS("start number-of-rounds", 5, Collections.singletonList(NodeType.REGISTRY)),

    EXIT_OVERLAY("exit-overlay", 7, Collections.singletonList(NodeType.MESSAGE_NODE)),

    PRINT_NEIGHBOR("print-neighbor", 8, Collections.singletonList(NodeType.MESSAGE_NODE)),
    MESSAGE_NEIGHBOR("message-neighbor", 9, Collections.singletonList(NodeType.MESSAGE_NODE)),

    PRINT_LOADS_ACROSS_NETWORK("print-loads-across-network", 10, Collections.singletonList(NodeType.MESSAGE_NODE)),

    PRINT_NUM_COMPLETED_TASKS("print-num-completed-tasks", 11, Collections.singletonList(NodeType.MESSAGE_NODE)),

    SEND_TEST_WIRE("send-wire (DEBUG ONLY)", 12, Collections.singletonList(NodeType.MESSAGE_NODE)),

    SEND_TEST_WIRE_BULK("send-wire-bulk (DEBUG ONLY)", 13, Collections.singletonList(NodeType.MESSAGE_NODE)),

    MANUAL_LOAD_BALANCE("manual-load-balance (DEBUG ONLY)", 14, Collections.singletonList(NodeType.MESSAGE_NODE)),

    PRINT_NUM_CURRENT_TASKS("print-num-current-tasks", 15, Collections.singletonList(NodeType.MESSAGE_NODE)),

    EXIT("exit", -1, Arrays.asList(NodeType.REGISTRY, NodeType.MESSAGE_NODE));
    private final String cmd;
    private final int cmdId;
    private final List <NodeType> nodeType;
    private UserCommands(String cmd, int cmdId, List <NodeType> nodeType) {
        this.cmd = cmd;
        this.cmdId = cmdId;
        this.nodeType = nodeType;
    }

    public String getCmd() {
        return cmd;
    }

    public int getCmdId() {
        return cmdId;
    }

    public List<NodeType> getNodeType() {
        return nodeType;
    }

    public static List<String> getUserRegistryCommands() {
        List <String> cmdGuide = new ArrayList<>();
        for (UserCommands cmd: Arrays.stream(UserCommands.values()).filter(i -> i.getNodeType().contains(NodeType.REGISTRY)).collect(Collectors.toList())){
            cmdGuide.add("Command : " + cmd.getCmd() +" "+ " ID: "+ cmd.getCmdId());
        }
        return cmdGuide;
    }

    public static String userRegistryCommandsToString() {
        List<String> cmdList = getUserRegistryCommands();
        return String.join("\n", cmdList);
    }

    public static List<String> getMessageNodeCommands() {
        List <String> cmdGuide = new ArrayList<>();
        for (UserCommands cmd: Arrays.stream(UserCommands.values()).filter(i -> i.getNodeType().contains(NodeType.MESSAGE_NODE)).collect(Collectors.toList())){
            cmdGuide.add("Command : " + cmd.getCmd() +" "+ " ID: "+ cmd.getCmdId());
        }
        return cmdGuide;
    }

    public static String messageNodeCommandsToString() {
        List<String> cmdList = getMessageNodeCommands();
        return String.join("\n", cmdList);
    }



}
