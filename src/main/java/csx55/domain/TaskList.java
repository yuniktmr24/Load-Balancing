package csx55.domain;
import csx55.hashing.Task;

import java.io.*;

public class TaskList implements Marshallable {
    private final int type = Protocol.TASK_LIST;
    private Task [] tasks;

    private TaskOperations operations;

    private int requestedTaskNum = 0 ; //during pull request

    private String requestingNode = ""; //during pull request

    public TaskList(Task[] tasks, TaskOperations oper) {

        this.tasks = tasks;
        this.operations = oper;
    }

    public TaskList(int numPull, String pullTarget) {
        this.tasks = new Task[0];
        this.operations = TaskOperations.PULL;
        this.requestedTaskNum = numPull;
        this.requestingNode = pullTarget;
    }

    public TaskList() {

    }

    public TaskList unmarshal (byte [] marshalledBytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));

        din.readInt(); // Read and discard the type

        int numTasks = din.readInt(); // Read the number of tasks
        Task[] tasks = new Task[numTasks];

        for (int i = 0; i < numTasks; i++) {
            int taskLength = din.readInt(); // Read length of task bytes
            byte[] taskBytes = new byte[taskLength];
            din.readFully(taskBytes); // Read task bytes

            Task task = new Task();
            tasks[i] = task.unmarshal(taskBytes); // Unmarshal each task
        }

        TaskOperations oper = TaskOperations.values()[din.readInt()];

        int requestedNumTasks = din.readInt();

        int requestingNodeLength = din.readInt();
        byte[] reqNodeBytes = new byte[requestingNodeLength];
        din.readFully(reqNodeBytes);
        String reqNode = new String(reqNodeBytes);

        TaskList taskList = new TaskList(tasks, oper);
        taskList.setRequestedTaskNum(requestedNumTasks);
        taskList.setRequestingNode(reqNode);

        byteArrayInputStream.close();
        din.close();

        return taskList;
    }

    public byte [] marshal () throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(byteArrayOutputStream));

        dout.writeInt(this.type);
        dout.writeInt(this.tasks.length);

        for (Task task : this.tasks) {
            byte[] taskBytes = task.marshal(); // Marshal each task
            dout.writeInt(taskBytes.length); // Write length of task bytes
            dout.write(taskBytes); // Write task bytes
        }
        dout.writeInt(operations.ordinal());
        dout.writeInt(this.requestedTaskNum);

        byte[] requestingNodeBytes = this.requestingNode.getBytes();
        int reqNodeLength = requestingNodeBytes.length;
        dout.writeInt(reqNodeLength);
        dout.write(requestingNodeBytes);

        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }

    public Task[] getTasks() {
        return tasks;
    }

    public TaskOperations getOperations() {
        return operations;
    }

    public void setOperations(TaskOperations operations) {
        this.operations = operations;
    }

    public int getRequestedTaskNum() {
        return requestedTaskNum;
    }

    public void setRequestedTaskNum(int requestedTaskNum) {
        this.requestedTaskNum = requestedTaskNum;
    }

    public String getRequestingNode() {
        return requestingNode;
    }

    public void setRequestingNode(String requestingNode) {
        this.requestingNode = requestingNode;
    }
}
