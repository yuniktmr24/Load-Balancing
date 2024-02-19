package csx55.domain;
import csx55.hashing.Task;

import java.io.*;

public class TaskList implements Marshallable {
    private final int type = Protocol.TASK_LIST;
    private Task [] tasks;

    public TaskList(Task[] tasks) {
        this.tasks = tasks;
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

        TaskList taskList = new TaskList(tasks);

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

        dout.flush();

        byte[] marshalledBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        dout.close();

        return marshalledBytes;
    }

    public Task[] getTasks() {
        return tasks;
    }
}
