package csx55.domain;

public interface Protocol {
    final int CLIENT_CONNECTION = 0;

    final int SERVER_RESPONSE = 1;

    final int TOPOLOGY_INFO = 2;

    final int LOAD_SUMMARY = 4;

    final int TEST_PAYLOAD = 5;

    final int TASK = 6;

    final int TASK_LIST = 7;
    int BALANCED_NODES = 8;
}
