package csx55.domain;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TestPayload implements Serializable {
    //private static final long serialversionUID = 1L;
    private final int type = Protocol.TEST_PAYLOAD;
    private String testString;

    private final MessageDigest sha256;

    private Integer testPort;

    public TestPayload(String testString, Integer testPort) {
        this.testString = testString;
        this.testPort = testPort;
        try {
            sha256 = MessageDigest.getInstance("SHA3-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTestString() {
        return testString;
    }

    public void setTestString(String testString) {
        this.testString = testString;
    }

    public Integer getTestPort() {
        return testPort;
    }

    public void setTestPort(Integer testPort) {
        this.testPort = testPort;
    }

    @Override
    public String toString() {
        return "TestPayload{" +
                "testString='" + testString + '\'' +
                ", sha256=" + sha256 +
                ", testPort=" + testPort +
                '}';
    }
}
