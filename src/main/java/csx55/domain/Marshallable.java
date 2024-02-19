package csx55.domain;

import java.io.IOException;

public interface Marshallable<T> {
    T unmarshal(byte[] bytes) throws IOException;
    byte[] marshal() throws IOException;
}
