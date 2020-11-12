package com.sirius.tomcat.ext;

import org.apache.tomcat.util.net.ApplicationBufferHandler;
import org.apache.tomcat.util.net.SocketBufferHandler;
import org.apache.tomcat.util.net.UDPSocketWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class EmptyUPDChannel extends UDPChannel {

    public EmptyUPDChannel(SocketBufferHandler bufHandler) {
        super(bufHandler);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void reset(SocketChannel channel, UDPSocketWrapper wrapper) {

    }

    @Override
    public void free() {
    }

    @Override
    public void setAppReadBufHandler(ApplicationBufferHandler handler) {
    }

    @Override
    public int read(ByteBuffer dst) {
        return -1;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
        return -1L;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        checkInterruptStatus();
        return -1;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
        return -1L;
    }

    @Override
    public String toString() {
        return "Closed UDPChannel";
    }

}
