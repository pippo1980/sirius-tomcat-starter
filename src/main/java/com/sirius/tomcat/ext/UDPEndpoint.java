package com.sirius.tomcat.ext;

import lombok.Getter;
import lombok.Setter;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.collections.SynchronizedStack;
import org.apache.tomcat.util.net.AbstractJsseEndpoint;
import org.apache.tomcat.util.net.NioEndpoint;
import org.apache.tomcat.util.net.SocketEvent;
import org.apache.tomcat.util.net.SocketProcessorBase;
import org.apache.tomcat.util.net.SocketWrapperBase;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.NetworkChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class UDPEndpoint extends AbstractJsseEndpoint<UDPChannel, DatagramChannel> {

    private static final Log log = LogFactory.getLog(NioEndpoint.class);


    private volatile DatagramChannel channel = null;
    private volatile CountDownLatch stopLatch = null;
    private long selectorTimeout = 1000;
    private SynchronizedStack<UDPPollerEvent> eventCache = null;
    private UDPEndpointPoller poller = null;

    @Override
    public void bind() throws Exception {
        DatagramChannel channel = DatagramChannel.open();
        channel.configureBlocking(false);
        channel.bind(new InetSocketAddress(this.getAddress(), this.getPortWithOffset()));

        DatagramSocket socket = channel.socket();
        socket.setReceiveBufferSize(1024 * 1024 * 64);
        socket.setSendBufferSize(1024 * 1024 * 64);

        this.channel = channel;
        this.stopLatch = new CountDownLatch(1);
    }

    @Override
    public void startInternal() throws Exception {
        if (!running) {
            running = true;
            paused = false;

            if (socketProperties.getProcessorCache() != 0) {
                processorCache = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE, socketProperties.getProcessorCache());
            }

            if (socketProperties.getEventCache() != 0) {
                eventCache = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE, socketProperties.getEventCache());
            }

            // Create worker collection
            if (getExecutor() == null) {
                createExecutor();
            }

            // Start poller thread
            poller = new UDPEndpointPoller(this);
            Thread pollerThread = new Thread(poller, getName() + "-ClientPoller");
            pollerThread.setPriority(threadPriority);
            pollerThread.setDaemon(true);
            pollerThread.start();
        }
    }

    @Override
    public void stopInternal() {

        if (!paused) {
            pause();
        }

        if (running) {
            running = false;
            acceptor.stop();
            if (poller != null) {
                poller.destroy();
                poller = null;
            }

            try {
                if (!getStopLatch().await(selectorTimeout + 100, TimeUnit.MILLISECONDS)) {
                    log.warn(sm.getString("endpoint.nio.stopLatchAwaitFail"));
                }
            } catch (InterruptedException e) {
                log.warn(sm.getString("endpoint.nio.stopLatchAwaitInterrupted"), e);
            }

            shutdownExecutor();

            if (eventCache != null) {
                eventCache.clear();
                eventCache = null;
            }

            if (processorCache != null) {
                processorCache.clear();
                processorCache = null;
            }
        }


    }

    @Override
    protected Log getLog() {
        return log;
    }

    @Override
    protected NetworkChannel getServerSocket() {
        return channel;
    }

    @Override
    protected boolean getDeferAccept() {
        return false;
    }

    @Override
    protected boolean setSocketOptions(DatagramChannel channel) {
        return false;
    }

    @Override
    protected DatagramChannel serverSocketAccept() {
        return channel;
    }

    @Override
    protected SocketProcessorBase<UDPChannel> createSocketProcessor(SocketWrapperBase<UDPChannel> socketWrapper, SocketEvent event) {
        return null;
    }

    @Override
    protected void doCloseServerSocket() {
        if (channel != null) {
            destroySocket(channel);
            channel = null;
        }
    }

    @Override
    protected void destroySocket(DatagramChannel channel) {
        try {
            channel.close();
        } catch (IOException ioe) {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("endpoint.err.close"), ioe);
            }
        }
    }

}
