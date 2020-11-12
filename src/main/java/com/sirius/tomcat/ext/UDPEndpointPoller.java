package com.sirius.tomcat.ext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.collections.SynchronizedQueue;
import org.apache.tomcat.util.net.NioEndpoint.NioSocketWrapper;
import org.apache.tomcat.util.net.SocketEvent;
import org.apache.tomcat.util.net.SocketWrapperBase;
import org.apache.tomcat.util.net.UDPSocketWrapper;
import org.apache.tomcat.util.res.StringManager;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Slf4j
public class UDPEndpointPoller implements Runnable {

    private static final StringManager sm = StringManager.getManager(UDPEndpointPoller.class);
    private static final int OP_REGISTER = 0x100;

    public UDPEndpointPoller(UDPEndpoint endpoint) throws IOException {
        this.endpoint = endpoint;
        this.selector = Selector.open();
    }

    private final UDPEndpoint endpoint;
    private final Selector selector;
    private final SynchronizedQueue<UDPPollerEvent> events = new SynchronizedQueue<>();

    private volatile boolean close = false;
    // Optimize expiration handling
    private long nextExpiration = 0;

    private AtomicLong wakeupCounter = new AtomicLong(0);

    private volatile int keyCount = 0;

    public void destroy() {
        // Wait for polltime before doing anything, so that the poller threads
        // exit, otherwise parallel closure of sockets which are still
        // in the poller can cause problems
        close = true;
        selector.wakeup();
    }

    void addEvent(UDPPollerEvent event) {
        events.offer(event);

        if (wakeupCounter.incrementAndGet() == 0) {
            selector.wakeup();
        }
    }

   public void add(UDPSocketWrapper wrapper, int interestOps) {
        UDPPollerEvent event = null;

        if (endpoint.getEventCache() != null) {
            event = endpoint.getEventCache().pop();
        }

        if (event == null) {
            event = new UDPPollerEvent(wrapper.getSocket(), interestOps);
        } else {
            event.reset(wrapper.getSocket(), interestOps);
        }

        addEvent(event);

        if (close) {
            endpoint.processSocket(wrapper, SocketEvent.STOP, false);
        }
    }

    boolean events() {
        boolean result = false;

        UDPPollerEvent event = null;
        for (int i = 0, size = events.size(); i < size && (event = events.poll()) != null; i++) {
            result = true;
            UDPChannel channel = event.getSocket();
            UDPSocketWrapper wrapper = channel.getSocketWrapper();
            int interestOps = event.getInterestOps();
            if (interestOps == OP_REGISTER) {
                try {
                    channel.getIOChannel().register(getSelector(), SelectionKey.OP_READ, wrapper);
                } catch (Exception x) {
                    log.error(sm.getString("endpoint.nio.registerFail"), x);
                }
            } else {
                final SelectionKey key = channel.getIOChannel().keyFor(getSelector());
                if (key == null) {
                    // The key was cancelled (e.g. due to socket closure)
                    // and removed from the selector while it was being
                    // processed. Count down the connections at this point
                    // since it won't have been counted down when the socket
                    // closed.
                    wrapper.close();
                } else {
                    final UDPSocketWrapper attachment = (UDPSocketWrapper) key.attachment();
                    if (attachment != null) {
                        // We are registering the key to start with, reset the fairness counter.
                        try {
                            int ops = key.interestOps() | interestOps;
                            attachment.interestOps(ops);
                            key.interestOps(ops);
                        } catch (CancelledKeyException ckx) {
                            cancelledKey(key, wrapper);
                        }
                    } else {
                        cancelledKey(key, attachment);
                    }
                }
            }

            if (endpoint.isRunning() && !endpoint.isPaused() && endpoint.getEventCache() != null) {
                event.reset();
                endpoint.getEventCache().push(event);
            }
        }

        return result;
    }

    public void register(final UDPChannel socket, final NioSocketWrapper wrapper) {
        wrapper.interestOps(SelectionKey.OP_READ);//this is what OP_REGISTER turns into.
        UDPPollerEvent event = null;

        if (endpoint.getEventCache() != null) {
            event = endpoint.getEventCache().pop();
        }

        if (event == null) {
            event = new UDPPollerEvent(socket, OP_REGISTER);
        } else {
            event.reset(socket, OP_REGISTER);
        }

        addEvent(event);
    }

    public void cancelledKey(SelectionKey key, SocketWrapperBase<UDPChannel> wrapper) {
        try {
            // If is important to cancel the key first, otherwise a deadlock may occur between the
            // poller select and the socket channel close which would cancel the key
            if (key != null) {
                key.attach(null);
                if (key.isValid()) {
                    key.cancel();
                }
            }
        } catch (Throwable e) {
            ExceptionUtils.handleThrowable(e);
            if (log.isDebugEnabled()) {
                log.error(sm.getString("endpoint.debug.channelCloseFail"), e);
            }
        } finally {
            if (wrapper != null) {
                wrapper.close();
            }
        }
    }

    @Override
    public void run() {
        // Loop until destroy() is called
        while (true) {

            boolean hasEvents = false;

            try {
                if (!close) {
                    hasEvents = events();

                    if (wakeupCounter.getAndSet(-1) > 0) {
                        // If we are here, means we have other stuff to do
                        // Do a non blocking select
                        keyCount = selector.selectNow();
                    } else {
                        keyCount = selector.select(endpoint.getSelectorTimeout());
                    }

                    wakeupCounter.set(0);
                }
                if (close) {
                    events();
                    timeout(0, false);
                    try {
                        selector.close();
                    } catch (IOException ioe) {
                        log.error(sm.getString("endpoint.nio.selectorCloseFail"), ioe);
                    }
                    break;
                }
            } catch (Throwable x) {
                ExceptionUtils.handleThrowable(x);
                log.error(sm.getString("endpoint.nio.selectorLoopError"), x);
                continue;
            }
            // Either we timed out or we woke up, process events first
            if (keyCount == 0) {
                hasEvents = (hasEvents | events());
            }

            Iterator<SelectionKey> iterator = keyCount > 0 ? selector.selectedKeys().iterator() : null;
            // Walk through the collection of ready keys and dispatch
            // any active event.
            while (iterator != null && iterator.hasNext()) {
                SelectionKey sk = iterator.next();
                UDPSocketWrapper wrapper = (UDPSocketWrapper) sk.attachment();
                // Attachment may be null if another thread has called
                // cancelledKey()
                if (wrapper == null) {
                    iterator.remove();
                } else {
                    iterator.remove();
                    processKey(sk, wrapper);
                }
            }

            // Process timeouts
            timeout(keyCount, hasEvents);
        }

        endpoint.getStopLatch().countDown();
    }

    protected void processKey(SelectionKey key, UDPSocketWrapper wrapper) {
        try {
            if (close) {
                cancelledKey(key, wrapper);
            } else if (key.isValid() && wrapper != null) {
                if (key.isReadable() || key.isWritable()) {

                    unreg(key, wrapper, key.readyOps());
                    boolean closeSocket = false;
                    // Read goes before write
                    if (key.isReadable()) {
                        if (!wrapper.isReadOperationProcess()) {
                            closeSocket = true;
                        } else if (!endpoint.processSocket(wrapper, SocketEvent.OPEN_READ, true)) {
                            closeSocket = true;
                        }
                    }

                    if (!closeSocket && key.isWritable()) {
                        if (!wrapper.isWriteOperationProcess()) {
                            closeSocket = true;
                        } else if (!endpoint.processSocket(wrapper, SocketEvent.OPEN_WRITE, true)) {
                            closeSocket = true;
                        }
                    }

                    if (closeSocket) {
                        cancelledKey(key, wrapper);
                    }

                }
            } else {
                // Invalid key
                cancelledKey(key, wrapper);
            }
        } catch (CancelledKeyException ckx) {
            cancelledKey(key, wrapper);
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            log.error(sm.getString("endpoint.nio.keyProcessingError"), t);
        }
    }

    protected void unreg(SelectionKey key, UDPSocketWrapper wrapper, int ops) {
        // This is a must, so that we don't have multiple threads messing with the socket
        reg(key, wrapper, key.interestOps() & (~ops));
    }

    protected void reg(SelectionKey key, UDPSocketWrapper wrapper, int ops) {
        key.interestOps(ops);
        wrapper.interestOps(ops);
    }

    protected void timeout(int keyCount, boolean hasEvents) {
        long now = System.currentTimeMillis();
        // This method is called on every loop of the Poller. Don't process
        // timeouts on every loop of the Poller since that would create too
        // much load and timeouts can afford to wait a few seconds.
        // However, do process timeouts if any of the following are true:
        // - the selector simply timed out (suggests there isn't much load)
        // - the nextExpiration time has passed
        // - the server socket is being closed
        if (nextExpiration > 0 && (keyCount > 0 || hasEvents) && (now < nextExpiration) && !close) {
            return;
        }
        int keycount = 0;
        try {
            for (SelectionKey key : selector.keys()) {
                keycount++;
                try {
                    UDPSocketWrapper wrapper = (UDPSocketWrapper) key.attachment();
                    if (wrapper == null) {
                        // We don't support any keys without attachments
                        cancelledKey(key, null);
                    } else if (close) {
                        key.interestOps(0);
                        // Avoid duplicate stop calls
                        wrapper.interestOps(0);
                        cancelledKey(key, wrapper);
                    } else if ((wrapper.interestOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ ||
                               (wrapper.interestOps() & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
                        boolean readTimeout = false;
                        boolean writeTimeout = false;
                        // Check for read timeout
                        if ((wrapper.interestOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                            long delta = now - wrapper.getLastRead();
                            long timeout = wrapper.getReadTimeout();
                            if (timeout > 0 && delta > timeout) {
                                readTimeout = true;
                            }
                        }
                        // Check for write timeout
                        if (!readTimeout && (wrapper.interestOps() & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
                            long delta = now - wrapper.getLastWrite();
                            long timeout = wrapper.getWriteTimeout();
                            if (timeout > 0 && delta > timeout) {
                                writeTimeout = true;
                            }
                        }
                        if (readTimeout || writeTimeout) {
                            key.interestOps(0);
                            // Avoid duplicate timeout calls
                            wrapper.interestOps(0);
                            wrapper.setError(new SocketTimeoutException());
                            if (readTimeout && !wrapper.isReadOperationProcess()) {
                                cancelledKey(key, wrapper);
                            } else if (writeTimeout && !wrapper.isWriteOperationProcess()) {
                                cancelledKey(key, wrapper);
                            } else if (!getEndpoint().processSocket(wrapper, SocketEvent.ERROR, true)) {
                                cancelledKey(key, wrapper);
                            }
                        }
                    }
                } catch (CancelledKeyException ckx) {
                    cancelledKey(key, (UDPSocketWrapper) key.attachment());
                }
            }
        } catch (ConcurrentModificationException cme) {
            // See https://bz.apache.org/bugzilla/show_bug.cgi?id=57943
            log.warn(sm.getString("endpoint.nio.timeoutCme"), cme);
        }

        // For logging purposes only
        long prevExp = nextExpiration;
        nextExpiration = System.currentTimeMillis() + endpoint.getSocketProperties().getTimeoutInterval();
        if (log.isTraceEnabled()) {
            log.trace("timeout completed: keys processed=" + keycount +
                      "; now=" + now + "; nextExpiration=" + prevExp +
                      "; keyCount=" + keyCount + "; hasEvents=" + hasEvents +
                      "; eval=" + ((now < prevExp) && (keyCount > 0 || hasEvents) && (!close)));
        }

    }

}
