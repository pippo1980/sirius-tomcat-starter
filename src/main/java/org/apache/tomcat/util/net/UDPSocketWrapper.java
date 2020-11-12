package org.apache.tomcat.util.net;

import com.sirius.tomcat.ext.EmptyUPDChannel;
import com.sirius.tomcat.ext.UDPChannel;
import com.sirius.tomcat.ext.UDPEndpoint;
import com.sirius.tomcat.ext.UDPEndpointPoller;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.ExceptionUtils;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Getter
@Slf4j
public class UDPSocketWrapper extends SocketWrapperBase<UDPChannel> {

    private static final EmptyUPDChannel EMPTY = new EmptyUPDChannel(null);

    private final UDPEndpointPoller poller;

    private int interestOps = 0;
    private CountDownLatch readLatch = null;
    private CountDownLatch writeLatch = null;
    private volatile long lastRead = System.currentTimeMillis();
    private volatile long lastWrite = lastRead;

    public boolean isReadOperationProcess() {
        return this.readOperation != null && this.readOperation.process();
    }

    public boolean isWriteOperationProcess() {
        return this.writeOperation != null && this.writeOperation.process();
    }

    public UDPSocketWrapper(UDPChannel channel, UDPEndpoint endpoint) {
        super(channel, endpoint);
        poller = endpoint.getPoller();
        socketBufferHandler = channel.getBufHandler();
    }

    public int interestOps() {
        return interestOps;
    }

    public int interestOps(int ops) {
        this.interestOps = ops;
        return ops;
    }

    protected CountDownLatch resetLatch(CountDownLatch latch) {
        if (latch == null || latch.getCount() == 0) {
            return null;
        } else {
            throw new IllegalStateException(sm.getString("endpoint.nio.latchMustBeZero"));
        }
    }

    public void resetReadLatch() {
        readLatch = resetLatch(readLatch);
    }

    public void resetWriteLatch() {
        writeLatch = resetLatch(writeLatch);
    }

    protected CountDownLatch startLatch(CountDownLatch latch, int cnt) {
        if (latch == null || latch.getCount() == 0) {
            return new CountDownLatch(cnt);
        } else {
            throw new IllegalStateException(sm.getString("endpoint.nio.latchMustBeZero"));
        }
    }

    public void startReadLatch(int cnt) {
        readLatch = startLatch(readLatch, cnt);
    }

    public void startWriteLatch(int cnt) {
        writeLatch = startLatch(writeLatch, cnt);
    }

    protected void awaitLatch(CountDownLatch latch, long timeout, TimeUnit unit) throws InterruptedException {
        if (latch == null) {
            throw new IllegalStateException(sm.getString("endpoint.nio.nullLatch"));
        }
        // Note: While the return value is ignored if the latch does time
        //       out, logic further up the call stack will trigger a
        //       SocketTimeoutException
        latch.await(timeout, unit);
    }

    public void awaitReadLatch(long timeout, TimeUnit unit) throws InterruptedException {
        awaitLatch(readLatch, timeout, unit);
    }

    public void awaitWriteLatch(long timeout, TimeUnit unit) throws InterruptedException {
        awaitLatch(writeLatch, timeout, unit);
    }

    public void updateLastWrite() {
        lastWrite = System.currentTimeMillis();
    }

    public long getLastWrite() {
        return lastWrite;
    }

    public void updateLastRead() {
        lastRead = System.currentTimeMillis();
    }

    public long getLastRead() {
        return lastRead;
    }

    @Override
    public boolean isReadyForRead() throws IOException {
        socketBufferHandler.configureReadBufferForRead();

        if (socketBufferHandler.getReadBuffer().remaining() > 0) {
            return true;
        }

        fillReadBuffer(false);

        boolean isReady = socketBufferHandler.getReadBuffer().position() > 0;
        return isReady;
    }

    @Override
    public int read(boolean block, byte[] b, int off, int len) throws IOException {
        int nRead = populateReadBuffer(b, off, len);
        if (nRead > 0) {
            return nRead;
            /*
             * Since more bytes may have arrived since the buffer was last
             * filled, it is an option at this point to perform a
             * non-blocking read. However correctly handling the case if
             * that read returns end of stream adds complexity. Therefore,
             * at the moment, the preference is for simplicity.
             */
        }

        // Fill the read buffer as best we can.
        nRead = fillReadBuffer(block);
        updateLastRead();

        // Fill as much of the remaining byte array as possible with the
        // data that was just read
        if (nRead > 0) {
            socketBufferHandler.configureReadBufferForRead();
            nRead = Math.min(nRead, len);
            socketBufferHandler.getReadBuffer().get(b, off, nRead);
        }
        return nRead;
    }

    @Override
    public int read(boolean block, ByteBuffer to) throws IOException {
        int nRead = populateReadBuffer(to);
        if (nRead > 0) {
            return nRead;
            /*
             * Since more bytes may have arrived since the buffer was last
             * filled, it is an option at this point to perform a
             * non-blocking read. However correctly handling the case if
             * that read returns end of stream adds complexity. Therefore,
             * at the moment, the preference is for simplicity.
             */
        }

        // The socket read buffer capacity is socket.appReadBufSize
        int limit = socketBufferHandler.getReadBuffer().capacity();
        if (to.remaining() >= limit) {
            to.limit(to.position() + limit);
            nRead = fillReadBuffer(block, to);
            if (log.isDebugEnabled()) {
                log.debug("Socket: [" + this + "], Read direct from socket: [" + nRead + "]");
            }
            updateLastRead();
        } else {
            // Fill the read buffer as best we can.
            nRead = fillReadBuffer(block);
            if (log.isDebugEnabled()) {
                log.debug("Socket: [" + this + "], Read into buffer: [" + nRead + "]");
            }
            updateLastRead();

            // Fill as much of the remaining byte array as possible with the
            // data that was just read
            if (nRead > 0) {
                nRead = populateReadBuffer(to);
            }
        }
        return nRead;
    }

    @Override
    protected void doClose() {
        if (log.isDebugEnabled()) {
            log.debug("Calling [" + getEndpoint() + "].closeSocket([" + this + "])");
        }
        try {
            // getEndpoint().connections.remove(getSocket().getIOChannel());
            if (getSocket().isOpen()) {
                getSocket().close(true);
            }
            if (getEndpoint().isRunning() && !getEndpoint().isPaused()) {
                getSocket().free();
            }
        } catch (Throwable e) {
            ExceptionUtils.handleThrowable(e);
            if (log.isDebugEnabled()) {
                log.error(sm.getString("endpoint.debug.channelCloseFail"), e);
            }
        } finally {
            socketBufferHandler = SocketBufferHandler.EMPTY;
            nonBlockingWriteBuffer.clear();
            reset(EMPTY);
        }

    }

    private int fillReadBuffer(boolean block) throws IOException {
        socketBufferHandler.configureReadBufferForWrite();
        return fillReadBuffer(block, socketBufferHandler.getReadBuffer());
    }

    private int fillReadBuffer(boolean block, ByteBuffer to) throws IOException {
        UDPChannel socket = getSocket();

        if (socket == EMPTY) {
            throw new ClosedChannelException();
        }

        return socket.read(to);
    }

    @Override
    protected void doWrite(boolean block, ByteBuffer from) throws IOException {
        UDPChannel socket = getSocket();

        if (socket == EMPTY) {
            throw new ClosedChannelException();
        }

        // TODO check buffer size < 64KB

        socket.write(from);
        updateLastWrite();
    }

    @Override
    public void registerReadInterest() {
        if (log.isDebugEnabled()) {
            log.debug(sm.getString("endpoint.debug.registerRead", this));
        }

        getPoller().add(this, SelectionKey.OP_READ);
    }

    @Override
    public void registerWriteInterest() {
        if (log.isDebugEnabled()) {
            log.debug(sm.getString("endpoint.debug.registerWrite", this));
        }

        getPoller().add(this, SelectionKey.OP_WRITE);
    }

    @Override
    public SendfileDataBase createSendfileData(String filename, long pos, long length) {
        throw new RuntimeException("not support");
    }

    @Override
    public SendfileState processSendfile(SendfileDataBase sendfileData) {
        throw new RuntimeException("not support");
    }

    @Override
    protected void populateRemoteAddr() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getInetAddress();
            if (inetAddr != null) {
                remoteAddr = inetAddr.getHostAddress();
            }
        }
    }

    @Override
    protected void populateRemoteHost() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getInetAddress();
            if (inetAddr != null) {
                remoteHost = inetAddr.getHostName();
                if (remoteAddr == null) {
                    remoteAddr = inetAddr.getHostAddress();
                }
            }
        }
    }

    @Override
    protected void populateRemotePort() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            remotePort = sc.socket().getPort();
        }
    }

    @Override
    protected void populateLocalName() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getLocalAddress();
            if (inetAddr != null) {
                localName = inetAddr.getHostName();
            }
        }
    }

    @Override
    protected void populateLocalAddr() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getLocalAddress();
            if (inetAddr != null) {
                localAddr = inetAddr.getHostAddress();
            }
        }
    }

    @Override
    protected void populateLocalPort() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            localPort = sc.socket().getLocalPort();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param clientCertProvider Ignored for this implementation
     */
    @Override
    public SSLSupport getSslSupport(String clientCertProvider) {
        return null;
    }

    @Override
    public void doClientAuth(SSLSupport sslSupport) throws IOException {

    }

    @Override
    public void setAppReadBufHandler(ApplicationBufferHandler handler) {
        getSocket().setAppReadBufHandler(handler);
    }

    @Override
    protected <A> OperationState<A> newOperationState(boolean read,
            ByteBuffer[] buffers,
            int offset,
            int length,
            BlockingMode block,
            long timeout,
            TimeUnit unit,
            A attachment,
            CompletionCheck check,
            CompletionHandler<Long, ? super A> handler,
            Semaphore semaphore,
            VectoredIOCompletionHandler<A> completion) {

        return new NioOperationState<>(read, buffers, offset, length, block,
                timeout, unit, attachment, check, handler, semaphore, completion);
    }

    private class NioOperationState<A> extends OperationState<A> {

        private volatile boolean inline = true;

        private NioOperationState(boolean read,
                ByteBuffer[] buffers,
                int offset,
                int length,
                BlockingMode block,
                long timeout,
                TimeUnit unit,
                A attachment,
                CompletionCheck check,
                CompletionHandler<Long, ? super A> handler,
                Semaphore semaphore,
                VectoredIOCompletionHandler<A> completion) {
            super(read, buffers, offset, length, block, timeout, unit, attachment, check, handler, semaphore, completion);
        }

        @Override
        protected boolean isInline() {
            return inline;
        }

        @Override
        public void run() {
            // Perform the IO operation
            // Called from the poller to continue the IO operation
            long nBytes = 0;
            if (getError() == null) {
                try {
                    synchronized (this) {
                        if (!completionDone) {
                            // This filters out same notification until processing
                            // of the current one is done
                            if (log.isDebugEnabled()) {
                                log.debug("Skip concurrent " + (read ? "read" : "write") + " notification");
                            }
                            return;
                        }
                        if (read) {
                            // Read from main buffer first
                            if (!socketBufferHandler.isReadBufferEmpty()) {
                                // There is still data inside the main read buffer, it needs to be read first
                                socketBufferHandler.configureReadBufferForRead();
                                for (int i = 0; i < length && !socketBufferHandler.isReadBufferEmpty(); i++) {
                                    nBytes += transfer(socketBufferHandler.getReadBuffer(), buffers[offset + i]);
                                }
                            }
                            if (nBytes == 0) {
                                nBytes = getSocket().read(buffers, offset, length);
                                updateLastRead();
                            }
                        } else {
                            boolean doWrite = true;
                            // Write from main buffer first
                            if (!socketBufferHandler.isWriteBufferEmpty()) {
                                // There is still data inside the main write buffer, it needs to be written first
                                socketBufferHandler.configureWriteBufferForRead();
                                do {
                                    nBytes = getSocket().write(socketBufferHandler.getWriteBuffer());
                                } while (!socketBufferHandler.isWriteBufferEmpty() && nBytes > 0);
                                if (!socketBufferHandler.isWriteBufferEmpty()) {
                                    doWrite = false;
                                }
                                // Preserve a negative value since it is an error
                                if (nBytes > 0) {
                                    nBytes = 0;
                                }
                            }
                            if (doWrite) {
                                long n = 0;
                                do {
                                    n = getSocket().write(buffers, offset, length);
                                    if (n == -1) {
                                        nBytes = n;
                                    } else {
                                        nBytes += n;
                                    }
                                } while (n > 0);
                                updateLastWrite();
                            }
                        }
                        if (nBytes != 0 || !buffersArrayHasRemaining(buffers, offset, length)) {
                            completionDone = false;
                        }
                    }
                } catch (IOException e) {
                    setError(e);
                }
            }
            if (nBytes > 0 || (nBytes == 0 && !buffersArrayHasRemaining(buffers, offset, length))) {
                // The bytes processed are only updated in the completion handler
                completion.completed(Long.valueOf(nBytes), this);
            } else if (nBytes < 0 || getError() != null) {
                IOException error = getError();
                if (error == null) {
                    error = new EOFException();
                }
                completion.failed(error, this);
            } else {
                // As soon as the operation uses the poller, it is no longer inline
                inline = false;
                if (read) {
                    registerReadInterest();
                } else {
                    registerWriteInterest();
                }
            }
        }

    }

}
