package com.sirius.tomcat.ext;

public class UDPPollerEvent {

    private UDPChannel socket;
    private int interestOps;

    public UDPPollerEvent(UDPChannel channel, int intOps) {
        reset(channel, intOps);
    }

    public void reset(UDPChannel ch, int intOps) {
        socket = ch;
        interestOps = intOps;
    }

    public UDPChannel getSocket() {
        return socket;
    }

    public int getInterestOps() {
        return interestOps;
    }

    public void reset() {
        reset(null, 0);
    }

    @Override
    public String toString() {
        return "Poller event: socket [" + socket + "], socketWrapper [" + socket.getSocketWrapper() + "], interestOps [" + interestOps + "]";
    }

}
