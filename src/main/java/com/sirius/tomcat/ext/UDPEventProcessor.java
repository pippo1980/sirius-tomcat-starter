package com.sirius.tomcat.ext;

import org.apache.tomcat.util.net.SocketEvent;
import org.apache.tomcat.util.net.UDPSocketWrapper;

public interface UDPEventProcessor {

    boolean process(UDPSocketWrapper wrapper, SocketEvent event, boolean dispatch);

}
