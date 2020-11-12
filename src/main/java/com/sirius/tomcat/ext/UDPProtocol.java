package com.sirius.tomcat.ext;

import org.apache.coyote.http11.AbstractHttp11JsseProtocol;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class UDPProtocol extends AbstractHttp11JsseProtocol<UDPChannel> {

    private static final Log log = LogFactory.getLog(Http11NioProtocol.class);

    public UDPProtocol() {
        super(new UDPEndpoint());
    }

    @Override
    protected Log getLog() {
        return log;
    }

    @Override
    protected String getNamePrefix() {
        return "http-udp";
    }

}
