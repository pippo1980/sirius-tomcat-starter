package com.sirius.temcat.test;

import com.sirius.tomcat.ext.UDPProtocol;
import org.apache.catalina.connector.Connector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;

public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public ServletWebServerFactory servletContainer() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.addAdditionalTomcatConnectors(createUDPConnector());
        return tomcat;
    }

    private Connector createUDPConnector() {
        Connector connector = new Connector(UDPProtocol.class.getName());
        // UDPProtocol protocol = (UDPProtocol) connector.getProtocolHandler();

        connector.setSecure(false);
        connector.setPort(8443);
        // protocol.setSSLEnabled(false);
        return connector;

    }

}
