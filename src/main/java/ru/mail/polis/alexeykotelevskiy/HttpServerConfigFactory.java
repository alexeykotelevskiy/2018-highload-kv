package ru.mail.polis.alexeykotelevskiy;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

public class HttpServerConfigFactory {
    public static HttpServerConfig create(int port) {
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }
}
