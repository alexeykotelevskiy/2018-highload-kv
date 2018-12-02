package ru.mail.polis.alexeykotelevskiy;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;

import java.util.concurrent.Callable;

import static ru.mail.polis.alexeykotelevskiy.KvServiceImpl.TIMESTAMP_HEADER;

public class KeyInfoTask implements Callable<KeyInfo> {
    private final String path;
    private final String[] headers;
    private final HttpClient client;

    public KeyInfoTask(String path, HttpClient client, String ...headers) {
        this.path = path;
        this.headers = headers;
        this.client = client;
    }

    private static String getHeaderValue(String header) {
        return header.split(":")[1].trim();
    }

    @Override
    public KeyInfo call() throws Exception {
            Response resp = client.get(path, headers);
            if (resp.getStatus() == 200) {
                return new KeyInfo(State.EXIST,
                        Long.parseLong(getHeaderValue(resp.getHeader(TIMESTAMP_HEADER))),
                        resp.getBody());
            }
            if (resp.getStatus() == 504) {
                return new KeyInfo(State.REMOVED,
                        Long.parseLong(getHeaderValue(resp.getHeader(TIMESTAMP_HEADER))),
                        resp.getBody());
            }
            if (resp.getStatus() == 404) {
                return new KeyInfo(State.NONE,
                        Long.parseLong(getHeaderValue(resp.getHeader(TIMESTAMP_HEADER))),
                        resp.getBody());
            }
        return null;
    }
}
