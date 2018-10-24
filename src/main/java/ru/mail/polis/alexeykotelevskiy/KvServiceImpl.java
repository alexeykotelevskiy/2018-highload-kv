package ru.mail.polis.alexeykotelevskiy;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;
import java.util.*;

public class KvServiceImpl extends HttpServer implements KVService {
    private final KVDaoImpl kvDao;
    private Map<String, HttpClient> cluster = new HashMap<>();
    private static final String API_POINT = "/v0/entity";
    private final static String NEED_REPL_HEADER = "X-Need-Repl";
    private final static String TIMESTAMP_HEADER = "X-Timestamp";

    public KvServiceImpl(int port, KVDao kvDao, Set<String> topology) throws IOException {
        super(HttpServerConfigFactory.create(port));
        this.kvDao = (KVDaoImpl) kvDao;
        for (String str : topology) {
            if (Integer.parseInt(str.split(":")[2]) != port) {
                cluster.put(str, new HttpClient(new ConnectionString(str)));

            } else {
                cluster.put(str, null);
            }
        }
    }

    private List<HttpClient> getNodes(byte[] key, AckFrom ackFrom) {
        List<HttpClient> nodes = new LinkedList<>();
        String[] hosts = cluster.keySet().toArray(new String[cluster.size()]);
        int currHost = Math.abs(Arrays.hashCode(key) % hosts.length);
        for (int i = 0; i < ackFrom.getFrom(); i++) {
            nodes.add(cluster.get(hosts[(currHost + i) % hosts.length]));
        }
        return nodes;
    }

    private static String getHeaderValue(String header) {
        return header.split(":")[1].trim();
    }

    private String makePath(String host, byte[] key) {
        return host + "?id=" + (new String(key));
    }

    private Response merge(List<KeyInfo> ans, AckFrom ackFrom) {
        KeyInfo lastUpdate = null;

        if (ans.size() < ackFrom.getAck()) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        for (KeyInfo keyInfo : ans) {
            if (keyInfo.getState() == State.EXIST) {
                if (lastUpdate == null || lastUpdate.getTimestamp() > keyInfo.getTimestamp()) {
                    lastUpdate = keyInfo;
                }
            }
            if (keyInfo.getState() == State.REMOVED) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
        }
        if (lastUpdate != null)
            return new Response(Response.OK, lastUpdate.getValue());
        else
            return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private Response getData(byte[] key, AckFrom ackFrom, boolean needRepl) {
        List<HttpClient> nodes = getNodes(key, ackFrom);
        if (!needRepl) {
            try {
                ValueNode node = kvDao.getWithInfo(key);
                if (node.isMilestone()) {
                    Response response = new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                    response.addHeader(TIMESTAMP_HEADER + ": " + String.valueOf(node.getTimestamp()));
                    return response;
                } else {
                    Response response = new Response(Response.OK, node.getVal());
                    response.addHeader(TIMESTAMP_HEADER + ": " + String.valueOf(node.getTimestamp()));
                    return response;
                }
            } catch (NoSuchElementException e) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
        }
        List<KeyInfo> ansList = new LinkedList<>();
        for (HttpClient client : nodes) {
            if (client == null) {
                try {
                    ValueNode node = kvDao.getWithInfo(key);
                    KeyInfo keyInfo = new KeyInfo(node.isMilestone() ? State.REMOVED : State.EXIST,
                            node.getTimestamp(),
                            node.getVal());
                    ansList.add(keyInfo);
                } catch (NoSuchElementException e) {
                    ansList.add(new KeyInfo(State.NONE, 0, null));
                }
            } else {
                try {
                    Response resp = client.get(makePath(API_POINT, key), NEED_REPL_HEADER + ": 1");
                    if (resp.getStatus() == 200) {
                        ansList.add(new KeyInfo(State.EXIST,
                                Long.parseLong(getHeaderValue(resp.getHeader(TIMESTAMP_HEADER))),
                                resp.getBody()));
                    }
                    if (resp.getStatus() == 504) {
                        ansList.add(new KeyInfo(State.REMOVED,
                                Long.parseLong(getHeaderValue(resp.getHeader(TIMESTAMP_HEADER))),
                                resp.getBody()));
                    }
                    if (resp.getStatus() == 404) {
                        ansList.add(new KeyInfo(State.NONE,
                                Long.parseLong(getHeaderValue(resp.getHeader(TIMESTAMP_HEADER))),
                                resp.getBody()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return merge(ansList, ackFrom);
    }

    private static Response checkAck(AckFrom ackFrom, int currAck, String successStatus) {
        if (currAck >= ackFrom.getAck()) {
            return new Response(successStatus, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response putData(byte[] key, byte[] value, AckFrom ackFrom, boolean needRepl) {
        List<HttpClient> nodes = getNodes(key, ackFrom);
        int ack = 0;
        if (!needRepl) {
            kvDao.upsert(key, value);
            return new Response(Response.CREATED, Response.EMPTY);
        }
        for (HttpClient client : nodes) {
            if (client == null) {
                kvDao.upsert(key, value);
                ack++;
            } else {
                try {
                    if (client.put(makePath(API_POINT, key), value, NEED_REPL_HEADER + ": 1").getStatus() == 201) {
                        ack++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return checkAck(ackFrom, ack, Response.CREATED);
    }

    private Response deleteData(byte[] key, AckFrom ackFrom, boolean needRepl) {
        List<HttpClient> nodes = getNodes(key, ackFrom);
        int ack = 0;
        if (!needRepl) {
            kvDao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        for (HttpClient client : nodes) {
            if (client == null) {
                kvDao.remove(key);
                ack++;
            } else {
                try {
                    if (client.delete(makePath(API_POINT, key), NEED_REPL_HEADER + ": 1").getStatus() == 202) {
                        ack++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return checkAck(ackFrom, ack, Response.ACCEPTED);
    }

    @Path("/v0/status")
    public Response status(Request request) {
        return Response.ok(Response.EMPTY);
    }

    @Path(API_POINT)
    public void apiPoint(Request request, HttpSession session) throws IOException {
        String strKey = request.getParameter("id=");
        String replicas = request.getParameter("replicas=");
        String needReplHeader = request.getHeader(NEED_REPL_HEADER);
        if (strKey == null || strKey.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "Bad Request");
            return;
        }
        AckFrom ackFrom;
        if (replicas != null) {
            String[] split = replicas.split("/");
            ackFrom = new AckFrom(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
            if (ackFrom.getAck() > ackFrom.getFrom() || ackFrom.getAck() == 0) {
                session.sendError(Response.BAD_REQUEST, "Bad Request");
                return;
            }
        } else {
            ackFrom = new AckFrom(cluster.size() / 2 + 1, cluster.size());
        }
        boolean needRepl = needReplHeader == null;
        byte[] key = strKey.getBytes();
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(getData(key, ackFrom, needRepl));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(putData(key, request.getBody(), ackFrom, needRepl));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(deleteData(key, ackFrom, needRepl));
                    return;
                default:
                    session.sendError(Response.BAD_REQUEST, "Unsupported method");
            }
        } catch (Exception e) {
            session.sendError(Response.INTERNAL_ERROR, "Internal error");
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendError(Response.BAD_REQUEST, Arrays.toString(Response.EMPTY));
    }
}

class AckFrom {
    private final int ack;
    private final int from;

    AckFrom(int ack, int from) {
        this.ack = ack;
        this.from = from;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
