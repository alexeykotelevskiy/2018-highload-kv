package ru.mail.polis.alexeykotelevskiy;

import one.nio.http.*;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;
import java.util.NoSuchElementException;

public class KvServiceImpl extends HttpServer implements KVService {
    private KVDao kvDao;

    public KvServiceImpl(int port, KVDao kvDao) throws IOException {
        super(HttpServerConfigFactory.create(port));
        this.kvDao = kvDao;
    }
    Response getData(byte[] key) throws IOException{
        try {
            byte[] value = kvDao.get(key);
            return Response.ok(value);
        } catch (NoSuchElementException e)
        {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    Response putData(byte[] key, byte[] value) throws IOException{
        kvDao.upsert(key, value);
        Response response = new Response(Response.CREATED, Response.EMPTY);
        return response;
    }

    Response deleteData(byte[] key) throws IOException{
        kvDao.remove(key);
        Response response = new Response(Response.ACCEPTED, Response.EMPTY);
        return response;
    }

    @Path("/v0/status")
    public Response status(Request request){
        return Response.ok(Response.EMPTY);
    }

    @Path("/v0/entity")
    public void apiPoint(Request request, HttpSession session) throws IOException{
        String strKey = request.getParameter("id=");
        if (strKey==null || strKey.isEmpty())
        {
            session.sendError(Response.BAD_REQUEST, "Bad Request");
            return;
        }
        byte[] key = strKey.getBytes();
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(getData(key));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(putData(key, request.getBody()));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(deleteData(key));
                    return;
                    default: session.sendError(Response.BAD_REQUEST, "Unsupported method");
            }
        } catch (IOException e){
            session.sendError(Response.INTERNAL_ERROR, "Internal error");
        }
    }
}
