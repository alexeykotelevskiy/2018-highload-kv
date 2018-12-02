package ru.mail.polis.alexeykotelevskiy;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

public class KVDaoImpl implements KVDao {
    private DB db;
    private ConcurrentMap map;

    public KVDaoImpl(File data) {
        db = DBMaker.fileDB(data.getPath() + File.separator + "database.db")
                .fileChannelEnable()
                .make();
        map = db.treeMap("map")
                .keySerializer(Serializer.JAVA)
                .valueSerializer(Serializer.JAVA).createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        ValueNode val = (ValueNode) map.get(bKey);
        if (val == null || val.isMilestone()) {
            throw new NoSuchElementException();
        }
        return val.getVal();
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        SerializeBuffer bKey = new SerializeBuffer(key);
        map.put(bKey, new ValueNode(value, System.currentTimeMillis(), false));
    }

    @Override
    public void remove(@NotNull byte[] key) {
        SerializeBuffer bKey = new SerializeBuffer(key);

        ValueNode val = (ValueNode) map.get(bKey);
        if (val != null) {
            if (val.isMilestone()) {
                map.remove(bKey);
            } else {
                val.setMilestone(true);
                map.put(bKey, val);
            }
        }
    }

    public ValueNode getWithInfo(byte[] key) throws NoSuchElementException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        ValueNode node = (ValueNode) map.get(bKey);
        if (node == null) {
            throw new NoSuchElementException();
        }
        return node;
    }

    @Override
    public void close() {
        db.close();
    }
}
