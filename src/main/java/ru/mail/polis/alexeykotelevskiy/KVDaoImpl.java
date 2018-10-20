package ru.mail.polis.alexeykotelevskiy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    private BTree<SerializeBuffer, ValueNode> bTree;
    private final String path;

    public KVDaoImpl(File data) {
        this.path = data.getPath() + File.separator;
        String pathRoot = this.path + "btree";
        if (Files.exists(Paths.get(pathRoot))) {
            bTree = BTree.readFromDisk(pathRoot);
        } else {
            bTree = new BTree<>(data.getPath());
        }
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        ValueNode val = bTree.search(bKey);
        if (val == null || val.isMilestone()) {
            throw new NoSuchElementException();
        }
        return val.getVal();
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        SerializeBuffer bKey = new SerializeBuffer(key);
        bTree.add(bKey, new ValueNode(value, System.currentTimeMillis(), false));
    }

    @Override
    public void remove(@NotNull byte[] key) {
        SerializeBuffer bKey = new SerializeBuffer(key);
        ValueNode val = bTree.search(bKey);
        if (val != null) {
            if (val.isMilestone()) {
                bTree.remove(bKey);
            } else {
                val.setMilestone(true);
                bTree.add(bKey, val);
            }
        }
    }

    public ValueNode getWithInfo(byte[] key) throws NoSuchElementException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        ValueNode node = bTree.search(bKey);
        if (node == null) {
            throw new NoSuchElementException();
        }
        return node;
    }

    @Override
    public void close() {
        bTree = null;
    }
}
