package org.reset;

import com.nimbus.net.Node;
import com.nimbus.proto.messages.RequestMessage;
import com.nimbus.proto.messages.ResponseMessage;
import com.nimbus.proto.protocol.HeaderProtocol;
import com.nimbus.proto.protocol.RequestProtocol;
import com.nimbus.proto.protocol.ResponseProtocol;
import io.netty.buffer.ByteBuf;
import org.reset.datastore.ReplicatingDataStore;
import org.reset.replication.ReplicationManager;

import java.util.*;
import java.util.function.Function;

public class CommandHandler {

    private final ReplicatingDataStore replicatedStore;
    private Function<ByteBuf, ByteBuf>[] commands;

    public CommandHandler(ReplicatingDataStore replicatedStore) {
        this.replicatedStore = replicatedStore;

        this.commands = new Function[102];

        this.commands[RequestProtocol.CMD_SET] = this::set;
        this.commands[RequestProtocol.CMD_GET] = this::get;
        this.commands[RequestProtocol.CMD_DEL] = this::delete;

        this.commands[RequestProtocol.REPL_CMD_ECHO] = this::echo;
        this.commands[RequestProtocol.REPL_CMD_BUCKET_MAPPING] = this::bucketPeerMapping;
    }

    public ByteBuf process(ByteBuf msg) {
        int cmd = RequestProtocol.getCommand(msg);

        System.out.println(cmd);

        if (cmd < 0 || cmd >= commands.length)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).buffer();

        Function<ByteBuf, ByteBuf> command = commands[cmd];
        if (command == null)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).buffer();

        return command.apply(msg);
    }

    ByteBuf set(ByteBuf msg) {
        RequestMessage req = new RequestMessage(msg);

        boolean compressed = req.compression() == 1;

        int count = req.count();

        int success = 0;
        int moved = 0;

        if (count <= 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).buffer();

        for (int x = 0; x < count; x++) {
            byte[] key = req.keyAsBytes();
            byte[] value = req.valueAsBytes();

            ReplicatingDataStore.OperationStatus status = replicatedStore.put(key, value);
            if (status == ReplicatingDataStore.OperationStatus.SUCCESS)
                success++;
            else if (status == ReplicatingDataStore.OperationStatus.DATA_NOT_OURS)
                moved++;
        }

        System.out.println("Set " + count + " keys");

        return ResponseMessage.of(req.buffer(), ResponseProtocol.STATUS_OK, count).end();
    }

    ByteBuf get(ByteBuf msg) {
        RequestMessage req = new RequestMessage(msg);

        HeaderProtocol.preintDebugHeaderLayout();

        boolean compressed = req.compression() == 1;

        System.out.println("Compression " + compressed);

        int count = req.count();
        if (count <= 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).end();

        ResponseMessage res = null;

        int found = 0;
        for (int x = 0; x < count; x++) {
            byte[] key = req.keyAsBytes();
            byte[] val = replicatedStore.get(key);

            System.out.println(new String(key));

            // try to reuse buffer if we already have what we need from req
            if (res == null)
                res = count == 1 ? new ResponseMessage(req) : new ResponseMessage();

            if (val != null) {
                res.key(key);
                res.value(val);
                System.out.println("keylen " + key.length + " vallen " + val.length);
                System.out.println("Wrote " + new String(key) + " -> " + new String(val));
                found++;
            }
        }

        res.status(ResponseProtocol.STATUS_OK);
        res.count(found);

        return res.end();
    }

    ByteBuf delete(ByteBuf msg) {
        RequestMessage req = new RequestMessage(msg);
        int count = req.count();
        if (count <= 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).end();

        int deleted = 0;
        for (int c = 0; c < count; c++) {
            byte[] key = req.keyAsBytes();
            ReplicatingDataStore.OperationStatus status = replicatedStore.remove(key);
            if (status == ReplicatingDataStore.OperationStatus.SUCCESS) {
                deleted++;
            } else if (status == ReplicatingDataStore.OperationStatus.DATA_NOT_EXIST) {
                // we need to indicate individual keys which didnt belong to us
            }
        }

        ResponseMessage res = new ResponseMessage(req);
        res.count(deleted);

        return res.end();
    }

    ByteBuf echo(ByteBuf msg) {
        RequestMessage req = new RequestMessage(msg);

        int count = req.count();
        if (count <= 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).end();

        byte[] data = req.valueAsBytes();
        if (data == null || data.length == 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).end();

        ResponseMessage res = new ResponseMessage(req);
        res.count(1);
        res.value(data);

        System.out.println("Echoing " + new String(data));

        return res.end();
    }

    /**
     * Write bucket mapping response in format of
     * count (unique count of hosts -> bucket mappings in response)
     * then #count repeated entries of
     * key (string) host node address
     * key (int) number of buckets entries following which are specific to this host
     * key (int), key (int).. repeated bucket index number values
     * @param msg
     * @return
     */
    ByteBuf bucketPeerMapping(ByteBuf msg) {
        ReplicationManager replicationManager = this.replicatedStore.getReplicationManager();

        Map<Integer, List<Node>> map = replicationManager == null ? Collections.emptyMap()
                : replicationManager.getLocalBucketMapping();
        Map<Node, List<Integer>> peerMap = new HashMap<>(map.size());

        map.forEach((bucket, peers) -> {
            peers.forEach(peer -> {
                peerMap.computeIfAbsent(peer, k -> new ArrayList<>()).add(bucket);
            });
        });

        final ResponseMessage res = new ResponseMessage(new RequestMessage(msg));

        res.count(peerMap.size());
        peerMap.forEach((peer, buckets) -> {
            res.key(peer.getHost());
            // res.key(peer.getId()); need ID??
            res.key(buckets.size());

            buckets.forEach(b -> res.key(b));
        });

        return res.end();
    }

}