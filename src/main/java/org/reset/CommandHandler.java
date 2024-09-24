package org.reset;

import com.nimbus.proto.messages.RequestMessage;
import com.nimbus.proto.messages.ResponseMessage;
import com.nimbus.proto.protocol.HeaderProtocol;
import com.nimbus.proto.protocol.RequestProtocol;
import com.nimbus.proto.protocol.ResponseProtocol;
import io.netty.buffer.ByteBuf;
import org.reset.datastore.DataStore;

import java.util.function.Function;

public class CommandHandler {

    private final DataStore dataStore;
    private Function<ByteBuf, ByteBuf>[] commands;

    public CommandHandler(DataStore dataStore) {
        this.dataStore = dataStore;
        this.commands = new Function[4];

        this.commands[RequestProtocol.CMD_SET] = this::set;
        this.commands[RequestProtocol.CMD_GET] = this::get;
        this.commands[RequestProtocol.CMD_DEL] = this::delete;
    }

    public ByteBuf process(ByteBuf msg) {
        int cmd = RequestProtocol.getCommand(msg);

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

        if (count <= 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).buffer();

        for (int x = 0; x < count; x++) {
            byte[] key = req.key();
            byte[] value = req.value();

            dataStore.put(key, value);
        }

        return ResponseMessage.of(req.buffer(), ResponseProtocol.STATUS_OK, count).end();
    }

    ByteBuf get(ByteBuf msg) {
        RequestMessage req = new RequestMessage(msg);

        HeaderProtocol.preintDebugHeaderLayout();

        boolean compressed = req.compression() == 1;

        System.out.println("Compression " + compressed);

        int count = req.count();
        System.out.println("Count " + count);
        if (count <= 0)
            return ResponseMessage.of(msg, ResponseProtocol.STATUS_INVALID_REQ, 0).end();

        ResponseMessage res = null;

        int found = 0;
        for (int x = 0; x < count; x++) {
            byte[] key = req.key();
            byte[] val = dataStore.get(key);

            System.out.println(new String(key));

            // try to reuse buffer if we already have what we need from req
            if (res == null)
                res = count == 1 ? new ResponseMessage(req) : new ResponseMessage();

            if (val != null) {
                res.key(key);
                res.value(val);
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
            byte[] key = req.key();
            if (dataStore.remove(key)) {
                deleted++;
            }
        }

        ResponseMessage res = new ResponseMessage(req);
        res.count(deleted);

        return res.end();
    }
}