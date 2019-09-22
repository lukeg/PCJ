package org.pcj.test.mpi;

import org.pcj.PCJ;
import org.pcj.internal.InternalPCJ;
import org.pcj.internal.Networker;
import org.pcj.internal.NetworkerInterface;
import org.pcj.internal.message.Message;
import org.pcj.internal.network.LoopbackSocketChannel;
import org.pcj.internal.network.MessageBytesInputStream;
import org.pcj.internal.network.MessageBytesOutputStream;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;

public class MpiNetworker implements NetworkerInterface {
    private NetworkerInterface baseNetworker;
    public MpiNetworker(NetworkerInterface baseNetworker) {
        this.baseNetworker = baseNetworker;
    }

    @Override
    public void send(SocketChannel socket, Message message) {
        if (socket instanceof LoopbackSocketChannel) {
            baseNetworker.send(socket, message);
            return;
        }

        MessageBytesOutputStream objectBytes = null;
        try {
            objectBytes = new MessageBytesOutputStream(message);
            objectBytes.writeMessage();
            objectBytes.close();
        } catch (Exception ex) {}

        ByteBuffer[] array = objectBytes.getByteBufferArray().getArray();

        byte[] serialized = new byte[Arrays.stream(array).mapToInt(subarray->subarray.remaining()).sum()];
        int offset = 0;
        for (ByteBuffer subarray : array) {
            int thisOffset = subarray.remaining();
            subarray.get(serialized, offset, serialized.length - offset);
            offset += thisOffset;
        }

        int physicalId = InternalPCJ.getNodeData().getSocketChannelByPhysicalId().entrySet().stream()
                .filter( entry -> entry.getValue() == socket)
                .findFirst().get().getKey();
        TestMpi.sendSerializedBytes(serialized, physicalId);
    }

    @Override
    public void processMessageBytes(SocketChannel socket, MessageBytesInputStream messageBytes) {
        baseNetworker.processMessageBytes(socket, messageBytes);
    }

    @Override
    public void startup() {
        baseNetworker.startup();
    }

    @Override
    public ServerSocketChannel bind(InetAddress hostAddress, int port, int backlog) throws IOException {
        return baseNetworker.bind(hostAddress, port, backlog);
    }

    @Override
    public SocketChannel connectTo(InetAddress hostAddress, int port) throws IOException, InterruptedException {
        return baseNetworker.connectTo(hostAddress, port);
    }

    @Override
    public void shutdown() {
        baseNetworker.shutdown();
    }
}
