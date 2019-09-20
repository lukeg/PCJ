package org.pcj.internal;

import org.pcj.internal.message.Message;
import org.pcj.internal.network.MessageBytesInputStream;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public interface NetworkerInterface {
    void startup();

    ServerSocketChannel bind(InetAddress hostAddress, int port, int backlog) throws IOException;

    SocketChannel connectTo(InetAddress hostAddress, int port) throws IOException, InterruptedException;

    default void waitForConnectionEstablished(SocketChannel socket) throws InterruptedException, IOException {
        synchronized (socket) {
            while (socket.isConnected() == false) {
                if (socket.isConnectionPending() == false) {
                    throw new IOException("Unable to connect to " + socket.getRemoteAddress());
                }
                socket.wait(100);
            }
        }
    }

    void shutdown();

    void send(SocketChannel socket, Message message);

    void processMessageBytes(SocketChannel socket, MessageBytesInputStream messageBytes);
}
