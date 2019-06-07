/*
 * Copyright (c) 2011-2019, PCJ Library, Marek Nowicki
 * All rights reserved.
 *
 * Licensed under New BSD License (3-clause license).
 *
 * See the file "LICENSE" for the full license governing this code.
 */
package org.pcj.internal.message.alive;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.pcj.internal.InternalPCJ;
import org.pcj.internal.NodeData;
import org.pcj.internal.message.Message;
import org.pcj.internal.message.MessageType;
import org.pcj.internal.network.MessageDataInputStream;
import org.pcj.internal.network.MessageDataOutputStream;

/**
 * Message sent by node0 to all nodes about completed of execution.
 *
 * @author Marek Nowicki (faramir@mat.umk.pl)
 */
final public class AliveMessage extends Message {

    public AliveMessage() {
        super(MessageType.ALIVE);
    }


    @Override
    public void write(MessageDataOutputStream out) throws IOException {
    }

    @Override
    public void onReceive(SocketChannel sender, MessageDataInputStream in) throws IOException {
        NodeData nodeData = InternalPCJ.getNodeData();
        AliveState state = nodeData.getAliveState();
        state.nodeNotified(sender);
    }
}
