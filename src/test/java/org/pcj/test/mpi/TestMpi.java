package org.pcj.test.mpi;

import org.pcj.*;
import org.pcj.internal.*;
import org.pcj.internal.message.Message;
import org.pcj.internal.message.MessageType;
import org.pcj.internal.message.MessageValuePutRequest;
import org.pcj.internal.network.MessageBytesInputStream;
import org.pcj.internal.network.MessageBytesOutputStream;
import org.pcj.internal.network.MessageDataInputStream;
import org.pcj.internal.network.MessageDataOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@RegisterStorage(TestMpi.Shared.class)
public class TestMpi implements StartPoint {

/*    private static class MpiMessageValuePutRequest  extends Message {

        private int requestNum;
        private int groupId;
        private int requesterThreadId;
        private int threadId;
        private String sharedEnumClassName;
        private String name;
        private int[] indices;
        private Object newValue;

        public MpiMessageValuePutRequest() {
            super(MessageType.VALUE_PUT_REQUEST);
        }

        public MpiMessageValuePutRequest(int groupId, int requestNum, int requesterThreadId, int threadId, String storageName, String name, int[] indices, Object newValue) {
            this();

            this.groupId = groupId;
            this.requestNum = requestNum;
            this.requesterThreadId = requesterThreadId;
            this.threadId = threadId;
            this.sharedEnumClassName = storageName;
            this.name = name;
            this.indices = indices;
            this.newValue = newValue;
        }

        @Override
        public void write(MessageDataOutputStream out) throws IOException {
            out.writeInt(groupId);
            out.writeInt(requestNum);
            out.writeInt(requesterThreadId);
            out.writeInt(threadId);
            out.writeString(sharedEnumClassName);
            out.writeString(name);
            out.writeIntArray(indices);
            out.writeObject(newValue);
        }

        @Override
        public void execute(SocketChannel sender, MessageDataInputStream in) throws IOException {
            groupId = in.readInt();
            requestNum = in.readInt();
            requesterThreadId = in.readInt();
            threadId = in.readInt();
            sharedEnumClassName = in.readString();
            name = in.readString();
            indices = in.readIntArray();

            NodeData nodeData = InternalPCJ.getNodeData();
            int globalThreadId = nodeData.getGroupById(groupId).getGlobalThreadId(threadId);
            PcjThread pcjThread = nodeData.getPcjThread(globalThreadId);
            InternalStorages storage = (InternalStorages) pcjThread.getThreadData().getStorages();

            try {
                newValue = in.readObject();
                storage.put(newValue, sharedEnumClassName, name, indices);
            } catch (Exception ex) {
            }

        }
    }*/
    private static class MPIStatus {
        public int count;
        public int MPI_SOURCE;
        public int MPI_TAG;
    }

    public static native void init();

    public static native void end();

    public static native void sendInts(int src, int dest, int[] array);

    public static native int[] receiveInts(int src);

    public static  native String openMpiPort();

    public static native void acceptConnectionAndCreateCommunicator();

    public static native void connectToNode0AndCreateCommunicator(String portName);

    public static native void prepareIntraCommunicator();

    public static native int mpiRank();

    public static native int mpiSize();

    public static native void createNodeLeadersCommunicator (boolean amIaLeader);

    public static native void mpiBarrier ();

    public static native boolean messageReady ();

    public static native void sendSerializedBytes (byte[] bytes, int target);

    public static native byte[] receiveSerializedBytes ();

    private void createNodeLeaders (boolean amIaLeader) {
        if (amIaLeader) {
            createNodeLeadersCommunicator(amIaLeader);
        }
    }
    enum Tags {
        PUT_LONG_ARRAY,
        GET_LONG_ARRAY,
        PUT_SERIALIZED_BYTES,
        GET_SERIALIZED_BYTES
    }

    private static final class GetPutMessageLong {
        public int tag;
        public int[] indices;
        public long[] data;
    }

    private static class MpiCommunicator implements Runnable {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                if (messageReady()) {
                    System.out.format ("PCJ thread #%d found message ready for reception%n", PCJ.myId());
                    byte[] received = receiveSerializedBytes();
                    System.out.format("PCJ thread #%d received %d bytes%n", PCJ.myId(), received.length);
                    //following call causes NPE, commentig it out - causes freeze
                    deserializeBytesAndPutLocally(received);
                }
            }
        }

        private void deserializeBytesAndPutLocally(byte[] received) {
            MessageBytesInputStream messageBytesInputStream = new MessageBytesInputStream();
            ByteBuffer receivedByteBuffer = ByteBuffer.wrap(received);
            messageBytesInputStream.offerNextBytes(receivedByteBuffer);

            MessageDataInputStream messageDataInputStream = messageBytesInputStream.getMessageDataInputStream();
            Networker networker = PCJ.getNetworker(); 
            networker.processMessageBytes(null, messageBytesInputStream);
            /*Message message;
            try {
                byte messageType = messageDataInputStream.readByte();
                message = MessageType.valueOf(messageType).create();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "Received message {0} from {1}", new Object[]{message.getType(), socket});
            }

            workers.submit(new Networker.WorkerTask(socket, message, messageDataInputStream));
            NodeData nodeData = PCJ.getNodeData();
            int globalThreadId = nodeData.getGroupById(groupId).getGlobalThreadId(threadId);
            PcjThread pcjThread = nodeData.getPcjThread(globalThreadId);
            InternalStorages storage = (InternalStorages) pcjThread.getThreadData().getStorages();
*/
        }
    }




    Thread mpiReceiverThread = new Thread(new MpiCommunicator());
    public void spinReceiverThread () {
        if (findLeadersOfNodes().containsValue(PCJ.myId())) {
            mpiReceiverThread.start();
        }
    }

    private HashMap<Integer, Integer> leadersOfNodes = null;
    public Map<Integer, Integer> findLeadersOfNodes() {
        if (leadersOfNodes == null) {
            leadersOfNodes = new HashMap<>();
            for (int thread = 0; thread < PCJ.threadCount(); thread++) {
                leadersOfNodes.putIfAbsent(PCJ.getNodeData().getPhysicalId(thread), thread);
            }
        }
        return leadersOfNodes;
    }

    private Boolean amIaLeader = null;
    private boolean thisThreadIsALeader () {
        if (amIaLeader == null) {
            Map<Integer, Integer> leaders = findLeadersOfNodes();
            int myId = PCJ.myId();
            amIaLeader = leaders.values().contains(myId);
        }
        return  amIaLeader;
    }


    public void establishNode0Connections(String portName) {
        Map<Integer, Integer> leaders = findLeadersOfNodes();
        if (PCJ.myId() == 0)
            for (int key : leaders.keySet()) {
                System.out.println(key + " " + leaders.get(key));
            }
        for (int node = 1; node < leaders.size(); node++) {
            if (PCJ.myId() == 0) {
                acceptConnectionAndCreateCommunicator();
            } else {
                if (PCJ.myId() == leaders.get(node)) {
                    connectToNode0AndCreateCommunicator(portName);
                } else if (PCJ.myId() < node && leaders.containsValue(PCJ.myId())) {
                    prepareIntraCommunicator();
                }
            }
            PCJ.barrier();
        }
        boolean leader = thisThreadIsALeader();
        System.out.format("I am thread #%d, and am I a leader? -> %b \n", PCJ.myId(), leader);
        createNodeLeaders(leader);
        createGroupForThreadsInANode();
    }

    private void createGroupForThreadsInANode() {
        PCJ.barrier();
        int myNode = PCJ.getNodeData().getPhysicalId();
        myNodeGroup = PCJ.join(String.format("node-%d", myNode));
        PCJ.barrier();
    }

    private Group myNodeGroup;
    @Storage(TestMpi.class)
    enum Shared {
        mpiPort,
        testValue
    }
    String mpiPort;

    int testValue;
    public static void main(String[] args) throws IOException {
        PCJ.deploy(TestMpi.class, new NodesDescription(new String[]{"localhost", "localhost", "localhost:8099"}));

                /*(new String[]{"localhost:8093", "localhost:8223",
        "localhost:8088", "localhost:8095", "localhost", "localhost"}));*/
    }

    public void initMpiSubsystem () {
        System.loadLibrary("native");
        init();
        if (PCJ.myId() == 0) {
            String pcjMpiPort = openMpiPort();
            PCJ.broadcast(pcjMpiPort, Shared.mpiPort);
        }
        PCJ.waitFor(Shared.mpiPort);
        establishNode0Connections(PCJ.getLocal(Shared.mpiPort));
        spinReceiverThread();
        PCJ.barrier();
    }
    @Override
    public void main() throws Throwable {
        System.out.format("Logical thread# = %d, node id = %d physical id = %d\n",
                PCJ.myId(), PCJ.getNodeId(), PCJ.getNodeData().getPhysicalId());
        initMpiSubsystem();
        System.out.println(String.format("I am PCJ thread %d of %d. In MPI I am thread %d of %d.",
                PCJ.myId(), PCJ.threadCount(), mpiRank(), mpiSize()));
        nativeBarrier();

        //PcjThread pcjThread = PCJ.getNodeData().getPcjThread(PCJ.myId());
        //InternalStorages storage = (InternalStorages) pcjThread.getThreadData().getStorages();


        if (PCJ.myId() == 0) {
            put(42, 2, Shared.testValue);
        } else if (PCJ.myId() == 2) {
            //  PCJ.waitFor(Shared.testValue);
            System.out.format("testValue = %d\n", testValue);
        };
        nativeBarrier();
        finalizeMpiInfrastructure();
    }

    public void nativeBarrier() {
        myNodeGroup.asyncBarrier().get();
        if (thisThreadIsALeader()) {
            mpiBarrier();
        }
    }
    private void finalizeMpiInfrastructure() {
        if (mpiReceiverThread.isAlive()) {
            mpiReceiverThread.interrupt();
            try {
                mpiReceiverThread.join();
            } catch (InterruptedException e) {
            }
        }
        end();
    }

    public <T> void put (T newValue, int threadId, Enum<?> variable, int... indices) {
        MessageValuePutRequest req = new MessageValuePutRequest(0, 0, 0, PCJ.myId(),
                variable.getDeclaringClass().getName(), variable.name(),
                indices, newValue);

        MessageBytesOutputStream objectBytes = null;
        try {
            objectBytes = new MessageBytesOutputStream(req);
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
        sendSerializedBytes(serialized, PCJ.getNodeData().getPhysicalId(threadId));
    }
}

