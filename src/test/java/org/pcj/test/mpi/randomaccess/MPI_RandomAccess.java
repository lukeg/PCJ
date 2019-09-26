package org.pcj.test.mpi.randomaccess;

import org.pcj.*;
import org.pcj.internal.*;
import org.pcj.internal.message.Message;
import org.pcj.internal.message.MessageType;
import org.pcj.internal.message.MessageValuePutRequest;
import org.pcj.internal.network.MessageBytesInputStream;
import org.pcj.internal.network.MessageBytesOutputStream;
import org.pcj.internal.network.MessageDataInputStream;
import org.pcj.internal.network.MessageDataOutputStream;
import org.pcj.test.mpi.MpiNetworker;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.logging.Level;

import static org.pcj.PCJ.*;

@RegisterStorage(MPI_RandomAccess.Shared.class)
public class MPI_RandomAccess implements StartPoint {

    private void createNodeLeaders(boolean amIaLeader) {
        if (amIaLeader) {
            createNodeLeadersCommunicator(amIaLeader);
        }
    }

    private static class MpiCommunicator implements Runnable {
        int[] senderId = new int[]{0};

        @Override
        public void run() {
            //System.out.println("Thread runnin'");
            mpiBarrier();
            while (!Thread.interrupted()) {
                testExistingRequests();
                if (messageReady()) {
                    //      System.out.format ("PCJ thread #%d found message ready for reception%n", PCJ.myId());

                    byte[] received = receiveSerializedBytes(senderId);
                    //    System.out.format("PCJ thread #%d received %d bytes%n", PCJ.myId(), received.length);
                    SocketChannel socket = InternalPCJ.getNodeData().getSocketChannelByPhysicalId().get(senderId[0]);
                    deserializeBytesAndPutLocally(socket, received);
                }
            }
        }

        private void deserializeBytesAndPutLocally(SocketChannel socket, byte[] received) {
            MessageBytesInputStream messageBytesInputStream = new MessageBytesInputStream();
            ByteBuffer receivedByteBuffer = ByteBuffer.wrap(received);
            messageBytesInputStream.offerNextBytes(receivedByteBuffer);

            MessageDataInputStream messageDataInputStream = messageBytesInputStream.getMessageDataInputStream();
            NetworkerInterface networker = PCJ.getNetworker();
            networker.processMessageBytes(socket, messageBytesInputStream);
        }
    }


    Thread mpiReceiverThread = new Thread(new MpiCommunicator());

    public void spinReceiverThread() {
        if (thisThreadIsALeader()) {
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

    private boolean thisThreadIsALeader() {
        if (amIaLeader == null) {
            Map<Integer, Integer> leaders = findLeadersOfNodes();
            int myId = PCJ.myId();
            amIaLeader = leaders.values().contains(myId);
        }
        return amIaLeader;
    }


    public void establishNode0Connections(String portName) {
        Map<Integer, Integer> leaders = findLeadersOfNodes();
//        if (PCJ.myId() == 0)
        //          for (int key : leaders.keySet()) {
        //            System.out.println(key + " " + leaders.get(key));
        //      }
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
    }

    private void createGroupForThreadsInANode() {
        PCJ.barrier();
        int myNode = PCJ.getNodeData().getPhysicalId();
        myNodeGroup = PCJ.join(String.format("node-%d", myNode));
        PCJ.barrier();
    }

    private Group myNodeGroup;
    String mpiPort;

    int testValue;

    public static void main(String[] args) throws IOException {
        String nodesFileName = "nodes.txt";
        if (args.length > 0) {
            nodesFileName = args[0];

        }
        PCJ.start(MPI_RandomAccess.class, new NodesDescription(nodesFileName));
    }



    NetworkerInterface oldNetworker;

    public void initMpiSubsystem(boolean selfStart) throws InterruptedException {
        boolean leader = thisThreadIsALeader();
        int myId = PCJ.myId();
        //System.out.println(myId + " staring loalibrary");
        //for (int thread = 0; thread < PCJ.threadCount(); thread++) {
        //System.out.println(myId + " inside loop thread = " + thread);

        if (leader) { // && thread == myId) {
            //      System.out.println(myId + " loading loalibrary");
            Runtime.getRuntime().loadLibrary("native");
        }
        // System.out.println(myId + " after condition");
        //  PCJ.barrier();
        //}
        //System.out.println(myId + " after loalibrary");

        PCJ.barrier();
        if (leader) {
            init();
        }
        PCJ.barrier();
        if (selfStart == true) {
            if (PCJ.myId() == 0) {
                String pcjMpiPort = openMpiPort();
                PCJ.broadcast(pcjMpiPort, Shared.mpiPort);
            }
            PCJ.waitFor(Shared.mpiPort);
            establishNode0Connections(PCJ.getLocal(Shared.mpiPort));
        } else {
            if (this.thisThreadIsALeader()) {
                PCJ.renumberProcessForPCJ(PCJ.myId());
            }
        }
        PCJ.barrier();
        //System.out.format("I am thread #%d, and am I a leader? -> %b \n", PCJ.myId(), leader);
        createNodeLeaders(leader);
        createGroupForThreadsInANode();

        PCJ.barrier(); //nativeBarrier();
        if (leader) {
            oldNetworker = PCJ.getNetworker();
            PCJ.setNetworker(new MpiNetworker(PCJ.getNetworker()));
            spinReceiverThread();
        }
        nativeBarrier();
    }

    int logN;
    int globalN;
    int logLocalN;
    int localN;
    int logNumProcs;
    long localUpdates;
    int threadCount;
    int myId;

    RandomForRA random;

    long timeBoundSeconds = Long.MAX_VALUE;
    boolean shutDown = false;
    int preparedLocally = 0;


    @Override
    public void main() throws Throwable {
        //       System.out.format("Logical thread# = %d, node id = %d physical id = %d\n",
        //             PCJ.myId(), PCJ.getNodeId(), PCJ.getNodeData().getPhysicalId());
        initMpiSubsystem(false);
        //   System.out.println(String.format("I am PCJ thread %d of %d. In MPI I am thread %d of %d.",
        //         PCJ.myId(), PCJ.threadCount(), mpiRank(), mpiSize()));
        PCJ.barrier(); //nativeBarrier();
        //      System.out.println(PCJ.myId() + " finished init");

        String[] rounds = {"Warmup", "After warmup"};
        for (String round : rounds) {

            System.out.println(round + " round");

            initializeData();
            PCJ.barrier();
            double start = System.currentTimeMillis();
            performRandomAccess();
            double stop = System.currentTimeMillis();

            PCJ.barrier();
            PCJ.putLocal(this.preparedLocally, Shared.executed);
            PCJ.waitFor(Shared.executed);
            System.out.println("Starting verification");
            verifyResultsLocally();
            thread0VerifyAll();
            if (myId == 0) {
                double seconds = (stop - start) * 1e-3;
                int updatesPerformedGlobally = 0;
                for (int PE = 0; PE < PCJ.threadCount(); PE++) {
                    updatesPerformedGlobally += (int) PCJ.get(PE, Shared.executed);
                }
                double gups = updatesPerformedGlobally * 1e-9 / seconds;
                System.out.println("Time: " + seconds + " s, global size = " + this.globalN + " updates = " + updatesPerformedGlobally + ", performance: " + gups);
            }
            PCJ.barrier();

        }




        //let's be brutal...
        System.exit(0);
        //  finalizeMpiInfrastructure();
    }

    public void nativeBarrier() {
        myNodeGroup.asyncBarrier().get();
        if (thisThreadIsALeader()) {
            mpiBarrier();
        }
        myNodeGroup.asyncBarrier().get();
    }

    private void finalizeMpiInfrastructure() throws InterruptedException {
        PCJ.barrier();
        if (thisThreadIsALeader()) {
            setNetworker(oldNetworker);
            if (mpiReceiverThread.isAlive()) {
                mpiReceiverThread.interrupt();
                try {
                    mpiReceiverThread.join();
                } catch (InterruptedException e) {
                }
            }
            end();
        } else {
            mpiReceiverThread.join();
        }
        PCJ.barrier();
    }



    @Storage(MPI_RandomAccess.class)
    enum Shared {
        mpiPort,

        table, receivedUpdates, test, okCells, executed, updatesShared
    }
    private long[] table;
    private ArrayList<Long>[][] receivedUpdates;
    Integer test;
    private int okCells;
    private int executed;
    List<Long> updatesShared[];

    public static final int BUFFERED_UPDATES = 1024;
    public static final long POISON_PILL = -1;



    private void initializeData() throws FileNotFoundException {

        try (Scanner in = new Scanner(new File("ra.config"))) {
            logN = in.nextInt();
            if (in.hasNextLong()) {
                this.timeBoundSeconds = in.nextLong();
            }
        }
        threadCount = PCJ.threadCount();
        myId = PCJ.myId();
        logNumProcs = (int) (Math.log(threadCount) / Math.log(2));
        logLocalN = logN - logNumProcs;
        localN = 1 << logLocalN;
        globalN = localN * threadCount;
        localUpdates = 4 * localN;
        long[] table = new long[localN];
        for (int i = 0; i < localN; i++) {
            table[i] = i + myId * localN;
        }

        PCJ.putLocal(table, Shared.table);
        PCJ.putLocal(0, Shared.executed);
        PCJ.putLocal(0, Shared.okCells);
        PCJ.putLocal(new ArrayList<?>[(int) (localUpdates / BUFFERED_UPDATES)][logNumProcs + 1], Shared.receivedUpdates);

        PCJ.monitor(Shared.receivedUpdates);
        PCJ.monitor(Shared.executed);
        PCJ.monitor(Shared.okCells);
        PCJ.monitor(Shared.table);
        random = new RandomForRA(myId);
        this.preparedLocally = 0;
        this.shutDown = false;
    }

    private int randomNumberToLocalPosition(long rand) {
        return (int) (rand & (localN - 1));
    }

    private List<Long> generateRemoteUpdates(int update, int CHUNK_SIZE) {
        List<Long> numbers = new ArrayList<>();

        if (isTimeBound() && shutDown) {
            return numbers;
        }

        for (int k = 0; k < CHUNK_SIZE && update + k < localUpdates; k++) {
            long rand = random.nextLong();
            numbers.add(rand);
        }

        preparedLocally += numbers.size();
        return numbers;
    }

    boolean isTimeBound() {
        return timeBoundSeconds != Long.MAX_VALUE;
    }

    private void performRandomAccess() {
        long timeBoundStart = System.currentTimeMillis();
        int iter = 0;
        for (int update = 0; update < localUpdates; update += BUFFERED_UPDATES) {
            List<Long> updateList = generateRemoteUpdates(update, BUFFERED_UPDATES);

            if (isTimeBound()) {
                if (PCJ.myId() == 0) {
                    shutDown = System.currentTimeMillis() - timeBoundStart > timeBoundSeconds * 1e3;
                }
            }

            updateList = alltoallHypercube(updateList, iter++);
           // updateList = alltoallBlocking(updateList, iter++);
            //updateList = allToAllNonBlocking(updateList, iter++);
            performUpdates(updateList);

            if (isTimeBound()) {
                if (shutDown) {
                    System.out.println("Shutting down");

                    break;
                }
            }
        }
    }

    private List<Long> prepareBlocks(List<Long> updates) {
        Iterator<Long> iter = updates.iterator();
        List<Long>[] updatesToAdd = (List<Long>[]) new ArrayList[PCJ.threadCount()];
        for (int i = 0; i < updatesToAdd.length; i++) {
            updatesToAdd[i] = new ArrayList<>();
        }
        while (iter.hasNext()) {
            long val = iter.next();
            int PENumber = whichPE(val);
            if (PENumber != PCJ.myId()) {
                updatesToAdd[PENumber].add(val);
                iter.remove();
            }
        }
        if (isTimeBound()) {
            if (shutDown) {
                for (int i = 0; i < updatesToAdd.length; i++) {
                    updatesToAdd[i].add(POISON_PILL);
                }
            }
        }
        PCJ.putLocal(updatesToAdd, Shared.updatesShared);
        return updates;
    }

    private List<Long> allToAllNonBlocking(List<Long> updates, int iterNo) {

        PCJ.barrier();
        updates = prepareBlocks(updates);
        PCJ.barrier();
        //prepare futures array
        PcjFuture<List<Long>>[] futures = new PcjFuture[PCJ.threadCount()];

        //get the data
        for (int image = (PCJ.myId() + 1) % PCJ.threadCount(), num = 0; num != PCJ.threadCount() - 1; image = (image + 1) % PCJ.threadCount()) {
            if (image != PCJ.myId()) {
                futures[image] = PCJ.asyncGet(image, Shared.updatesShared, PCJ.myId());
            }
            num++;
        }

        int numReceived = 0;
        while (numReceived != PCJ.threadCount() - 1) {
            for (int i = 0; i < futures.length; i++) {
                if (futures[i] != null && futures[i].isDone()) {
                    List<Long> recv = futures[i].get();
                    updates.addAll(recv);
                    numReceived++;
                    futures[i] = null;
                }
            }
        }
        return updates;
    }

    private List<Long> alltoallBlocking(List<Long> updates, int iterNo) {
        updates = prepareBlocks(updates);
        PCJ.barrier();
        for (int image = (PCJ.myId() + 1) % PCJ.threadCount(), num = 0; num != PCJ.threadCount() - 1; image = (image + 1) % PCJ.threadCount()) {
            List<Long> recv = (List<Long>) PCJ.get(image, Shared.updatesShared, PCJ.myId());
            updates.addAll(recv);
            num++;
        }
        PCJ.barrier();
        return updates;
    }

    private List<Long> alltoallHypercube(List<Long> updates, int iterNo) {
        //all-to-all hypercube personalized communication, per
        //http://www.sandia.gov/~sjplimp/docs/cluster06.pdf, p. 5.
        List<Long> send = new ArrayList<>();
        List<Long> keep = new ArrayList<>();

        for (int dimension = 0; dimension < logNumProcs; dimension++) {

            int partner = (1 << dimension) ^ myId;

            long mask = 1L << (logLocalN + dimension);

            prepareUpdateLists(partner, updates, keep, send, mask);
            sendListToPartner(send, partner, iterNo, dimension);
            List<Long> received = receiveUpdates(iterNo, dimension);
            updates = keep;
            updates.addAll(received);
            keep = new ArrayList<>();
        }
        //List<Long> received = receiveUpdates(logNumProcs);
        //updates.addAll(received);
        return updates;
    }

    private void sendListToPartner(List<Long> send, int partner, int iterNo, int dimension) throws ClassCastException {
        PCJ.put(send, partner, Shared.receivedUpdates, iterNo, dimension);
        send.clear();
    }

    private List<Long> receiveUpdates(int iterNo, int dimension) {
        List<Long> received = null;
        while (received == null) {
            received = PCJ.getLocal(Shared.receivedUpdates, iterNo, dimension);
        }
        PCJ.putLocal(null, Shared.receivedUpdates, iterNo, dimension);
        return received;
    }

    private void prepareUpdateLists(int partner, List<Long> updates1, List<Long> keep, List<Long> send, long mask) {
        if (partner > myId) {
            for (long update : updates1) {

                if (update == POISON_PILL) {
                    shutDown = true;
                    continue;
                }
                if ((update & mask) != 0) {
                    send.add(update);
                } else {
                    keep.add(update);
                }
            }
        } else {
            for (long update : updates1) {

                if (update == POISON_PILL) {
                    shutDown = true;
                    continue;
                }

                if ((update & mask) != 0) {
                    keep.add(update);
                } else {
                    send.add(update);
                }
            }
        }

        if (shutDown) {
            keep.add(POISON_PILL);
            send.add(POISON_PILL);
        }
    }

    int whichPE(long pos) {
        return (int) (pos >> logLocalN) & (threadCount - 1);
    }

    private void performUpdates(List<Long> updateList) {
        for (long update : updateList) {
            if (update == POISON_PILL) {
                shutDown = true;
            } else {
                updateSingleCell(update);
            }
        }
    }

    private void updateSingleCell(long update) throws ClassCastException {
        int localOffset = randomNumberToLocalPosition(update);
        long val = PCJ.getLocal(Shared.table, localOffset);
        val ^= update;
        PCJ.putLocal(val, Shared.table, localOffset);
    }

    private void verifyResultsLocally() {
        PCJ.barrier();

        for (int PE = 0; PE < threadCount; PE++) {
            RandomForRA random = new RandomForRA(PE);

            int executedRemotely = PCJ.get(PE, Shared.executed);
            for (int update = 0; update < executedRemotely; update++) {
                long val = random.nextLong();
                if (whichPE(val) == myId) {

                    updateSingleCell(val);
                }
            }
        }

        long[] table = PCJ.getLocal(Shared.table);
        int ok = 0;
        for (int i = 0; i < table.length; i++) {
            if (table[i] == i + myId * localN) {
                ok++;
            } else {
                //PCJ.log("Cell #" + i + " is " + table[i] + ", should be: " + (i + myId * localN));
            }
        }
        PCJ.putLocal(ok, Shared.okCells);
        PCJ.waitFor(Shared.okCells);
        PCJ.barrier();
    }

    private void thread0VerifyAll() {
        if (myId == 0) {
            for (int i = 0; i < threadCount; i++) {
                int remoteOk = PCJ.get(i, Shared.okCells);
                if (remoteOk != localN) {
                    System.out.println("Verification failed for thread #" + i);
                }
            }
        }
    }

    private static class RandomForRA {

        public static final long POLY = 0x0000000000000007;
        public static final long PERIOD = 1317624576693539401L;
        private long rand;

        public RandomForRA(int initialVal) {
            long n = initialVal * 4;
            int i, j;
            /* unsigned */ long[] m2 = new /* unsigned */ long[64];
            /* unsigned */ long temp, ran;

            while (n < 0) {
                n += PERIOD;
            }
            while (n > PERIOD) {
                n -= PERIOD;
            }
            if (n == 0) {
                rand = 0x1;
            }

            temp = 0x1;
            for (i = 0; i < 64; i++) {
                m2[i] = temp;
                temp = (temp << 1) ^ ((long) temp < 0 ? POLY : 0);
                temp = (temp << 1) ^ ((long) temp < 0 ? POLY : 0);
            }

            for (i = 62; i >= 0; i--) {
                if (((n >> i) & 1) == 1) {
                    break;
                }
            }

            ran = 0x2;
            while (i > 0) {
                temp = 0;
                for (j = 0; j < 64; j++) {
                    if (((ran >> j) & 1) == 1) {
                        temp ^= m2[j];
                    }
                }
                ran = temp;
                i -= 1;
                if (((n >> i) & 1) == 1) {
                    ran = (ran << 1) ^ ((long) ran < 0 ? POLY : 0);
                }
            }

            rand = ran;
        }

        public long nextLong() {

            rand = (rand << 1) ^ ((long) rand < 0L ? POLY : 0L);
            // System.out.println(rand);
            return rand;
        }
    }

}

