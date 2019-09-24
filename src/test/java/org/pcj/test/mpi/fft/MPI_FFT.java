package org.pcj.test.mpi.fft;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;

import static org.pcj.PCJ.*;

@RegisterStorage(MPI_FFT.Shared.class)
public class MPI_FFT implements StartPoint {

    private void createNodeLeaders (boolean amIaLeader) {
        if (amIaLeader) {
            createNodeLeadersCommunicator(amIaLeader);
        }
    }


    private static class Utilities {

        public static int number_of_bits (long num) {
            int n = 0;
            while (num > 0) {
                n++;
                num >>>= 1;
            }
            return n;
        }

    }

    private static class MpiCommunicator implements Runnable {
        int[] senderId = new int[] {0};
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
    public void spinReceiverThread () {
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
        PCJ.start(MPI_FFT.class, new NodesDescription(nodesFileName));
    }

    NetworkerInterface oldNetworker;
    public void initMpiSubsystem (boolean selfStart) throws InterruptedException {
        if (thisThreadIsALeader()) {
            System.loadLibrary("native");
        }
        PCJ.barrier();
        if (thisThreadIsALeader()) {
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
        boolean leader = thisThreadIsALeader();
        //System.out.format("I am thread #%d, and am I a leader? -> %b \n", PCJ.myId(), leader);
        createNodeLeaders(leader);
        createGroupForThreadsInANode();

        PCJ.barrier(); //nativeBarrier();
        if (thisThreadIsALeader()) {
            oldNetworker = PCJ.getNetworker();
            PCJ.setNetworker(new MpiNetworker(PCJ.getNetworker()));
            spinReceiverThread();
        }
        nativeBarrier();
    }
    @Override
    public void main() throws Throwable {
 //       System.out.format("Logical thread# = %d, node id = %d physical id = %d\n",
   //             PCJ.myId(), PCJ.getNodeId(), PCJ.getNodeData().getPhysicalId());
        initMpiSubsystem(false);
     //   System.out.println(String.format("I am PCJ thread %d of %d. In MPI I am thread %d of %d.",
       //         PCJ.myId(), PCJ.threadCount(), mpiRank(), mpiSize()));
        PCJ.barrier(); //nativeBarrier();
    //    System.out.println(PCJ.myId() + " finished init");
        world_size = PCJ.threadCount();
        world_logsize = Utilities.number_of_bits(world_size) - 1L;
        rank = PCJ.myId();
        int global_logsize = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("size.txt"))) {
            global_logsize = Integer.parseInt(br.readLine());
            communication = br.readLine().trim();
        }
        local_logsize = global_logsize - world_logsize;

        local_size = 1L << local_logsize;

        bitinit();

        mystart = rank * local_size;

        long time = Long.MAX_VALUE;
        long t_all_old = Long.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            PCJ.barrier();
       //     System.out.println(PCJ.myId() + " starting fft init");

            fft_init(local_size, mystart, rank);
         //   System.out.println(PCJ.myId() + " stopped fft init");

            tstart = System.nanoTime();
            debug2 = "c";
           // System.out.println(PCJ.myId() + " starting fft inner");

            fft_inner(local_size, 1L);
            //System.out.println(PCJ.myId() + " stopped fft inner");

            tend = System.nanoTime();
            PCJ.barrier();
            if (tend - tstart < time) {
                time = tend - tstart;
                t_all_old = t_all;
            }
        }

        if (DO_PRINT) {
            if (PCJ.myId() != 0L) {
                PCJ.waitFor(Shared.dummy);
            }

            for (int i = 0; i < c.length / 2; i++) {
                System.out.println("#" + PCJ.myId() + " " + c[i * 2] + " + " + c[i * 2 + 1] + "i");
            }

            if (PCJ.myId() != PCJ.threadCount() - 1) {
                PCJ.put(true, PCJ.myId() + 1, Shared.dummy);
            }
        }
        PCJ.barrier();

        fft_verif(world_size, local_size, mystart);

        PCJ.barrier();

        if (rank == 0) {
            n = local_logsize + world_logsize;
            two_n = 1L << n;
            tsec = (time) * 1e-9;

            gflops = ((5.0d * n * two_n) / tsec) * 1e-9;
            System.out.println("Elapsed time:" + tsec + " Num PEs " + +world_size + "nodes " + PCJ.getNodeCount() +" Local size: " + local_size + " GFlops = " + gflops + " communication = " + communication + " Alltoall time = " + t_all_old * 1e-9);
        }
        //       PCJ.log("#" + PCJ.myId() + " Alltoall time = " + t_all_old * 1e-9);
        System.err.flush();

       PCJ.barrier(); // nativeBarrier();

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



    public final static boolean DO_PRINT = false;

    @Storage(MPI_FFT.class)
    enum Shared {
        dummy, blocks, blocksHypercube,

            mpiPort
        }

    private boolean dummy;
    private double[][] blocks;
    private double[][][] blocksHypercube;


    private long n, two_n, world_size, world_logsize;
    private long local_size, local_logsize;
    private long tstart, tend, rate, mystart, rank;
    private double tdelta, gflops, tsec;
    private long t_all = 0L;
    private long seed = 0L;

    String debug = "";
    String debug2 = "";

    public void print_array(double[] arr, String name) {
        //       System.err.println(name);
        String out = "";
        for (int z = 0; z < arr.length / 2; z++) {
            out += "#" + PCJ.myId() + " " + name + " = " + arr[2 * z] + " " + arr[2 * z + 1] + "i\n";
        }
        System.out.println(out);
    }

    String communication = "h";


    private double[] c;
    private double[] spare;
    private double[] twiddles;

    void fft_init(long n_local_size, long local_start, long rank) {
        long i;
        twiddles = new double[(int) (n_local_size)];
        c = new double[(int) (2L * n_local_size)];
        spare = new double[(int) (2L * n_local_size)];
        t_all = 0L;

        Random random = new Random();
        seed = random.nextLong();

        initialize_data_array(n_local_size, local_start, rank, c);
    }

    private long mask64, mask32, mask16, mask8, mask4, mask2, mask1;

    public void bitinit() {
        mask64 = ~0L;
        mask32 = mask64 << 32L;
        mask16 = (mask32 >>> 16L) ^ mask32;
        mask8 = (mask16 >>> 8L) ^ mask16;
        mask4 = (mask8 >>> 4L) ^ mask8;
        mask2 = (mask4 >>> 2L) ^ mask4;
        mask1 = (mask2 >>> 1L) ^ mask2;
    }

    public long interchange(long ival, long imask, long ishift) {
        return ((ival & imask) >>> ishift) | ((ival & ~imask) << ishift);
    }

    public long i_bitreverse(long i, long n) {
        long itmp;
        long out;
        itmp = interchange(i, mask32, 32L);
        itmp = interchange(itmp, mask16, 16L);
        itmp = interchange(itmp, mask8, 8L);
        itmp = interchange(itmp, mask4, 4L);
        itmp = interchange(itmp, mask2, 2L);
        itmp = interchange(itmp, mask1, 1L);

        if (n - 64L < 0L) {
            out = itmp >>> -(n - 64L);
        } else {
            out = itmp << (n - 64L);
        }
        return out;
    }

    private void initialize_data_array(long n_local_size, long local_start, long rank, double[] buffer) {
        long i, j;
        int n;
        //int seed = 314159265;
        double h, h2;
        Random random = new Random(seed);
        for (i = 0L; i < buffer.length / 2L; i++) {
            buffer[(int) (2L * i)] = random.nextDouble();
            buffer[(int) (2L * i + 1L)] = random.nextDouble();
        }
    }

    private void fft_inner(long n_local_size, long direction) {
        long world_size, n_world_size;
        long rank, lo, hi, lstride, i, j, k;
        long levels, l, loc_comm, m, m2;
        double cer, cei, crr, cri, clr, cli; //complex
        double two_pi, angle_base;

        rank = PCJ.myId();
        world_size = PCJ.threadCount();
        n_world_size = world_size * n_local_size;
        two_pi = 2.0d * Math.PI * direction;

        levels = Utilities.number_of_bits(n_world_size) - 1L;
        loc_comm = Utilities.number_of_bits(n_local_size);
        long tt = 0L;


        long t1 = System.nanoTime();
        permute(c, n_world_size, spare);
        long t2 = System.nanoTime();

        tt += t2 - t1;

        for (i = 0L; i < n_local_size / 2L; i++) {

            double x = 0.0d;
            double y = ((i) * ((-two_pi) / n_local_size));
            spare[(int) (2L * i)] = Math.exp(x) * Math.cos(y);
            spare[(int) (2L * i + 1L)] = Math.exp(x) * Math.sin(y);
        }

        //phase 1L computation
        for (l = 1L; l < loc_comm; l++) {
            m = 1L << l;
            m2 = m >>> 1L;
            lstride = n_local_size >>> l;
            for (int zz = 0, aa = 0; zz < m2; zz++, aa += lstride) {
                twiddles[2 * zz] = spare[2 * aa];
                twiddles[2 * zz + 1] = spare[2 * aa + 1];
            }
            for (k = 0L; k < n_local_size; k += m) {
                for (j = k; j < k + m2; j++) {
                    cer = twiddles[(int) (2L * (j - k))];
                    cei = twiddles[(int) (2L * (j - k) + 1L)];

                    double wr = c[(int) (2L * (j + m2))];
                    double wi = c[(int) (2L * (j + m2) + 1L)];

                    crr = cer * wr - cei * wi;
                    cri = cer * wi + cei * wr;
                    clr = c[(int) (2L * j)];
                    cli = c[(int) (2L * j + 1L)];
                    c[(int) (2L * j)] = clr + crr;
                    c[(int) (2L * j + 1L)] = cli + cri;
                    c[(int) (2L * (j + m2))] = clr - crr;
                    c[(int) (2L * (j + m2) + 1L)] = cli - cri;
                }
            }
        }

        //System.out.println(PCJ.myId() + " starts transpose1");

        t1 = System.nanoTime();
        transpose(c, n_world_size, world_size, spare, true);
        t2 = System.nanoTime();
        //System.out.println(PCJ.myId() + " stops transpose1");

        tt += t2 - t1;

        for (l = loc_comm; l <= levels; l++) {
            m = (1L << l) / world_size;
            m2 = m >>> 1L;
            angle_base = (-two_pi) / (1L << l);
            for (k = 0L; k < n_local_size; k += m) {
                for (j = k; j < k + m2; j++) {
                    double x = 0.0d;
                    double y = (((j - k) * world_size + rank) * angle_base);
                    cer = Math.exp(x) * Math.cos(y);
                    cei = Math.exp(x) * Math.sin(y);

                    double wr = c[(int) (2L * (j + m2))];
                    double wi = c[(int) (2L * (j + m2) + 1L)];

                    crr = cer * wr - cei * wi;
                    cri = cer * wi + cei * wr;

                    clr = c[(int) (2L * j)];
                    cli = c[(int) ((2L * j) + 1L)];

                    c[(int) (2L * j)] = clr + crr;
                    c[(int) ((2L * j) + 1L)] = cli + cri;
                    c[(int) (2L * (j + m2))] = clr - crr;
                    c[(int) (2L * (j + m2) + 1L)] = cli - cri;
                }
            }
        }
    //    System.out.println(PCJ.myId() + " starts transpose2");

        t1 = System.nanoTime();
        transpose(c, n_world_size, n_local_size / world_size, spare, false);
        t2 = System.nanoTime();
      //  System.out.println(PCJ.myId() + " stops transpose2");

        tt += t2 - t1;
    }

    private void permute(double[] c, long n, double[] scratch) {
        long world_size, local_n, block_size;

        world_size = PCJ.threadCount();
        local_n = n / world_size;
        block_size = local_n / world_size;
        packf(c, scratch, local_n, world_size, 32L, 1024L, 0L, true);

        alltoall(scratch, c, block_size);
        System.arraycopy(c, 0, scratch, 0, c.length);
        permute_locally(c, scratch, local_n, 1L << 22L);

    }

    private void packf(double[] input, double[] output, long n, long p, long n_b, long cp_b, long npadding, boolean do_bitreverse) {
        long[] pe_bufstart = new long[(int) p];
        long pe_buflen, buf, p_bits, ii, i, jj, j, ooffset, ioffset, p_b;

        p_b = cp_b;

        p_bits = Utilities.number_of_bits(p - 1L);
        pe_buflen = n / p + npadding;

        if (do_bitreverse) {
            for (i = 0L; i < p; i++) {
                pe_bufstart[(int) i] = i_bitreverse(i, p_bits) * pe_buflen;
            }
        } else {
            for (i = 0L; i < p; i++) {
                pe_bufstart[(int) i] = i * pe_buflen;
            }
        }

        if (p_b > p) {
            p_b = p;
        }

        for (jj = 0L; jj < p; jj += p_b) {
            ooffset = 0L;
            for (ii = 0L; ii < n; ii += (p * n_b)) {
                for (j = jj; j < jj + p_b; j++) {
                    buf = pe_bufstart[(int) j];
                    ioffset = ooffset;
                    for (i = ii; i <= Math.min(ii + p * (n_b - 1L), n - 1L); i += p) {
                        output[(int) (2L * (buf + ioffset))] = input[(int) (2L * (i + j))];
                        output[(int) (2L * (buf + ioffset) + 1L)] = input[(int) (2L * (i + j) + 1L)];
                        ioffset++;
                    }
                }
                ooffset += n_b;
            }
        }
    }

    void alltoall(double[] source, double[] dest, long blockSize) {
        t_all = System.nanoTime();
        allToAllPerform(source, dest, blockSize);
        t_all = System.nanoTime() - t_all;
    }

    /**
     * @param source    Data to be sent to other threads - will be put into the
     *                  "blocks" shared array
     * @param blockSize
     */
    void allToAllPerform(double[] source, double[] dest, long blockSize) {
        switch (communication) {
            case "h":
                alltoallHypercube(source, dest, blockSize);
                break;
            case "b":
                PCJ.barrier();
                prepareAllToAll(blockSize, source);
                PCJ.barrier();
                allToAllBlocking(dest, blockSize);
                System.arraycopy(source, PCJ.myId() * (int) (2 * blockSize), dest, PCJ.myId() * (int) (2 * blockSize), (int) (2 * blockSize));
                break;
            case "n":
                PCJ.barrier();
                prepareAllToAll(blockSize, source);
                PCJ.barrier();
                allToAllNonBlocking(dest, blockSize);
                System.arraycopy(source, PCJ.myId() * (int) (2 * blockSize), dest, PCJ.myId() * (int) (2 * blockSize), (int) (2 * blockSize));
                break;
        }
    }

    private double[][] blocked;

    private void alltoallHypercube(double[] src, double[] dest, long blockSize) {
        PCJ.barrier();
        blocked = new double[PCJ.threadCount()][(int) (2L * blockSize)];
        for (int i = 0; i < blocked.length; i++) {
            System.arraycopy(src, i * 2 * (int) blockSize, blocked[i], 0, (int) (2 * blockSize));
        }
        //all-to-all hypercube personalized communication, per
        //http://www.sandia.gov/~sjplimp/docs/cluster06.pdf, p. 5.
        int logNumProcs = (int) (Math.log(PCJ.threadCount()) / Math.log(2.0d));
        double[][][] blocksLocal = new double[logNumProcs][][];
        PCJ.putLocal(blocksLocal, Shared.blocksHypercube);
        PCJ.barrier();
        int myId = PCJ.myId();

        double[][] toSend = new double[PCJ.threadCount() / 2][];
        for (int dimension = 0; dimension < logNumProcs; dimension++) {

            int partner = (1 << dimension) ^ myId;

            long mask = 1L << dimension;
            int j = 0;
            for (int i = 0; i < PCJ.threadCount(); i++) {
                if (partner < myId) {
                    if ((i & mask) == 0) {
                        toSend[j++] = blocked[i];
                    }
                } else {
                    if ((i & mask) != 0) {
                        toSend[j++] = blocked[i];
                    }
                }
            }

            PCJ.put(toSend, partner, Shared.blocksHypercube, dimension);

            //wait to receive
            double[][] checked = null;
            while (checked == null) {
                j = 0;
                checked = PCJ.getLocal(Shared.blocksHypercube, dimension);
                if (checked != null) {

                    PCJ.putLocal(null, Shared.blocksHypercube, dimension);
                    for (int i = 0; i < PCJ.threadCount(); i++) {
                        if (partner < myId) {
                            if ((i & mask) == 0) {
                                blocked[i] = checked[j++];
                            }
                        } else {
                            if ((i & mask) != 0) {
                                blocked[i] = checked[j++];
                            }
                        }
                    }
                }
            }
        }

        for (int i = 0; i < blocked.length; i++) {
            System.arraycopy(blocked[i], 0, dest, (int) (i * 2 * blockSize), (int) (2 * blockSize));
        }
        PCJ.barrier();
    }

    private void allToAllBlocking(double[] dest, long blockSize) {
        //algorithm inspired from http://www.pgas2013.org.uk/sites/default/files/finalpapers/Day1/H1/3_paper20.pdf
        for (int image = (PCJ.myId() + 1) % PCJ.threadCount(), num = 0; num != PCJ.threadCount() - 1; image = (image + 1) % PCJ.threadCount()) {
            double[] recv = (double[]) PCJ.get(image, Shared.blocks, PCJ.myId());

            if (recv == null) {
                System.out.println("myid = " + PCJ.myId() + " debug = " + debug + " debug2 = " + debug2);
            }
            System.arraycopy(recv, 0, dest, (int) (image * 2 * blockSize), (int) (2 * blockSize));
            num++;
        }
    }

    private void allToAllNonBlocking(double[] dest, long blockSize) {
        //prepare futures array
        PcjFuture<double[]>[] futures = new PcjFuture[PCJ.threadCount()];

        //get the data
        for (int i = 0; i < PCJ.threadCount(); i++) {
            if (i != PCJ.myId()) {
                futures[i] = PCJ.asyncGet(i, Shared.blocks, PCJ.myId());
            }
        }

        int numReceived = 0;
        while (numReceived != PCJ.threadCount() - 1) {
            for (int i = 0; i < futures.length; i++) {
                if (futures[i] != null && futures[i].isDone()) {
                    double[] recv = futures[i].get();
                    System.arraycopy(recv, 0, dest, (int) (i * 2 * blockSize), (int) (2 * blockSize));
                    numReceived++;
                    futures[i] = null;
                }
            }
        }
    }

    private void prepareAllToAll(long blockSize, double[] source) {
        //1) copy information from source to blocks temporary shared array
        blocks = new double[PCJ.threadCount()][];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new double[(int) (2 * blockSize)];
            System.arraycopy(source, (int) (2 * i * blockSize), blocks[i], 0, (int) (2 * blockSize));
        }
        PCJ.putLocal(blocks, Shared.blocks);
    }

    private void transpose(double[] c, long n, long n_transpose, double[] scratch, boolean forward) {
        long world_size = PCJ.threadCount();
        long local_n = n / world_size;
        long block_size = local_n / world_size;

        if (forward) {
            packf(c, scratch, local_n, n_transpose, 32L, 1024L, 0L, false);
            debug = "t1";
            alltoall(scratch, c, block_size);
            System.arraycopy(c, 0, scratch, 0, c.length);
        } else {
            debug = "t2";
            alltoall(c, scratch, block_size);
            System.arraycopy(scratch, 0, c, 0, scratch.length);
            packf(scratch, c, local_n, n_transpose, 32L, 1024L, 0L, false);
        }
    }

    private void fft_verif(long world_size, long n_local_size, long local_start) {
        long i, j, rank, mei;
        double norm, error, max_error, residue, logm;
        final double epsilon = 1.1e-16;
        final double max_residue = 16.0d;

        //perform inverse fft
        for (i = 0L; i < n_local_size; i++) {

            double wr = world_size * n_local_size;
            double wi = 0.0d;

            double mod = ((wr != 0.0 || wi != 0.0d) ? Math.sqrt(wr * wr + wi * wi) : 0.0d);
            double den = Math.pow(mod, 2.0d);

            double cr = c[(int) (2L * i)];
            double ci = c[(int) ((2L * i) + 1L)];

            cr = (cr * wr + ci * wi) / den;
            ci = (ci * wr - cr * wi) / den;

            c[(int) (2L * i)] = cr;
            c[(int) (2L * i + 1L)] = ci;

        }
        debug2 = "v";
        fft_inner(n_local_size, -1L);
        PCJ.barrier();
        rank = PCJ.myId();
        initialize_data_array(n_local_size, local_start, rank, spare);
        PCJ.barrier();
        max_error = -1.0d;
        mei = -1L;
        for (i = 0L; i < n_local_size; i++) {


            double er = c[(int) (2L * i)] - spare[(int) (2L * i)];
            double ei = c[(int) (2L * i + 1L)] - spare[(int) (2L * i + 1L)];
            error = Math.abs(er * er + ei * ei);
            if (error > max_error) {
                mei = i;
            }
            max_error = Math.max(max_error, error);
        }

        logm = Utilities.number_of_bits(world_size) - 1L + Utilities.number_of_bits(n_local_size) - 1L;
        residue = (max_error / epsilon) / logm;
        if (residue < max_residue && rank == 0L) {
            System.out.println("Verification successful");
        } else {
            if (residue >= max_residue) {
                System.out.println("Verification failed (residue = " + residue + ")");
                System.out.println("   Max error: " + max_error);
                System.out.println("   In: (" + c[(int) mei] + "); Out: (" + spare[(int) mei] + ")");
            }
        }
    }

    private void permute_locally(double[] dest, double[] src, long n, long cn_b) {
        long i, j, n_bits, n_b;

        n_b = cn_b;

        n_bits = Utilities.number_of_bits(n - 1L);

        if (n_b > n) {
            n_b = n;
        }

        for (j = 0L; j <= n_b; j++) {
            for (i = j; i < n; i += n_b) {
                dest[(int) (2L * i_bitreverse(i, n_bits))] = src[(int) (2L * i)];
                dest[(int) (2L * i_bitreverse(i, n_bits) + 1L)] = src[(int) (2L * i + 1L)];
            }
        }
    }


}

