#include <mpi.h>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <iostream>
#include "org_pcj_test_mpi_TestMpi.h"
#include "mpi.h"

using namespace std;
char portName[MPI_MAX_PORT_NAME] = {};
MPI_Comm pcjCommunicator = MPI_COMM_NULL;
MPI_Comm leaderCommunicator = MPI_COMM_NULL;


//Code below comes from:
//https://stackoverflow.com/questions/24806782/mpi-merge-multiple-intercoms-into-a-single-intracomm/31148792#31148792
// The Borg routine: given
//   (1) a (quiesced) intra-communicator with one or more members, and
//   (2) a (quiesced) inter-communicator with exactly two members, one
//       of which is rank zero of the intra-communicator, and
//       the other of which is an unrelated spawned rank,
// return a new intra-communicator which is the union of both inputs.
//
// This is a collective operation.  All ranks of the intra-
// communicator, and the remote rank of the inter-communicator, must
// call this routine.  Ranks that are members of the intra-comm must
// supply the proper value for the "intra" argument, and MPI_COMM_NULL
// for the "inter" argument.  The remote inter-comm rank must
// supply MPI_COMM_NULL for the "intra" argument, and the proper value
// for the "inter" argument.  Rank zero (only) of the intra-comm must
// supply proper values for both arguments.
//
// N.B. It would make a certain amount of sense to split this into
// separate routines for the intra-communicator processes and the
// remote inter-communicator process.  The reason we don't do that is
// that, despite the relatively few lines of code,  what's going on here
// is really pretty complicated, and requires close coordination of the
// participating processes.  Putting all the code for all the processes
// into this one routine makes it easier to be sure everything "lines up"
// properly.
MPI_Comm
assimilateComm(MPI_Comm intra, MPI_Comm inter)
{
    MPI_Comm peer = MPI_COMM_NULL;
    MPI_Comm newInterComm = MPI_COMM_NULL;
    MPI_Comm newIntraComm = MPI_COMM_NULL;

    // The spawned rank will be the "high" rank in the new intra-comm
    int high = (MPI_COMM_NULL == intra) ? 1 : 0;

    // If this is one of the (two) ranks in the inter-comm,
    // create a new intra-comm from the inter-comm
    if (MPI_COMM_NULL != inter) {
        MPI_Intercomm_merge(inter, high, &peer);
    } else {
        peer = MPI_COMM_NULL;
    }

    // Create a new inter-comm between the pre-existing intra-comm
    // (all of it, not only rank zero), and the remote (spawned) rank,
    // using the just-created intra-comm as the peer communicator.
    int tag = 12345;
    if (MPI_COMM_NULL != intra) {
        // This task is a member of the pre-existing intra-comm
        MPI_Intercomm_create(intra, 0, peer, 1, tag, &newInterComm);
    }
    else {
        // This is the remote (spawned) task
        MPI_Intercomm_create(MPI_COMM_SELF, 0, peer, 0, tag, &newInterComm);
    }

    // Now convert this inter-comm into an intra-comm
    MPI_Intercomm_merge(newInterComm, high, &newIntraComm);


    // Clean up the intermediaries
    if (MPI_COMM_NULL != peer) MPI_Comm_free(&peer);
    MPI_Comm_free(&newInterComm);

    // Delete the original intra-comm
    if (MPI_COMM_NULL != intra && MPI_COMM_SELF != intra) MPI_Comm_free(&intra);

    // Return the new intra-comm
    return newIntraComm;
}

std::mutex mpiMutex;
JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_init  (JNIEnv *, jclass) {
    lock_guard<decltype(mpiMutex)> guard{mpiMutex};
    clog << "Initializing\n";
    int status;
    MPI_Initialized (&status);
    if (!status) {
        MPI_Init(NULL, NULL);
    }
}

/*
 * Class:     org_pcj_internal_mpi_Mpi
 * Method:    end
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_end (JNIEnv *, jclass) {
    lock_guard<decltype(mpiMutex)> guard{mpiMutex};

    int status;
    MPI_Finalized(&status);

    if (!status) {
        if (strlen(portName)) {
            MPI_Close_port(portName);
        }
        if (!status) {
            MPI_Finalize();
        }
    }
}

JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_sendInts (JNIEnv *env, jclass, jint src, jint dst, jintArray array) {
    jsize length = env->GetArrayLength(array);
    jint *arrayBody = env->GetIntArrayElements (array, 0);
    MPI_Send(arrayBody, length, MPI_INT, dst, 0, MPI_COMM_WORLD);
    return;
}

JNIEXPORT jintArray JNICALL Java_org_pcj_test_mpi_TestMpi_receiveInts (JNIEnv *, jclass, jint) {
    return NULL;
}

/*
 * Class:     org_pcj_internal_mpi_Mpi
 * Method:    messageAvailable
 * Signature: (Lorg/pcj/internal/mpi/Mpi/MPIStatus;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_pcj_test_mpi_TestMpi_messageAvailable (JNIEnv *env, jclass, jobject result) {
    int flag;
    MPI_Status status;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
    if (flag) {
        return JNI_TRUE;
    }   else {
        return JNI_FALSE;
    }
}

JNIEXPORT jstring JNICALL Java_org_pcj_test_mpi_TestMpi_openMpiPort   (JNIEnv *env, jclass) {
    MPI_Open_port(MPI_INFO_NULL, portName);
    std::clog << "Opening port, its name = " << portName << std::endl;
    jstring portJavaString = env->NewStringUTF(portName);
    pcjCommunicator = MPI_COMM_SELF;
    return portJavaString;
}

JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_acceptConnectionAndCreateCommunicator (JNIEnv *, jclass) {
    clog << "Server: " << (pcjCommunicator == MPI_COMM_SELF) << endl;
    MPI_Comm newInter;
    clog << "Server started accepting\n";
    MPI_Comm_accept (portName, MPI_INFO_NULL, 0, MPI_COMM_SELF, &newInter);
    clog << "Server finished accepting, starting assimilation\n";
    pcjCommunicator = assimilateComm (pcjCommunicator, newInter);
    clog << "Server finished assimilating\n";
}

JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_connectToNode0AndCreateCommunicator (JNIEnv *env, jclass, jstring port) {
    clog << "Client: " << (pcjCommunicator == MPI_COMM_SELF) << endl;
    MPI_Comm newInter;
    const char *portCString = env->GetStringUTFChars(port, 0);
        clog << "Client started connecting to " << portCString << "\n";
    MPI_Comm_connect (portCString, MPI_INFO_NULL, 0, MPI_COMM_SELF, &newInter);
        clog << "Client stopped connecting\n";
    env->ReleaseStringUTFChars (port, portCString);
        clog << "Client started assimilating\n";
    pcjCommunicator = assimilateComm(MPI_COMM_NULL, newInter);
    clog << "Client finished assimilating\n";
}

JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_prepareIntraCommunicator (JNIEnv *, jclass) {
    pcjCommunicator = assimilateComm(pcjCommunicator, MPI_COMM_NULL);
}

JNIEXPORT jint JNICALL Java_org_pcj_test_mpi_TestMpi_mpiRank (JNIEnv *, jclass) {
    int rank;
    MPI_Comm_rank (pcjCommunicator, &rank);
    return rank;
}

JNIEXPORT jint JNICALL Java_org_pcj_test_mpi_TestMpi_mpiSize (JNIEnv *, jclass) {
    int size;
    MPI_Comm_size (pcjCommunicator, &size);
    return size;
}

JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_createNodeLeadersCommunicator (JNIEnv *env, jclass, jboolean amIaLeader) {
    MPI_Comm_split(pcjCommunicator, amIaLeader, 0, &leaderCommunicator);
}



JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_mpiBarrier (JNIEnv *, jclass) {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    clog << "Before barrier (rank = ) " << rank << "\n";
    MPI_Barrier(pcjCommunicator);
    clog << "After barrier (rank = ) " << rank << "\n";
}



JNIEXPORT jboolean JNICALL Java_org_pcj_test_mpi_TestMpi_messageReady (JNIEnv *, jclass) {
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, pcjCommunicator, &flag, MPI_STATUS_IGNORE);
    if (flag != 0) {
        clog << "Found message ready for reception\n";
    }
    return flag != 0;
}



JNIEXPORT void JNICALL Java_org_pcj_test_mpi_TestMpi_sendSerializedBytes
  (JNIEnv *env, jclass, jbyteArray toSend, jint targetNode) {
    int length = env->GetArrayLength(toSend);
    jbyte *elements = env->GetByteArrayElements(toSend, 0);
    MPI_Send(elements, length, MPI_BYTE, targetNode, 0, pcjCommunicator);
    env->ReleaseByteArrayElements(toSend, elements, 0);
    clog << "Sent " << length << " bytes to node " << targetNode << "\n";
  }

/*
 * Class:     org_pcj_test_mpi_TestMpi
 * Method:    receiveSerializedBytes
 * Signature: ()[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_pcj_test_mpi_TestMpi_receiveSerializedBytes
  (JNIEnv *env, jclass) {
    MPI_Status status;
    MPI_Probe (MPI_ANY_SOURCE, MPI_ANY_TAG, pcjCommunicator, &status);
    int length;
    MPI_Get_count (&status, MPI_BYTE, &length);
    jbyteArray returnArray = env->NewByteArray(length);
    jbyte *elements = env->GetByteArrayElements(returnArray, 0);
    MPI_Recv (elements, length, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, pcjCommunicator, MPI_STATUS_IGNORE);
    env->ReleaseByteArrayElements(returnArray, elements, 0);
    return returnArray;
   }
