/*
 * Copyright (c) 2011-2019, PCJ Library, Marek Nowicki
 * All rights reserved.
 *
 * Licensed under New BSD License (3-clause license).
 *
 * See the file "LICENSE" for the full license governing this code.
 */
package org.pcj;

/**
 * Class that represents group of PCJ Threads.
 *
 * @author Marek Nowicki (faramir@mat.umk.pl)
 */
public interface Group {

    /**
     * Gets identifier of current PCJ Thread in the group. Identifiers are consecutive numbers that start with 0.
     *
     * @return current PCJ Thread identifier
     */
    int myId();

    /**
     * Gets total number of PCJ Thread in the group.
     *
     * @return total number of PCJ Thread in the group
     */
    int threadCount();

    /**
     * Gets group name. Global group name is empty string {@code ""}.
     *
     * @return name of the group
     */
    String getName();

    /**
     * Starts asynchronos barrier. After starting barrier the PcjFuture is returned.
     * <p>
     * PCJ Thread can continue to work and can check returned PcjFuture to find if every thread arrive at this barrier
     * using {@link PcjFuture#isDone()} method.
     * PcjFuture returns null when completed.
     *
     * @return {@link org.pcj.PcjFuture}&lt;{@link java.lang.Void}&gt; to check barrier state
     */
    PcjFuture<Void> asyncBarrier();

    /**
     * Starts asynchronous barrier with one peer PCJ Thread.
     * Given threadId should be different from current PCJ Thread id, otherwise the exception is thrown.
     * <p>
     * PCJ Thread can continue to work and can check returned PcjFuture to find if every thread done this barrier
     * using {@link PcjFuture#isDone()} method.
     * PcjFuture returns null when completed.
     *
     * @param threadId current group PCJ Thread
     * @return {@link org.pcj.PcjFuture}&lt;{@link java.lang.Void}&gt; to check barrier state
     */
    PcjFuture<Void> asyncBarrier(int threadId);

    /**
     * Asynchronous get operation. Gets value of shared variable from PCJ Thread from the group.
     *
     * @param <T>      type of value
     * @param threadId peer PCJ Thread
     * @param variable variable name
     * @param indices  (optional) indices for array variable
     * @return {@link org.pcj.PcjFuture} that will contain shared variable value
     */
    <T> PcjFuture<T> asyncGet(int threadId, Enum<?> variable, int... indices);

    /**
     * Asynchronous collect operation. Gets value of shared variable from all PCJ Threads from the group.
     *
     * @param <T>      type of value
     * @param variable variable name
     * @param indices  (optional) indices for array variable
     * @return {@link org.pcj.PcjFuture} that will contain shared variable value
     */
    <T> PcjFuture<T> asyncCollect(Enum<?> variable, int... indices);

    /**
     * Asynchronous reduce operation. Reduce value of shared variable from all PCJ Threads from the group.
     *
     * @param <T>      type of value
     * @param function reduce function
     * @param variable variable name
     * @param indices  (optional) indices for array variable
     * @return {@link org.pcj.PcjFuture} that will contain shared variable value
     */
    <T> PcjFuture<T> asyncReduce(ReduceOperation<T> function, Enum<?> variable, int... indices);

    /**
     * Asynchronous put operation. Puts value into shared variable to PCJ Thread from the group.
     * PcjFuture returns null when completed.
     *
     * @param <T>      type of value
     * @param newValue new variable value
     * @param threadId peer PCJ Thread
     * @param variable variable name
     * @param indices  (optional) indices for array variable
     * @return {@link org.pcj.PcjFuture}&lt;{@link java.lang.Void}&gt;
     */
    <T> PcjFuture<Void> asyncPut(T newValue, int threadId, Enum<?> variable, int... indices);

    /**
     * Asynchronous accumulate operation. Accumulate value into shared variable to PCJ thread from the group.
     * PcjFuture returns null when completed.
     *
     * @param <T>      type of value
     * @param function reduce function
     * @param newValue new variable value
     * @param threadId peer PCJ Thread
     * @param variable variable name
     * @param indices  (optional) indices for array variable
     * @return {@link org.pcj.PcjFuture}&lt;{@link java.lang.Void}&gt;
     */
    <T> PcjFuture<Void> asyncAccumulate(ReduceOperation<T> function, T newValue, int threadId, Enum<?> variable, int... indices);

    /**
     * Asynchronous execution operation. Executes associated function on specified thread and returns value.
     *
     * @param <T>       type of returned value
     * @param threadId  peer PCJ Thread
     * @param asyncTask function to be executed
     * @return {@link org.pcj.PcjFuture} that will contain value returned by the function
     */
    <T> PcjFuture<T> asyncAt(int threadId, AsyncTask<T> asyncTask);

    /**
     * Asynchronous broadcast operation. Broadcasts value into shared variable of all PCJ Threads from the group.
     * PcjFuture returns null when completed.
     *
     * @param <T>      type of value
     * @param newValue new variable value
     * @param variable variable name
     * @param indices  (optional) indices for array variable
     * @return {@link org.pcj.PcjFuture}&lt;{@link java.lang.Void}&gt;
     */
    <T> PcjFuture<Void> asyncBroadcast(T newValue, Enum<?> variable, int... indices);

    /**
     * This function will be removed.
     *
     * @return name of the group
     * @deprecated use {@link #getName()} instead
     */
    @Deprecated(forRemoval = true)
    default String getGroupName() {
        return getName();
    }
}
