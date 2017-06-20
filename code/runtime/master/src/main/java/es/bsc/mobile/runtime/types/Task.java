/*
 *  Copyright 2008-2016 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package es.bsc.mobile.runtime.types;

import java.util.concurrent.atomic.AtomicInteger;

import es.bsc.mobile.runtime.types.data.parameter.Parameter;

import es.bsc.mobile.runtime.types.data.access.DataAccess;
import es.bsc.mobile.runtime.utils.GraphNode;
import java.io.Serializable;

public class Task extends GraphNode<Task, DataAccess> implements Comparable<Task>, Serializable {

    private static final AtomicInteger NEXT_TASK_ID = new AtomicInteger(1);

    private static final byte OFFLOADED = 0x20;
    private static final byte PENDING_ANALYSIS = 0x10;
    private static final byte HAS_DEPENDENCIES = 0x08;
    private static final byte RUNNING_LOCALLY = 0x04;
    private static final byte EXECUTED = 0x02;
    private static final byte FAILED_EXECUTION = 0x01;

    private final int taskId;
    private final int processId;
    private final long threadId;
    private final TaskParameters taskParams;
    private byte state;

    public Task(String className, String methodName, boolean hasTarget, Parameter[] parameters) {
        taskId = NEXT_TASK_ID.getAndIncrement();
        //processId = android.os.Process.myPid();       
        processId = 19999;
        threadId = Thread.currentThread().getId();
        taskParams = new TaskParameters(className, methodName, hasTarget, parameters);
        state = PENDING_ANALYSIS;
    }

    public int getId() {
        return taskId;
    }

    public int getProcessId() {
        return processId;
    }

    public long getThreadId() {
        return threadId;
    }

    public TaskParameters getTaskParams() {
        return taskParams;
    }

    public byte getState() {
        return state;
    }

    public void hasBeenOffloaded() {
        state = (byte) (state | OFFLOADED);
    }

    public boolean isOffloaded() {
        return (state & OFFLOADED) == OFFLOADED;
    }

    public void dependenciesAnalysed(boolean hasDependencies) {
        state = (byte) (state & ~PENDING_ANALYSIS);
        if (hasDependencies) {
            state = (byte) (state | HAS_DEPENDENCIES);
        } else {
            state = (byte) (state & ~HAS_DEPENDENCIES);
        }
    }

    public void releasedDependencies() {
        state = (byte) (state & ~HAS_DEPENDENCIES);
    }

    public boolean isDependencyFree() {
        return (state & (PENDING_ANALYSIS | HAS_DEPENDENCIES)) == 0x00;
    }

    public void endsExecution(boolean failed) {
        if (failed) {
            state = (byte) (state | EXECUTED | FAILED_EXECUTION);
        } else {
            state = (byte) ((state | EXECUTED) & ~FAILED_EXECUTION);
        }
    }

    public boolean isExecuted() {
        return (state & (EXECUTED | FAILED_EXECUTION)) == EXECUTED;
    }

    public void runsLocal() {
        state = (byte) (state | RUNNING_LOCALLY);
    }

    public void stoppedLocally() {
        state = (byte) (state & ~RUNNING_LOCALLY);
    }

    public void endLocally(boolean failed) {
        state = (byte) ((state | EXECUTED) & ~RUNNING_LOCALLY);
        if (failed) {
            state = (byte) (state | EXECUTED | FAILED_EXECUTION);
        } else {
            state = (byte) ((state | EXECUTED) & ~FAILED_EXECUTION);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Task Id=").append(taskId);
        sb.append(" created by thread ").append(threadId);
        sb.append(" in process ").append(processId);

        if ((state & (RUNNING_LOCALLY | EXECUTED)) != 0x00) {
            if ((state & EXECUTED) != 0x00) {
                if ((state & FAILED_EXECUTION) != 0x00) {
                    sb.append(" has failed on its execution");
                } else {
                    sb.append(" has executed properly");
                }
            } else {
                sb.append(" is running in a local computing device");
            }

        } else {
            if ((state & PENDING_ANALYSIS) == PENDING_ANALYSIS) {
                sb.append(" is pending to be analysed for dependencies");
            } else if ((state & HAS_DEPENDENCIES) == HAS_DEPENDENCIES) {
                sb.append(" has still some dependencies to resolve");
            } else {
                sb.append(" is free of dependencies");
            }

            if ((state & OFFLOADED) == OFFLOADED) {
                sb.append(" and has already been offloaded ");
            }
        }

        sb.append("\n");
        sb.append(taskParams);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public int compareTo(Task o) {
        return Integer.compare(taskId, ((Task) o).taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Task)) {
            return false;
        }
        return taskId == ((Task) o).taskId;
    }

    @Override
    public int hashCode() {
        return taskId;
    }

}
