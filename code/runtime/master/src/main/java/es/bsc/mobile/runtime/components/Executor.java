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
package es.bsc.mobile.runtime.components;

import es.bsc.comm.Node;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.requests.executor.EndTask;
import es.bsc.mobile.runtime.types.requests.executor.ExecutorRequest;
import es.bsc.mobile.runtime.types.requests.executor.RunTask;
import es.bsc.mobile.runtime.types.resources.ComputingPlatform;
import es.bsc.mobile.runtime.utils.ResourceManager;
import es.bsc.mobile.types.JobProfile;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Executor extends Thread {

    private boolean keepGoing;
    protected final LinkedBlockingQueue<ExecutorRequest> requestQueue;
    private final LinkedList<ComputingPlatform> platforms;
    private static final int CHECKER_TIMEOUT = 500;

    public Executor(ResourceManager resources) {
        keepGoing = true;
        requestQueue = new LinkedBlockingQueue<>();
        platforms = resources.getComputingPlatforms();
        setName("Task Executor");
    }

    @Override
    public void run() {
        for (ComputingPlatform cp : platforms) {
            cp.init();
        }
        ExecutorRequest request;
        while (keepGoing) {
            try {
                request = requestQueue.poll(CHECKER_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                continue;
            }
            if (request != null) {
                request.dispatch(platforms);
            }
        }
    }

    public void shutdown() {
        keepGoing = false;
    }

    public void executeTask(Task task) {
        RunTask rt = new RunTask(task);
        requestQueue.add(rt);
    }

    public void jobEnd(int taskId, int platformId, JobProfile jp, Node runner) {
        EndTask et = new EndTask(taskId, platformId, jp, runner);
        requestQueue.add(et);

    }

    public void resizeCores() {
        for (ComputingPlatform cp : platforms) {
            cp.resizeCores();
        }
    }
}
