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
package es.bsc.mobile.worker;

import es.bsc.mobile.scheduler.BasicScheduler;
import es.bsc.mobile.scheduler.JobScheduler;
import es.bsc.mobile.utils.JobExecutor;
import es.bsc.comm.Node;
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.node.RuntimeNode;
import es.bsc.mobile.types.Implementation;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobProfile;
import es.bsc.mobile.types.messages.runtime.JobCompleted;
import es.bsc.mobile.types.JobExecution;
import es.bsc.mobile.types.JobParameter;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class RuntimeWorker extends RuntimeNode {

    private int slots = 0;
    private final ExecutorService executor;
    private final JobScheduler scheduler;
    private final LinkedBlockingQueue<JobExecution> toExecuteJobs;
    private final Node masterNodeRuntime;

    public RuntimeWorker(int slots, Node me, Node masterRuntime) {
        super(me);
        this.slots = slots;
        masterNodeRuntime = masterRuntime;
        scheduler = new BasicScheduler(Integer.MAX_VALUE);
        toExecuteJobs = new LinkedBlockingQueue<>();
        executor = Executors.newFixedThreadPool(slots + 1);
    }

    @Override
    public void init() {
        super.init();
        scheduler.start();
        for (int i = 0; i < slots; i++) {
            executor.execute(new JobExecutor(toExecuteJobs));
        }
    }

    /**
     * The system submits a new job to be executed on this node
     *
     * @param job Job description
     */
    @Override
    public void newJob(Job job) {
        JobExecution jl = new WorkerJobExecution(job);
        scheduler.newJob(jl);
    }

    public void runJob(Object executorId, JobExecution je) {
        toExecuteJobs.offer(je);
    }

    @Override
    public void jobFinished(int taskId, int platformId, JobProfile job, Node executor) {

    }

    @Override
    public void updateMaster(Node node) {
        /*Despite we are using the same node and the nodeRef keeps 
         being the same object, the IP address of the node may have 
         changed. Therefore, it is necessary to update the
         hostname of the noderef since the data locations stored in 
         the dataregistry changes.
         */
        //updateRepresentative(masterNodeRuntime, node);
    }

    private class WorkerJobExecution extends JobExecution {

        public WorkerJobExecution(Job j) {
            super(j);
        }

        @Override
        protected void requestParamDataExistence(String dataId, int paramId, DataManager.DataExistenceListener listener) {
            RuntimeWorker.this.requestDataExistence(dataId, listener);
        }

        @Override
        public void paramDataExists(JobParameter jp) {
            scheduler.notifyParamValueExistence(getJob(), jp);
        }

        @Override
        public void allParamDataExists() {
            scheduler.dependencyFreeJob(this);
        }

        @Override
        protected void obtainDataSize(String dataId, int paramId, LoadParamData listener) {
            RuntimeWorker.this.obtainDataSize(dataId, listener);
        }

        @Override
        protected void obtainDataAsObject(String dataId, String dataRenaming, int paramId, LoadParamData listener) {
            RuntimeWorker.this.obtainDataAsObject(dataId, dataRenaming, listener);
        }

        @Override
        protected void obtainDataAsFile(String dataId, String dataRenaming, int paramId, LoadParamData listener) {
            RuntimeWorker.this.obtainDataAsFile(dataId, dataRenaming, listener);
        }

        @Override
        public void allDataPresent() {
            allDataReady();
        }

        @Override
        public void prepareJobParameter(int paramId) {
            //Parameters do not go through preparation stage. Upon obtention can already be used
        }

        @Override
        public void prepareTargetObject() {
            //Parameters do not go through preparation stage. Upon obtention can already be used
        }

        @Override
        public void prepareResult() {
            //Parameters do not go through preparation stage. Upon obtention can already be used
        }

        @Override
        public void allDataReady() {
            scheduler.allValuesReady(this);
        }

        @Override
        public LinkedList<es.bsc.mobile.types.Implementation> getCompatibleImplementations() {
            LinkedList<es.bsc.mobile.types.Implementation> impls = new LinkedList<>();
            for (Implementation impl : getJob().getImplementations()) {
                impls.add(impl);
            }
            return impls;
        }

        @Override
        public void executeOn(Object ID, Implementation impl) {
            getJob().selectImplementation(impl);
            runJob(ID, this);
        }

        @Override
        public void failedExecution() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void finishedExecution() {
            scheduler.executedJob(this);
        }

        @Override
        protected void storeObject(String dataId, Object value, DataManager.DataOperationListener listener) {
            RuntimeWorker.this.storeObject(dataId, value, listener);
        }

        @Override
        protected void storeFile(String dataId, String location, DataManager.DataOperationListener listener) {
            RuntimeWorker.this.storeFile(dataId, location, listener);
        }

        @Override
        public void completed() {
            scheduler.completedJob(this);
            CommunicationManager.notifyCommand(masterNodeRuntime, new JobCompleted(getJob().getTaskId(), getJob().getPlatformId(), getJob().getJobProfile(), getMe()));
        }
    }
}
