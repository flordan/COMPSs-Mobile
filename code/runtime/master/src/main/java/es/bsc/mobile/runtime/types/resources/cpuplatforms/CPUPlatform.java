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
package es.bsc.mobile.runtime.types.resources.cpuplatforms;

import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.types.Implementation;
import es.bsc.mobile.runtime.types.Method;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.profile.ImplementationProfile;
import es.bsc.mobile.runtime.types.resources.LocalComputingPlatform;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.scheduler.BasicScheduler;
import es.bsc.mobile.types.calc.MinMax;
import es.bsc.mobile.utils.JobExecutor;
import es.bsc.mobile.scheduler.JobScheduler;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobExecution;
import es.bsc.mobile.types.JobParameter;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class CPUPlatform extends LocalComputingPlatform<CPUResource> {

    private static final int CPU_CORE_COUNT = 4;
    private final static long POWER_CONSUMPTION = 1600l;

    private final ExecutorService executor;
    private final JobScheduler scheduler;
    private final LinkedBlockingQueue<JobExecution> toExecuteJobs;

    public CPUPlatform(RuntimeHandler rh, String name) {
        super(rh, name);
        scheduler = new BasicScheduler(Integer.MAX_VALUE);
        executor = Executors.newFixedThreadPool(CPU_CORE_COUNT);
        toExecuteJobs = new LinkedBlockingQueue<>();
    }

    @Override
    public void addResource(CPUResource res) {

    }

    @Override
    public void init() {
        scheduler.start();
        for (int i = 0; i < CPU_CORE_COUNT; i++) {
            executor.execute(new JobExecutor(toExecuteJobs));
        }
    }

    @Override
    protected boolean canRun(Implementation impl) {
        return impl instanceof Method;
    }

    @Override
    public MinMax getWaitingForecast() {
        MinMax waiting = new MinMax();
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            ImplementationProfile prof = getCoreProfile(coreId);
            waiting.aggregate(tasksToProcess[coreId]/CPU_CORE_COUNT, prof.getExecutionTime());
        }
        return waiting;
    }

    @Override
    protected MinMax getTimeForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl) {
        ImplementationProfile prof = getImplementationProfile(impl.getCoreElementId(), impl.getImplementationId());
        return new MinMax(prof.getExecutionTime());
    }

    @Override
    protected MinMax getEnergyForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl) {
        ImplementationProfile prof = getImplementationProfile(impl.getCoreElementId(), impl.getImplementationId());
        return prof.getEnergyConsumption();
    }

    @Override
    protected MinMax getCostForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl) {
        return new MinMax();
    }

    @Override
    public void submitTask(Task task) {
        tasksToProcess[task.getTaskParams().getCoreId()]++;
        Job job = createJob(task);
        JobExecution jl = new CPUJobExecution(job);
        scheduler.newJob(jl);
    }

    public void runJob(Object executorId, JobExecution je) {
        toExecuteJobs.offer(je);
    }

    private class CPUJobExecution extends JobExecution {

        public CPUJobExecution(Job j) {
            super(j);
        }

        @Override
        protected void requestParamDataExistence(String dataId, int paramId, DataManager.DataExistenceListener listener) {
            CPUPlatform.this.requestDataExistence(dataId, listener);
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
            CPUPlatform.this.obtainDataSize(dataId, listener);
        }

        @Override
        protected void obtainDataAsObject(String dataId, String dataRenaming, int paramId, LoadParamData listener) {
            CPUPlatform.this.obtainDataAsObject(dataId, dataRenaming, listener);
        }

        @Override
        protected void obtainDataAsFile(String dataId, String dataRenaming, int paramId, LoadParamData listener) {
            CPUPlatform.this.obtainDataAsFile(dataId, dataRenaming, listener);
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
            return CPUPlatform.this.getCompatibleImplementations(getJob().getImplementations()[0].getCoreElementId());
        }

        @Override
        public void executeOn(Object ID, es.bsc.mobile.types.Implementation impl) {
            getJob().selectImplementation(impl);
            runJob(ID, this);
        }

        @Override
        public void failedExecution() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void finishedExecution() {
            tasksToProcess[this.getJob().getJobProfile().getCoreId()]--;
            scheduler.executedJob(this);
        }

        @Override
        protected void storeObject(String dataId, Object value, DataManager.DataOperationListener listener) {
            CPUPlatform.this.storeObject(dataId, value, listener);
        }

        @Override
        protected void storeFile(String dataId, String location, DataManager.DataOperationListener listener) {
            CPUPlatform.this.storeFile(dataId, location, listener);
        }

        @Override
        public void completed() {
            long length = this.getJob().getJobProfile().getExecutionTime();
            this.getJob().setConsumption(POWER_CONSUMPTION * length);
            scheduler.completedJob(this);
            jobCompleted(this.getJob());
        }
    }
}
