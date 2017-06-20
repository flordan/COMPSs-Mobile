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
package es.bsc.mobile.runtime.types.resources.gpuplatforms;

import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.data.access.RAccess;
import es.bsc.mobile.data.access.RWAccess;
import es.bsc.mobile.data.access.WAccess;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.types.Implementation;
import es.bsc.mobile.types.Kernel;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.profile.ImplementationProfile;
import es.bsc.mobile.runtime.types.resources.LocalComputingPlatform;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.utils.Expression;
import es.bsc.mobile.scheduler.BasicScheduler;
import es.bsc.mobile.scheduler.JobScheduler;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobExecution;
import es.bsc.mobile.types.JobParameter;
import es.bsc.mobile.types.JobParameter.ObjectJobParameter;
import es.bsc.mobile.types.calc.MinMax;
import es.bsc.opencl.wrapper.Device;
import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class GPUPlatform extends LocalComputingPlatform<OpenCLResource> {

    private final static long POWER_CONSUMPTION = 2500l;

    private Device device;
    private final JobScheduler scheduler;
    private TreeMap<String, OnCreationDataInformation> onCreationData = new TreeMap<>();
    private final ValueFetcher vf = new ValueFetcher();

    public GPUPlatform(RuntimeHandler rh, String name) {
        super(rh, name);
        scheduler = new BasicScheduler(Integer.MAX_VALUE);
        new Thread(vf).start();
    }

    @Override
    public void init() {
        scheduler.start();
    }

    @Override
    public void addResource(OpenCLResource res) {
        device = res.getDevice();
    }

    @Override
    protected boolean canRun(es.bsc.mobile.runtime.types.Implementation impl) {
        try {
            es.bsc.mobile.runtime.types.Kernel ocl = (es.bsc.mobile.runtime.types.Kernel) impl;
            if (device.addProgramFromSource(ocl.getProgram(), ocl.getSourceCode()) == null) {
                return false;
            }
        } catch (ClassCastException cce) {
            return false;
        }
        return true;
    }

    @Override
    public void submitTask(Task task) {
        tasksToProcess[task.getTaskParams().getCoreId()]++;
        Job job = createJob(task);
        JobExecution jl = new GPUJobExecution(job);
        scheduler.newJob(jl);
    }

    @Override
    public MinMax getWaitingForecast() {
        MinMax waiting = new MinMax();
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            ImplementationProfile prof = getCoreProfile(coreId);
            waiting.aggregate(tasksToProcess[coreId], prof.getExecutionTime());
        }
        return waiting;
    }

    @Override
    protected MinMax getTimeForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl) {
        ImplementationProfile prof = getImplementationProfile(impl.getCoreElementId(), impl.getImplementationId());
        return new MinMax(prof.getExecutionTime());
    }

    @Override
    protected MinMax getEnergyForecast(LinkedList<DataAndStatus> inData, Implementation impl) {
        ImplementationProfile prof = getImplementationProfile(impl.getCoreElementId(), impl.getImplementationId());
        return prof.getEnergyConsumption();
    }

    @Override
    protected MinMax getCostForecast(LinkedList<DataAndStatus> inData, Implementation impl) {
        return new MinMax();
    }

    private class OnCreationDataInformation {

        public boolean issued = false;
        public LinkedList<JobExecution.LoadParamData> listeners = new LinkedList<JobExecution.LoadParamData>();
    }

    private class GPUJobExecution extends JobExecution {

        private class ParamInfo {

            final String inData;
            final String outData;
            final boolean reads;
            final boolean writes;
            boolean bypassed;

            public ParamInfo() {
                inData = "";
                outData = "";
                reads = true;
                writes = false;
            }

            public ParamInfo(String inData, String outData) {
                this.inData = inData;
                this.reads = (inData != null);
                this.outData = outData;
                this.writes = (outData != null);
            }

        }
        private final ParamInfo[] paramsInfo;
        private final boolean[] updates;

        private final Object[] parValues;

        private Class<?> resultType;
        private int[] resultSizes;
        private final AtomicInteger pendingPrepares;
        private final AtomicInteger pendingResults;

        public GPUJobExecution(Job j) {
            super(j);
            int paramCount = j.getParamValues().length;

            if (j.getResult() != null) {
                paramCount++;
            }

            updates = new boolean[paramCount];

            parValues = new Object[paramCount];
            paramsInfo = new ParamInfo[paramCount];
            this.pendingPrepares = new AtomicInteger(paramCount);
            this.pendingResults = new AtomicInteger(paramCount);

            int paramId = 0;
            final JobParameter[] params = j.getParams();
            for (; paramId < params.length; paramId++) {
                JobParameter jp = params[paramId];
                switch (jp.getType()) {
                    case OBJECT:
                        ObjectJobParameter op = (ObjectJobParameter) jp;
                        String outDataId;
                        switch (op.getDirection()) {
                            case IN:
                                RAccess read = (RAccess) op.getDataAccess();
                                paramsInfo[paramId] = new ParamInfo(read.getReadDataInstance(), null);
                                break;
                            case INOUT:
                                RWAccess readWrite = (RWAccess) op.getDataAccess();
                                outDataId = readWrite.getWrittenDataInstance();
                                paramsInfo[paramId] = new ParamInfo(readWrite.getReadDataInstance(), outDataId);
                                onCreationData.put(outDataId, new OnCreationDataInformation());
                                break;
                            default: //case OUT:
                                WAccess write = (WAccess) op.getDataAccess();
                                outDataId = write.getWrittenDataInstance();
                                paramsInfo[paramId] = new ParamInfo(null, outDataId);
                                onCreationData.put(outDataId, new OnCreationDataInformation());
                        }
                        break;
                    default:
                }
            }
            if (j.getResult() != null) {
                ObjectJobParameter op = (ObjectJobParameter) j.getResult();
                String dataId = ((WAccess) op.getDataAccess()).getWrittenDataInstance();
                paramsInfo[paramsInfo.length - 1] = new ParamInfo(null, dataId);
                onCreationData.put(dataId, new OnCreationDataInformation());
            }
        }

        @Override
        protected void requestParamDataExistence(String dataId, int paramId, DataManager.DataExistenceListener listener) {
            if (onCreationData.containsKey(dataId)) {
                paramsInfo[paramId].bypassed = true;
                listener.exists();
            } else {
                paramsInfo[paramId].bypassed = false;
                GPUPlatform.this.requestDataExistence(dataId, listener);
            }
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
            GPUPlatform.this.obtainDataSize(dataId, listener);
        }

        @Override
        protected void obtainDataAsObject(String dataId, String dataRenaming, int paramId, LoadParamData listener) {
            if (!paramsInfo[paramId].bypassed) {
                GPUPlatform.this.obtainDataAsObject(dataId, dataRenaming, listener);
            } else {
                OnCreationDataInformation dataInfo = onCreationData.get(dataId);
                if (!dataInfo.issued) {
                    LinkedList<LoadParamData> pendingPrepares = dataInfo.listeners;
                    pendingPrepares.add(listener);
                } else {
                    listener.skipLoadValue();
                }
            }
        }

        @Override
        protected void obtainDataAsFile(String dataId, String dataRenaming, int paramId, LoadParamData listener) {
            GPUPlatform.this.obtainDataAsFile(dataId, dataRenaming, listener);
        }

        @Override
        public void allDataPresent() {
            scheduler.allValuesObtained(this);
        }

        @Override
        public void prepareJobParameter(int paramId) {
            JobParameter jp = getJob().getParams()[paramId];
            PrepareListener prepListenner = new PrepareListener(paramId);
            switch (jp.getType()) {
                case OBJECT:
                    ObjectJobParameter op = (ObjectJobParameter) jp;
                    boolean copyIn = true;
                    boolean canWrite = true;
                    String valueRenaming;
                    switch (op.getDirection()) {
                        case IN:
                            canWrite = false;
                            valueRenaming = ((RAccess) op.getDataAccess()).getReadDataInstance();
                            break;
                        case INOUT:
                            valueRenaming = ((RWAccess) op.getDataAccess()).getWrittenDataInstance();
                            updates[paramId] = true;
                            break;
                        default: //case OUT:
                            copyIn = false;
                            valueRenaming = ((WAccess) op.getDataAccess()).getWrittenDataInstance();
                            updates[paramId] = true;
                    }
                    Object o = getJob().getParamValues()[paramId];
                    int size = 1;
                    Class<?> oClass = null;
                    if (o != null) {
                        oClass = o.getClass();
                        Object sizeObject = o;
                        while (oClass.isArray()) {
                            size *= Array.getLength(sizeObject);
                            sizeObject = Array.get(sizeObject, 0);
                            oClass = oClass.getComponentType();
                        }
                    }
                    device.prepareValueForKernel(prepListenner, valueRenaming, o, oClass, size, copyIn, canWrite);
                    break;
                default:
                    prepListenner.completed(getJob().getParamValues()[paramId]);
            }
        }

        @Override
        public void prepareTargetObject() {
            //For OpenCL there is no target object
        }

        @Override
        public void prepareResult() {
            ObjectJobParameter op = (ObjectJobParameter) getJob().getResult();
            if (op != null) {
                updates[parValues.length - 1] = true;
                PrepareListener prepListenner = new PrepareListener(parValues.length - 1);
                String valueRenaming = ((WAccess) op.getDataAccess()).getWrittenDataInstance();

                Kernel k = (Kernel) this.getCompatibleImplementations().getFirst();
                Expression[] resultSizeExpressions = k.getResultSizeExpressions();
                resultType = k.getResultType();
                int resultDims = resultSizeExpressions.length;
                resultSizes = new int[resultDims];
                int resultSize = 1;
                for (int resultDim = 0; resultDim < resultDims; resultDim++) {
                    resultSizes[resultDim] = resultSizeExpressions[resultDim].evaluate(getJob().getParamValues());
                    resultSize *= resultSizes[resultDim];
                }
                device.prepareValueForKernel(prepListenner, valueRenaming, null, resultType, resultSize, false, true);
            }
        }

        private class PrepareListener extends Device.OpenCLMemoryPrepare {

            private final int paramId;

            private PrepareListener(int paramId) {
                this.paramId = paramId;
            }

            @Override
            public void completed(Object paramValue) {
                parValues[paramId] = paramValue;
                int pending = pendingPrepares.decrementAndGet();
                if (pending == 0) {
                    allDataReady();
                }
            }

            @Override
            public void failed() {
            }
        }

        @Override
        public void allDataReady() {
            scheduler.allValuesReady(this);
        }

        @Override
        public LinkedList<es.bsc.mobile.types.Implementation> getCompatibleImplementations() {
            return GPUPlatform.this.getCompatibleImplementations(getJob().getImplementations()[0].getCoreElementId());
        }

        @Override
        public void executeOn(Object Id, Implementation impl) {
            Job job = getJob();
            job.selectImplementation(impl);
            Kernel k = ((Kernel) job.getSelectedImplementation());
            String programName = k.getProgram();
            String methodName = k.getMethodName();
            Expression[] workSizeExpressions = k.getWorkloadExpressions();
            Expression[] localSizeExpressions = k.getLocalSizeExpressions();
            Expression[] offsetExpressions = k.getOffsetExpressions();
            int workSizeDims = workSizeExpressions.length;
            long[] globalWorkSize = new long[workSizeDims];
            long[] offset = new long[workSizeDims];
            long[] localSize = new long[localSizeExpressions.length];
            for (int i = 0; i < workSizeDims; i++) {
                globalWorkSize[i] = workSizeExpressions[i].evaluate(getJob().getParamValues());
                offset[i] = offsetExpressions[i].evaluate(getJob().getParamValues());
                if (localSizeExpressions.length != 0) {
                    localSize[i] = localSizeExpressions[i].evaluate(getJob().getParamValues());
                }
            }

            synchronized (this) {
                device.runKernel(programName, methodName, updates, parValues, globalWorkSize, localSize, offset, new ExecutionListenner());
                for (int paramId = 0; paramId < paramsInfo.length; paramId++) {
                    if (paramsInfo[paramId] != null && paramsInfo[paramId].writes) {
                        OnCreationDataInformation dataInfo = onCreationData.get(paramsInfo[paramId].outData);
                        dataInfo.issued = true;
                        LinkedList<LoadParamData> waiting = dataInfo.listeners;
                        for (LoadParamData listener : waiting) {
                            listener.skipLoadValue();
                        }
                    }
                }
                Object expandedResult = Array.newInstance(resultType, resultSizes);
                getJob().setResultValue(expandedResult);
            }
        }

        private class ExecutionListenner extends Device.OpenCLExecutionListener {

            @Override
            public void completed(long start, long end) {
                long length = end - start;
                long startTime = System.currentTimeMillis() * 1_000_000 - length;
                getJob().startExecution(startTime);
                getJob().endsExecution();
                getJob().setConsumption(POWER_CONSUMPTION * length);
                vf.fetch(GPUJobExecution.this);
                GPUJobExecution.this.finishedTask(getJob().getJobProfile().getCoreId());
            }

            @Override
            public void failed() {
                failedExecution();
            }
        }

        private void finishedTask(int coreId) {
            GPUPlatform.this.tasksToProcess[coreId]--;
        }

        public void fetchParamValue(int paramId) {
            JobParameter jp = getJob().getParams()[paramId];
            switch (jp.getType()) {
                case OBJECT:
                    Object o = getJob().getParamValues()[paramId];
                    device.collectValueFromKernel(parValues[paramId], o, updates[paramId]);
                    break;
                default:
            }
            int pending = pendingResults.decrementAndGet();
            if (pending == 0) {
                finishedExecution();
            }
        }

        public void fetchResultValue() {
            if (getJob().getResult() != null) {
                Object expandedResult = getJob().getResultValue();
                device.collectValueFromKernel(parValues[parValues.length - 1], expandedResult, true);
                int pending = pendingResults.decrementAndGet();
                if (pending == 0) {
                    finishedExecution();
                }
            }
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
            OnCreationDataInformation dataInfo = onCreationData.remove(dataId);
            GPUPlatform.this.storeObject(dataId, value, listener);
            for (LoadParamData lpd : dataInfo.listeners) {
                GPUPlatform.this.obtainDataSize(dataId, lpd);
            }
        }

        @Override
        protected void storeFile(String dataId, String location, DataManager.DataOperationListener listener) {
            GPUPlatform.this.storeFile(dataId, location, listener);
        }

        @Override
        public void completed() {
            scheduler.completedJob(this);
            jobCompleted(this.getJob());
        }
    }

    private class ValueFetcher implements Runnable {

        LinkedBlockingQueue<GPUJobExecution> queue = new LinkedBlockingQueue<GPUJobExecution>();

        public void run() {
            while (true) {
                try {
                    GPUJobExecution gje = queue.take();
                    for (int i = 0; i < gje.getJob().getParams().length; i++) {
                        gje.fetchParamValue(i);
                    }
                    synchronized (gje) {
                        gje.fetchResultValue();
                    }
                } catch (InterruptedException ex) {

                }
            }
        }

        public void fetch(GPUJobExecution job) {
            queue.offer(job);
        }
    }
}
