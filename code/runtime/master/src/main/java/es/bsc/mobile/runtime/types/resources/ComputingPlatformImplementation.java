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
package es.bsc.mobile.runtime.types.resources;

import es.bsc.comm.Node;
import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.data.DataManager.DataStatus;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.TaskParameters;
import es.bsc.mobile.runtime.types.data.parameter.BasicTypeParameter;
import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.types.Implementation;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobParameter;
import es.bsc.mobile.types.JobProfile;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ComputingPlatformImplementation<T extends Resource> implements ComputingPlatform<T> {

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Jobs");
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();
    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();
    private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

    private final int id;
    private final String name;
    private final RuntimeHandler rh;

    public ComputingPlatformImplementation(RuntimeHandler rh, String name) {
        this.id = NEXT_ID.getAndIncrement();
        this.rh = rh;
        this.name = name;
    }

    public final int getId() {
        return id;
    }

    public final String getName() {
        return this.name;
    }

    protected void submitJob(Job job, Node target) {
        rh.submitJob(job, target);
    }

    protected DataStatus getDataStatus(String dataId) {
        return rh.getDataStatus(dataId);
    }

    protected Job createJob(Task t) {
        Job job = new Job(t.getId(), id);
        TaskParameters tp = t.getTaskParams();
        int coreId = tp.getCoreId();
        es.bsc.mobile.runtime.types.Implementation[] coreImpls = CoreManager.getCoreImplementations(coreId);

        Implementation[] jobImpls = new Implementation[coreImpls.length];
        job.setImplementations(jobImpls);
        for (es.bsc.mobile.runtime.types.Implementation impl : coreImpls) {
            jobImpls[impl.getImplementationId()] = impl.getInternalImplementation();
        }

        Parameter[] taskParams = tp.getParameters();
        int paramCount = taskParams.length;
        if (tp.hasReturn()) {
            paramCount--;
        }
        if (tp.hasTarget()) {
            paramCount--;
        }

        JobParameter[] jobParams = new JobParameter[paramCount];
        int parId;
        for (parId = 0; parId < paramCount; parId++) {
            switch (taskParams[parId].getType()) {
                case FILE:
                    RegisteredParameter fileParam = (RegisteredParameter) taskParams[parId];
                    registerDataFutureLocations(fileParam);
                    jobParams[parId] = new JobParameter.FileJobParameter(fileParam.getDirection(), fileParam.getDAccess().prepareToOffload());
                    break;
                case OBJECT:
                    RegisteredParameter objectParam = (RegisteredParameter) taskParams[parId];
                    registerDataFutureLocations(objectParam);
                    jobParams[parId] = new JobParameter.ObjectJobParameter(objectParam.getDirection(), objectParam.getDAccess().prepareToOffload());
                    break;
                default:
                    BasicTypeParameter btp = (BasicTypeParameter) taskParams[parId];
                    jobParams[parId] = getBasicTypeParameter(btp);
            }
        }
        job.setParams(jobParams);

        if (tp.hasTarget()) {
            RegisteredParameter objectParam = (RegisteredParameter) taskParams[parId];
            JobParameter.ObjectJobParameter ojp = new JobParameter.ObjectJobParameter(objectParam.getDirection(), objectParam.getDAccess().prepareToOffload());
            registerDataFutureLocations(objectParam);
            job.setTarget(ojp);
            parId++;
        }

        if (tp.hasReturn()) {
            RegisteredParameter objectParam = (RegisteredParameter) taskParams[parId];
            JobParameter.ObjectJobParameter ojp = new JobParameter.ObjectJobParameter(objectParam.getDirection(), objectParam.getDAccess().prepareToOffload());
            registerDataFutureLocations(objectParam);
            job.setResult(ojp);
            parId++;
        }
        return job;
    }

    protected abstract void registerDataFutureLocations(RegisteredParameter param);

    protected JobParameter getBasicTypeParameter(BasicTypeParameter btp) {
        JobParameter jp;
        switch (btp.getType()) {
            case BOOLEAN:
                jp = new JobParameter.BooleanJobParameter(btp.getDirection(), (Boolean) btp.getValue());
                break;
            case CHAR:
                jp = new JobParameter.CharJobParameter(btp.getDirection(), (Character) btp.getValue());
                break;
            case STRING:
                jp = new JobParameter.StringJobParameter(btp.getDirection(), (String) btp.getValue());
                break;
            case BYTE:
                jp = new JobParameter.ByteJobParameter(btp.getDirection(), (Byte) btp.getValue());
                break;
            case SHORT:
                jp = new JobParameter.ShortJobParameter(btp.getDirection(), (Short) btp.getValue());
                break;
            case INT:
                jp = new JobParameter.IntegerJobParameter(btp.getDirection(), (Integer) btp.getValue());
                break;
            case LONG:
                jp = new JobParameter.LongJobParameter(btp.getDirection(), (Long) btp.getValue());
                break;
            case FLOAT:
                jp = new JobParameter.FloatJobParameter(btp.getDirection(), (Float) btp.getValue());
                break;
            case DOUBLE:
                jp = new JobParameter.DoubleJobParameter(btp.getDirection(), (Double) btp.getValue());
                break;
            default:
                jp = null;
        }
        return jp;
    }

    protected void jobCompleted(Job job) {
        int taskId = job.getTaskId();
        int platformId = job.getPlatformId();
        JobProfile jp = job.getJobProfile();
        if (INFO_ENABLED) {
            LOGGER.info("Job " + taskId + " completed.");
            if (DEBUG_ENABLED) {
                LOGGER.debug(jp.toString());
            }
        }
        rh.jobFinished(taskId, platformId, jp, rh.getMe());
    }

    protected void requestDataExistence(String dataId, DataManager.DataExistenceListener listener) {
        rh.requestDataExistence(dataId, listener);
    }

    protected void obtainDataSize(String dataId, DataManager.DataOperationListener listener) {
        rh.obtainDataSize(dataId, listener);
    }

    protected void obtainDataAsObject(String dataId, String dataRenaming, DataManager.DataOperationListener listener) {
        rh.obtainDataAsObject(dataId, dataRenaming, listener);
    }

    protected void obtainDataAsFile(String dataId, String dataRenaming, DataManager.DataOperationListener listener) {
        rh.obtainDataAsFile(dataId, dataRenaming, listener);
    }

    protected void storeObject(String dataId, Object value, DataManager.DataOperationListener listener) {
        rh.storeObject(dataId, value, listener);
    }

    protected void storeFile(String dataId, String location, DataManager.DataOperationListener listener) {
        rh.storeFile(dataId, location, listener);
    }
}
