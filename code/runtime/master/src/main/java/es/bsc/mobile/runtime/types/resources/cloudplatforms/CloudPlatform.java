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
package es.bsc.mobile.runtime.types.resources.cloudplatforms;

import es.bsc.comm.Node;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.data.DataInstance;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.access.WriteAccess;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.types.profile.ImplementationProfile;
import es.bsc.mobile.runtime.types.resources.ComputingPlatformImplementation;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobProfile;
import es.bsc.mobile.types.calc.MinMax;
import java.util.LinkedList;
import java.util.TreeSet;

public abstract class CloudPlatform extends ComputingPlatformImplementation<RemoteResource> {

    private final TreeSet<String> offloadedData = new TreeSet<>();
    private ImplementationProfile[] profiles = new ImplementationProfile[0];

    public CloudPlatform(RuntimeHandler rh, String name) {
        super(rh, name);
    }

    @Override
    public void defineDefaultProfiles(String profiles) {

    }

    @Override
    public void resizeCores() {
        int coreCount = CoreManager.getCoreCount();
        int oldCoreCount = this.profiles.length;
        ImplementationProfile[] profiles = new ImplementationProfile[coreCount];
        int coreId = 0;
        for (; coreId < oldCoreCount; coreId++) {
            profiles[coreId] = this.profiles[coreId];
        }
        for (; coreId < coreCount; coreId++) {
            profiles[coreId] = new ImplementationProfile(0);
        }
        this.profiles = profiles;
    }

    @Override
    public void init() {

    }

    @Override
    public boolean canRun(Task task) {
        return true;
    }

    @Override
    public ExecutionScore getExecutionForecast(Task t, LinkedList<TaskData> inData, LinkedList<TaskData> outData) {
        MinMax missingInput = new MinMax();
        MinMax remoteOutput = new MinMax();
        for (TaskData td : inData) {
            if (!offloadedData.contains(td.getDataName())) {
                missingInput.aggregate(td.getDataSize());
            }
        }
        for (TaskData td : outData) {
            remoteOutput.aggregate(td.getDataSize());
        }

        int coreId = t.getTaskParams().getCoreId();
        MinMax time = getTimeForecast(missingInput, remoteOutput, coreId);
        MinMax energy = getEnergyForecast(missingInput, remoteOutput, coreId);
        MinMax cost = getCostForecast(missingInput, remoteOutput, coreId);
        return new ExecutionScore(time, energy, cost);
    }

    private MinMax getTimeForecast(MinMax inData, MinMax outData, int coreId) {
        MinMax prediction = new MinMax();
        prediction.aggregate(profiles[coreId].getExecutionTime());
        return prediction;
    }

    private MinMax getEnergyForecast(MinMax inData, MinMax outData, int coreId) {
        MinMax prediction = new MinMax();
        return prediction;
    }

    private MinMax getCostForecast(MinMax inData, MinMax outData, int coreId) {
        MinMax prediction = new MinMax();
        return prediction;
    }

    protected void submitTask(Task t, RemoteResource res) {
        Job job = createJob(t);
        submitJob(job, res.getNode());
    }

    @Override
    protected void registerDataFutureLocations(RegisteredParameter param) {
        DataInstance daId;
        switch (param.getDirection()) {
            case IN:
                daId = ((ReadAccess) param.getDAccess()).getReadDataInstance();
                offloadedData.add(daId.getRenaming());
                break;
            case INOUT:
                daId = ((ReadWriteAccess) param.getDAccess()).getReadDataInstance();
                offloadedData.add(daId.getRenaming());
                daId = ((ReadWriteAccess) param.getDAccess()).getWrittenDataInstance();
                offloadedData.add(daId.getRenaming());
                break;
            default:
                daId = ((WriteAccess) param.getDAccess()).getWrittenDataInstance();
                offloadedData.add(daId.getRenaming());
        }
    }

    @Override
    public final void endTask(int taskId, JobProfile jp, Node runner) {
        int coreId = jp.getCoreId();
        profiles[coreId].registerProfiledJob(jp);
        finishedTaskNotification(taskId, jp, runner);
    }

    public abstract void finishedTaskNotification(int taskId, JobProfile jp, Node runner);
}
