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
package es.bsc.mobile.runtime.types.requests.executor;

import es.bsc.mobile.annotations.Parameter.Type;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.TaskParameters;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.access.WriteAccess;
import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.types.profile.CoreProfile;
import es.bsc.mobile.runtime.types.resources.ComputingPlatform;
import es.bsc.mobile.runtime.types.resources.ComputingPlatform.ExecutionScore;
import es.bsc.mobile.runtime.types.resources.ComputingPlatform.TaskData;
import es.bsc.mobile.runtime.types.resources.StaticAssignationManager;
import es.bsc.mobile.runtime.utils.CoreManager;
import java.util.LinkedList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RunTask extends ExecutorRequest {

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Offload");
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();
    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();
    private final Task task;

    public RunTask(Task task) {
        this.task = task;
    }

    public Task getTask() {
        return task;
    }

    @Override
    public void dispatch(LinkedList<ComputingPlatform> platforms) {
        ExecutionScore bestScore = null;
        ComputingPlatform bestPlatform = null;
        TaskParameters tp = task.getTaskParams();
        int coreId = tp.getCoreId();
        Parameter[] taskParams = tp.getParameters();

        //Data Size Analysis
        CoreProfile profile = CoreManager.getCoreProfile(coreId);
        int numParams = profile.getNumParams();

        LinkedList<TaskData> inData = new LinkedList();
        LinkedList<TaskData> outData = new LinkedList();

        String dataName;
        int paramId;
        for (paramId = 0; paramId < numParams; paramId++) {
            if (taskParams[paramId].getType() == Type.OBJECT || taskParams[paramId].getType() == Type.FILE) {
                RegisteredParameter rp = (RegisteredParameter) taskParams[paramId];
                switch (rp.getDAccess().getAction()) {
                    case READ:
                        dataName = ((ReadAccess) rp.getDAccess()).getReadDataInstance().getRenaming();
                        inData.add(new TaskData(dataName, profile.getParamInSize(paramId)));
                        break;
                    case UPDATE:
                        dataName = ((ReadWriteAccess) rp.getDAccess()).getReadDataInstance().getRenaming();
                        inData.add(new TaskData(dataName, profile.getParamInSize(paramId)));
                        dataName = ((ReadWriteAccess) rp.getDAccess()).getWrittenDataInstance().getRenaming();
                        outData.add(new TaskData(dataName, profile.getParamOutSize(paramId)));
                        break;
                    default:
                        //WRITE
                        dataName = ((WriteAccess) rp.getDAccess()).getWrittenDataInstance().getRenaming();
                        outData.add(new TaskData(dataName, profile.getParamOutSize(paramId)));
                        break;

                }
            }
        }
        if (tp.hasTarget()) {
            RegisteredParameter rp = (RegisteredParameter) taskParams[paramId];
            ReadWriteAccess rwa = (ReadWriteAccess) rp.getDAccess();
            dataName = rwa.getReadDataInstance().getRenaming();
            inData.add(new TaskData(dataName, profile.getTargetInSize()));
            dataName = rwa.getWrittenDataInstance().getRenaming();
            outData.add(new TaskData(dataName, profile.getTargetOutSize()));
            paramId++;
        }

        if (tp.hasReturn()) {
            RegisteredParameter rp = (RegisteredParameter) taskParams[paramId];
            dataName = ((WriteAccess) rp.getDAccess()).getWrittenDataInstance().getRenaming();
            outData.add(new TaskData(dataName, profile.getResultSize()));
        }

        bestPlatform = StaticAssignationManager.getPredefinedPlatform(task);

        if (bestPlatform != null && !bestPlatform.canRun(task)) {
            bestPlatform = null;
        }

        if (bestPlatform == null) {
            for (ComputingPlatform cp : platforms) {
                if (!cp.canRun(task)) {
                    continue;
                }
                ExecutionScore scp = cp.getExecutionForecast(task, inData, outData);
                if (scp != null) {
                    if (DEBUG_ENABLED) {
                        LOGGER.debug("Forecasts to run task " + task.getId() + " on " + cp.getName() + " are:\n" + scp.toString(17));
                    }
                    if (scp.compareTo(bestScore) > 0) {
                        bestScore = scp;
                        bestPlatform = cp;
                    }
                }
            }
        }
        if (bestPlatform != null) {
            if (INFO_ENABLED) {
                LOGGER.info("Task " + task.getId() + " will run on computing platform " + bestPlatform.getName());
            }
            bestPlatform.submitTask(task);
        } else {
            LOGGER.error("There's no computing platform able to run task " + task.getId());
        }
    }
}
