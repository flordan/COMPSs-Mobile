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

import es.bsc.comm.Node;
import es.bsc.mobile.runtime.types.profile.CoreProfile;
import es.bsc.mobile.runtime.types.resources.ComputingPlatform;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.types.JobProfile;
import java.util.LinkedList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EndTask extends ExecutorRequest {

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Offload");
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();
    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private final int taskId;
    private final int platformId;
    private final JobProfile jobProfile;
    private final Node runner;

    public EndTask(int taskId, int platformId, JobProfile profile, Node runner) {
        this.taskId = taskId;
        this.platformId = platformId;
        this.jobProfile = profile;
        this.runner = runner;
    }

    public int getTaskId() {
        return taskId;
    }

    public JobProfile getJobProfile() {
        return jobProfile;
    }

    public Node getRunner() {
        return runner;
    }

    @Override
    public void dispatch(LinkedList<ComputingPlatform> platforms) {
        int coreId = jobProfile.getCoreId();
        CoreProfile coreProfile = CoreManager.getCoreProfile(coreId);
        coreProfile.registerProfiledJob(jobProfile);
        for (ComputingPlatform cp : platforms) {
            if (cp.getId() == platformId) {
                if (INFO_ENABLED) {
                    if (DEBUG_ENABLED) {
                        LOGGER.debug(jobProfile);
                    }
                }
                cp.endTask(taskId, jobProfile, runner);
            }
        }
    }
}
