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
package es.bsc.mobile.types.messages.runtime;

import es.bsc.comm.Connection;
import es.bsc.comm.Node;
import es.bsc.mobile.node.RuntimeNode;
import es.bsc.mobile.types.JobProfile;
import es.bsc.mobile.types.messages.Message;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobCompleted extends Message<RuntimeNode> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Jobs");
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();
    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private int taskId;
    private int platformId;
    private JobProfile jp;
    private Node executor;

    public JobCompleted() {

    }

    public JobCompleted(int taskId, int platformId, JobProfile jp, Node n) {
        this.taskId = taskId;
        this.platformId = platformId;
        this.jp = jp;
        this.executor = n;
    }

    public int getTaskId() {
        return this.taskId;
    }

    public JobProfile getJobProfile() {
        return this.jp;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(taskId);
        out.writeInt(platformId);
        out.writeObject(jp);
        out.writeObject(executor);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        taskId = in.readInt();
        platformId = in.readInt();
        jp = (JobProfile) in.readObject();
        executor = (Node) in.readObject();
    }

    @Override
    public void handleMessage(Connection source, RuntimeNode handler) {
        if (INFO_ENABLED) {
            LOGGER.info("Job " + taskId + " completed.");
            if (DEBUG_ENABLED) {
                LOGGER.debug(jp.toString());
            }
        }
        completeSource(executor, source);
        handler.jobFinished(taskId, platformId, jp, executor);
    }

    @Override
    public String toString() {
        return "job " + jp.getJobId() + " completion";
    }
}
