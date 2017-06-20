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
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.node.RuntimeNode;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.messages.Message;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class NewJob extends Message<RuntimeNode> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Jobs");
    private static final boolean LOG_ENABLED = LOGGER.isInfoEnabled();

    private Job job;

    public NewJob() {
    }

    public NewJob(Job job) {
        super();
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(job);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        job = (Job) in.readObject();
    }

    @Override
    public void handleMessage(Connection source, RuntimeNode handler) {
        if (LOG_ENABLED) {
            LOGGER.info("New job " + job + " arrived.");
        }
        CommunicationManager.receivedNotification(source);
        handler.newJob(job);
    }

    @Override
    public String toString() {
        return "job " + job.getId() + " execution request";
    }
}
