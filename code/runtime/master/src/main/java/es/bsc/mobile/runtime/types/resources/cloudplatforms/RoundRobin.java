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
import es.bsc.mobile.types.JobProfile;

public class RoundRobin extends CloudPlatform {

    private RemoteResource[] resources = new RemoteResource[0];
    private int nextId = 0;

    public RoundRobin(RuntimeHandler rh, String name) {
        super(rh, name);
    }

    @Override
    public void addResource(RemoteResource res) {
        int oldResourceCount = this.resources.length;
        RemoteResource[] resources = new RemoteResource[oldResourceCount + 1];
        System.arraycopy(this.resources, 0, resources, 0, oldResourceCount);
        resources[oldResourceCount] = res;
        this.resources = resources;
    }

    @Override
    public void submitTask(Task task) {
        RemoteResource r = resources[nextId];
        nextId = (nextId) % resources.length;
        nextId++;
        submitTask(task, r);
        task.hasBeenOffloaded();
    }

    @Override
    public void finishedTaskNotification(int taskId, JobProfile jp, Node runner) {
        //Do nothing
    }
}
