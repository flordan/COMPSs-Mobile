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

import es.bsc.mobile.runtime.types.Task;
import java.util.HashMap;
import java.util.LinkedList;

public class StaticAssignationManager {

    private static HashMap<String, LinkedList<Integer>> platformToTasks = new HashMap<>();
    private static ComputingPlatform[] taskToPlatform = new ComputingPlatform[0];
    public static ComputingPlatform defaultPlatform;

    static {
        //Set LinkedList with taskIds to each platform
    }

    public static void setDefaultPlatform(ComputingPlatform cp) {
        defaultPlatform = cp;
        for (int i = 0; i < taskToPlatform.length; i++) {
            if (taskToPlatform[i] == null) {
                taskToPlatform[i] = cp;
            }
        }
    }

    public static void registerPlatform(ComputingPlatform cp) {
        LinkedList<Integer> tasks = platformToTasks.remove(cp.getName());
        if (tasks == null) {
            return;
        }

        for (int taskId : tasks) {
            taskToPlatform[taskId] = cp;
        }
    }

    public static boolean runsTask(Task t, ComputingPlatform cp) {
        int taskId = t.getId();
        if (taskId < taskToPlatform.length && taskToPlatform[t.getId()] != null) {
            return taskToPlatform[t.getId()] == cp;
        } else {
            return defaultPlatform == cp;
        }
    }

    public static ComputingPlatform getPredefinedPlatform(Task t) {
        int taskId = t.getId();
        if (taskId < taskToPlatform.length && taskToPlatform[t.getId()] != null) {
            return taskToPlatform[t.getId()];
        } else {
            return defaultPlatform;
        }
    }

}
