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
import es.bsc.mobile.annotations.Parameter.Type;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.TaskParameters;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.access.WriteAccess;
import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.types.profile.CoreProfile;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.types.JobProfile;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

public class DataLocality extends CloudPlatform {

    private final TreeMap<Integer, LinkedList<Data>> taskToInData = new TreeMap<>();
    private final TreeMap<Integer, LinkedList<TaskData>> taskToOutData = new TreeMap<>();
    private final TreeMap<Integer, RemoteResource> expectedExecutor = new TreeMap<>();
    private RemoteResource[] resources = new RemoteResource[0];
    private int nextResourceIdx = 0;

    public DataLocality(RuntimeHandler rh, String name) {
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
    public void submitTask(Task t) {
        RemoteResource sr = null;
        LinkedList<Data> inData = new LinkedList<>();
        taskToInData.put(t.getId(), inData);
        LinkedList<TaskData> outData = new LinkedList<>();
        taskToOutData.put(t.getId(), outData);
        analyseParameters(t.getTaskParams(), inData, outData);
        HashMap<RemoteResource, Long> hostScore = new HashMap<>();
        long topScore = -1;
        for (Data d : inData) {
            for (RemoteResource r : d.getResource()) {
                Long score = hostScore.get(r);
                if (score == null) {
                    score = 0L;
                }
                score += d.getSize();
                if (score > topScore) {
                    sr = r;
                    topScore = score;
                }
                hostScore.put(r, score);
            }
        }
        if (sr == null) {
            sr = resources[nextResourceIdx % resources.length];
            nextResourceIdx++;
        }
        expectedExecutor.put(t.getId(), sr);
        submitTask(t, sr);
        t.hasBeenOffloaded();

        for (Data d : inData) {
            d.addResourceReplica(sr);
        }
        for (TaskData td : outData) {
            td.data.addResourceReplica(sr);
        }

    }

    private void analyseParameters(TaskParameters tp, LinkedList<Data> inData, LinkedList<TaskData> outData) {
        int coreId = tp.getCoreId();
        Data d;
        CoreProfile profile = CoreManager.getCoreProfile(coreId);
        int paramCount = profile.getNumParams();
        int pIdx = 0;
        for (; pIdx < paramCount; pIdx++) {
            Parameter p = tp.getParameters()[pIdx];
            if (p.getType() == Type.OBJECT || p.getType() == Type.FILE) {

                RegisteredParameter rp = (RegisteredParameter) p;
                switch (rp.getDirection()) {
                    case IN:
                        ReadAccess ra = (ReadAccess) rp.getDAccess();
                        inData.add(Data.getData(ra.getReadDataInstance().getRenaming(), profile.getParamOutSize(pIdx).average()));
                        break;
                    case OUT:
                        WriteAccess wa = (WriteAccess) rp.getDAccess();
                        d = Data.getData(wa.getWrittenDataInstance().getRenaming(), profile.getParamOutSize(pIdx).average());
                        outData.add(new TaskData(d, pIdx));
                        break;
                    default:
                        ReadWriteAccess rwa = (ReadWriteAccess) rp.getDAccess();
                        inData.add(Data.getData(rwa.getReadDataInstance().getRenaming(), profile.getParamInSize(pIdx).average()));
                        d = Data.getData(rwa.getWrittenDataInstance().getRenaming(), profile.getParamOutSize(pIdx).average());
                        outData.add(new TaskData(d, pIdx));
                        break;
                }
            }
        }
        if (tp.hasTarget()) {
            RegisteredParameter rp = (RegisteredParameter) tp.getParameters()[pIdx];
            ReadWriteAccess rwa = (ReadWriteAccess) rp.getDAccess();
            inData.add(Data.getData(rwa.getReadDataInstance().getRenaming(), profile.getTargetInSize().average()));
            d = Data.getData(rwa.getWrittenDataInstance().getRenaming(), profile.getTargetOutSize().average());
            outData.add(new TaskData(d, TaskData.TARGET));
            pIdx++;
        }
        if (tp.hasReturn()) {
            RegisteredParameter rp = (RegisteredParameter) tp.getParameters()[pIdx];
            WriteAccess wa = (WriteAccess) rp.getDAccess();
            d = Data.getData(wa.getWrittenDataInstance().getRenaming(), profile.getResultSize().average());
            outData.add(new TaskData(d, TaskData.RESULT));
        }
    }

    @Override
    public void finishedTaskNotification(int taskId, JobProfile jp, Node runner) {
        RemoteResource expected = expectedExecutor.get(taskId);
        RemoteResource real = getResource(runner);
        //Update size values
        LinkedList<TaskData> outData = taskToOutData.get(taskId);
        for (TaskData td : outData) {
            if (td.param == TaskData.RESULT) {
                td.data.setSize(jp.getResultSize());
            } else if (td.param == TaskData.TARGET) {
                td.data.setSize(jp.getTargetSize(false));
            } else {
                td.data.setSize(jp.getParamsSize(false, td.param));
                if (real != expected) {
                    td.data.removeResourceReplica(expected);
                    td.data.addResourceReplica(real);
                }
            }
        }

        if (real != expected) {
            LinkedList<Data> inData = taskToInData.get(taskId);
            for (Data d : inData) {
                d.removeResourceReplica(expected);
                d.addResourceReplica(real);
            }
        }
    }

    private RemoteResource getResource(Node n) {
        for (RemoteResource r : resources) {
            if (r.getNode().equals(n)) {
                return r;
            }
        }
        return null;
    }

    private static class TaskData {

        private static final int RESULT = -2;
        private static final int TARGET = -1;

        Data data;
        int param;

        public TaskData(Data d, int pIdx) {
            data = d;
            param = pIdx;
        }
    }

    private static class Data {

        private static final HashMap<String, Data> DATA = new HashMap<>();
        private final HashMap<RemoteResource, Integer> resourceCount = new HashMap<>();
        private long size;

        private Data() {
        }

        public static Data getData(String dataId, long expectedSize) {
            Data d = DATA.get(dataId);
            if (d == null) {
                d = new Data();
                d.size = expectedSize;
                DATA.put(dataId, d);
            }
            return d;
        }

        public Set<RemoteResource> getResource() {
            return resourceCount.keySet();
        }

        public void addResourceReplica(RemoteResource resource) {
            Integer replicas = resourceCount.get(resource);
            if (replicas == null) {
                resourceCount.put(resource, 1);
            } else {
                resourceCount.put(resource, replicas + 1);
            }
        }

        public void removeResourceReplica(RemoteResource resource) {
            Integer replicas = resourceCount.remove(resource);
            if (replicas > 1) {
                resourceCount.put(resource, replicas - 1);
            }
        }

        public void setSize(long length) {
            size = length;
        }

        public long getSize() {
            return size;
        }
    }
}
