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
import es.bsc.mobile.data.DataManager.DataStatus;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.types.Implementation;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.data.DataInstance;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.access.WriteAccess;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.types.profile.ImplementationProfile;

import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.types.JobProfile;
import es.bsc.mobile.types.calc.MinMax;

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;

public abstract class LocalComputingPlatform<T extends Resource> extends ComputingPlatformImplementation<T> {

    private final TreeMap<String, String> defaultProfiles = new TreeMap<>();
    private ImplementationProfile[] coreProfiles = new ImplementationProfile[0];
    private ImplementationProfile[][] implProfiles = new ImplementationProfile[0][];
    private LinkedList<es.bsc.mobile.types.Implementation>[] compatibleImpls = new LinkedList[0];

    private final static TreeSet<String> futureData = new TreeSet<>();

    protected int[] tasksToProcess = new int[CoreManager.getCoreCount()];

    public LocalComputingPlatform(RuntimeHandler rh, String name) {
        super(rh, name);
    }

    @Override
    public void defineDefaultProfiles(String profiles) {
        readMap(profiles);
    }

    @Override
    public final void resizeCores() {
        int coreCount = CoreManager.getCoreCount();
        int oldCoreCount = this.implProfiles.length;
        ImplementationProfile[] coreProfiles = new ImplementationProfile[coreCount];
        ImplementationProfile[][] implProfiles = new ImplementationProfile[coreCount][];
        LinkedList<es.bsc.mobile.types.Implementation>[] compatibleImpls = new LinkedList[coreCount];
        int[] waitingTasks = new int[coreCount];
        System.arraycopy(this.coreProfiles, 0, coreProfiles, 0, oldCoreCount);
        System.arraycopy(this.tasksToProcess, 0, waitingTasks, 0, oldCoreCount);
        for (int coreId = 0; coreId < coreCount; coreId++) {
            es.bsc.mobile.runtime.types.Implementation[] impls = CoreManager.getCoreImplementations(coreId);
            int implCount = impls.length;
            implProfiles[coreId] = new ImplementationProfile[implCount];

            int implId = 0;
            if (coreId < oldCoreCount) {
                int oldImplCount = implProfiles[coreId].length;
                for (; implId < oldImplCount; implId++) {
                    implProfiles[coreId][implId] = this.implProfiles[coreId][implId];
                }
                compatibleImpls[coreId] = this.compatibleImpls[coreId];
            } else {
                compatibleImpls[coreId] = new LinkedList<>();
                coreProfiles[coreId] = new ImplementationProfile(coreId);
            }
            String methodSignature = CoreManager.getSignature(coreId);
            for (; implId < implCount; implId++) {
                Implementation impl = impls[implId];
                if (canRun(impls[implId])) {
                    String implSignature = impl.completeSignature(methodSignature);
                    String defaultProfile = defaultProfiles.get(implSignature);
                    if (defaultProfile != null) {
                        implProfiles[coreId][implId] = new ImplementationProfile(implId, defaultProfile);
                    } else {
                        implProfiles[coreId][implId] = new ImplementationProfile(implId);
                    }
                    compatibleImpls[coreId].add(impls[implId].getInternalImplementation());
                }
            }
        }
        this.tasksToProcess = waitingTasks;
        this.coreProfiles = coreProfiles;
        this.implProfiles = implProfiles;
        this.compatibleImpls = compatibleImpls;
    }

    protected abstract boolean canRun(Implementation impl);

    @Override
    public boolean canRun(Task task) {
        int coreId = task.getTaskParams().getCoreId();
        for (ImplementationProfile ip : implProfiles[coreId]) {
            if (ip != null) {
                return true;
            }
        }
        return false;
    }

    protected LinkedList<es.bsc.mobile.types.Implementation> getCompatibleImplementations(int coreId) {
        return compatibleImpls[coreId];
    }

    protected ImplementationProfile getCoreProfile(int coreId) {
        return coreProfiles[coreId];
    }

    protected ImplementationProfile getImplementationProfile(int coreId, int implId) {
        return implProfiles[coreId][implId];
    }

    @Override
    public ExecutionScore getExecutionForecast(Task t, LinkedList<ComputingPlatform.TaskData> inData, LinkedList<ComputingPlatform.TaskData> outData) {

        LinkedList<DataAndStatus> dataAndStatus = new LinkedList<>();
        for (ComputingPlatform.TaskData td : inData) {
            dataAndStatus.add(new DataAndStatus(td.getDataSize(), getDataStatus(td.getDataName())));
        }
        MinMax waitingTime = getWaitingForecast();
        int coreId = t.getTaskParams().getCoreId();
        ExecutionScore bestScore = null;
        for (es.bsc.mobile.types.Implementation impl : getCompatibleImplementations(coreId)) {
            MinMax time = getTimeForecast(dataAndStatus, impl);
            time.aggregate(waitingTime);
            MinMax energy = getEnergyForecast(dataAndStatus, impl);
            MinMax cost = getCostForecast(dataAndStatus, impl);
            ExecutionScore score = new ComputingPlatform.ExecutionScore(time, energy, cost);
            if (bestScore == null) {
                bestScore = score;
            } else if (score.compareTo(bestScore) > 0) {
                bestScore = score;
            }
        }
        return bestScore;
    }

    protected abstract MinMax getWaitingForecast();

    protected abstract MinMax getTimeForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl);

    protected abstract MinMax getEnergyForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl);

    protected abstract MinMax getCostForecast(LinkedList<DataAndStatus> inData, es.bsc.mobile.types.Implementation impl);

    public class DataAndStatus {

        private final MinMax size;
        private final DataStatus status;

        public DataAndStatus(MinMax size, DataStatus status) {
            this.size = size;
            this.status = status;
        }

    }

    @Override
    protected void registerDataFutureLocations(RegisteredParameter param) {
        DataInstance daId;
        switch (param.getDirection()) {
            case IN:
                daId = ((ReadAccess) param.getDAccess()).getReadDataInstance();
                futureData.add(daId.getRenaming());
                break;
            case INOUT:
                daId = ((ReadWriteAccess) param.getDAccess()).getReadDataInstance();
                futureData.add(daId.getRenaming());
                daId = ((ReadWriteAccess) param.getDAccess()).getWrittenDataInstance();
                futureData.add(daId.getRenaming());
                break;
            default:
                daId = ((WriteAccess) param.getDAccess()).getWrittenDataInstance();
                futureData.add(daId.getRenaming());
        }
    }

    @Override
    public final void endTask(int taskId, JobProfile jp, Node runner) {
        int coreId = jp.getCoreId();
        int implId = jp.getImplementationId();
        coreProfiles[coreId].registerProfiledJob(jp);
        implProfiles[coreId][implId].registerProfiledJob(jp);
    }

    private TreeMap<String, String> readMap(String input) {
        String key = null;
        String value = null;
        StringBuilder substring = new StringBuilder();
        int openPars = 0;
        int openBracets = 0;
        int openCurlyBracets = -1;
        for (char c : input.toCharArray()) {
            switch (c) {
                case '[':
                    openBracets++;
                    substring.append("[");
                    break;
                case ']':
                    openBracets--;
                    substring.append("]");
                    break;
                case '(':
                    openPars++;
                    substring.append("(");
                    break;
                case ')':
                    openPars--;
                    substring.append(")");
                    break;
                case '{':
                    openCurlyBracets++;
                    if (openCurlyBracets > 0) {
                        substring.append("{");
                    }
                    break;
                case '}':
                    openCurlyBracets--;
                    if (openCurlyBracets == -1) {
                        value = substring.toString();
                        defaultProfiles.put(key, value);
                        return defaultProfiles;
                    } else {
                        substring.append("}");
                    }
                    break;
                case ',':
                    if (openBracets == 0 && openCurlyBracets == 0 && openPars == 0) {
                        value = substring.toString();
                        defaultProfiles.put(key, value);
                        key = null;
                        value = null;
                        substring = new StringBuilder();
                    } else {
                        substring.append(",");
                    }
                    break;
                case '=':
                    if (openBracets == 0 && openCurlyBracets == 0 && openPars == 0) {
                        key = substring.toString();
                        substring = new StringBuilder();
                    } else {
                        substring.append("=");
                    }
                    break;
                case ' ':
                    if (openBracets != 0 || openCurlyBracets != 0 || openPars != 0) {
                        substring.append(" ");
                    }
                    break;
                default:
                    substring.append(c);
            }

        }
        return defaultProfiles;
    }
}
