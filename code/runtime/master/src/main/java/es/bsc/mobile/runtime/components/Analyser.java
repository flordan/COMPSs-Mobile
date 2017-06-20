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
package es.bsc.mobile.runtime.components;

import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.data.DataInstance;
import es.bsc.mobile.runtime.types.data.access.DataAccess;
import es.bsc.mobile.runtime.types.data.access.DataAccess.Action;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.access.WriteAccess;
import es.bsc.mobile.runtime.types.data.parameter.BasicTypeParameter;
import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.types.exceptions.ElementNotFoundException;
import es.bsc.mobile.runtime.types.requests.analyser.*;
import es.bsc.mobile.runtime.utils.Graph;
import es.bsc.mobile.runtime.utils.GraphEdge;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Analyser extends Thread {

    private static final Logger LOGGER = LogManager.getLogger("Runtime");

    private static final Logger TASK_LOGGER = LogManager.getLogger("Runtime.Tasks");
    private static final boolean TASK_ENABLED = TASK_LOGGER.isInfoEnabled();

    private static final Logger CHECKPOINTING_LOGGER = LogManager.getLogger("Runtime.Checkpointing");
    private static final boolean CHECKPOINTING_ENABLED = CHECKPOINTING_LOGGER.isInfoEnabled();
    private static final int CHECKER_TIMEOUT = 500;

    private boolean keepGoing;
    protected final LinkedBlockingQueue<AnalyserRequest> requestQueue;

    private final Graph<Integer, Task, DataAccess> depGraph;
    private final TreeMap<Integer, Task> writers;

    private static final int BLOCK_LIMIT = 3;

    private final HashMap<Integer, Integer> taskToBlock = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, DataInstance>> blocksData;
    private int currentBlock = 0;
    private int currentBlockSize = 0;

    private HashMap<Integer, DataInstance> currentBlockData = new HashMap<>();
    private final HashMap<String, SavingData> savingData = new HashMap<>();

    private final RuntimeHandler rh;

    public Analyser(RuntimeHandler rh) {
        keepGoing = true;
        this.rh = rh;
        requestQueue = new LinkedBlockingQueue<>();
        blocksData = new HashMap<>();
        depGraph = new Graph<>();
        writers = new TreeMap<>();
        blocksData.put(currentBlock, currentBlockData);
        this.setName("Access Analyzer");
    }

    @Override
    public void run() {
        AnalyserRequest request;
        while (keepGoing) {
            try {
                request = requestQueue.poll(CHECKER_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                continue;
            }
            if (request != null) {
                try {
                    request.dispatch(rh, this);
                } catch (Exception e) {
                    LOGGER.error("Error dispatching request " + request + ".", e);
                }
            }
        }
    }

    public void shutdown() {
        keepGoing = false;
    }

    public void addTaskToGraph(Task task) throws ElementNotFoundException {
        if (CHECKPOINTING_ENABLED) {
            CHECKPOINTING_LOGGER.info("Task " + task.getId() + " included in block " + currentBlock);
        }
        depGraph.addNode(task.getId(), task);
        Parameter[] params = task.getTaskParams().getParameters();
        boolean hasDependencies = false;
        for (Parameter param : params) {
            if (!(param instanceof BasicTypeParameter)) {
                RegisteredParameter rp = (RegisteredParameter) param;
                DataAccess da = rp.getDAccess();
                Integer dataId = da.getDataID();
                Task lastWriter = writers.get(dataId);

                if (da.getAction() != Action.WRITE
                        && lastWriter != null && depGraph.get(lastWriter.getId()) != null && lastWriter.getId() != task.getId()) {
                    if (TASK_ENABLED) {
                        TASK_LOGGER.info("Task " + task.getId() + " depends on task" + lastWriter.getId() + " for data " + da.getDataID());
                    }
                    depGraph.addEdge(lastWriter, task, da);
                    hasDependencies = true;
                }
                if (da.getAction() != Action.READ) {
                    writers.put(dataId, task);
                    DataInstance daId;
                    if (da.getAction() == Action.UPDATE) {
                        ReadWriteAccess rwa = (ReadWriteAccess) da;
                        daId = rwa.getWrittenDataInstance();
                    } else {
                        //Is a WRITE ACCESS
                        WriteAccess wa = (WriteAccess) da;
                        daId = wa.getWrittenDataInstance();
                    }
                    if (CHECKPOINTING_ENABLED) {
                        CHECKPOINTING_LOGGER.info("Task block " + currentBlock + " generates data " + daId);
                    }
                    currentBlockData.put(daId.getDataId(), daId);
                }
            }
        }
        taskToBlock.put(task.getId(), currentBlock);
        currentBlockSize++;
        if (currentBlockSize == BLOCK_LIMIT) {
            currentBlock++;
            currentBlockSize = 0;
            currentBlockData = new HashMap<>();
            blocksData.put(currentBlock, currentBlockData);
        }

        task.dependenciesAnalysed(hasDependencies);
        if (TASK_ENABLED) {
            TASK_LOGGER.info("Task " + task.getId() + (hasDependencies ? " has dependencies with other tasks." : " is dependency-free."));
        }
    }

    public void addRequest(AnalyserRequest req) {
        requestQueue.add(req);
    }

    /*
     ****************************************
     ****** PUBLIC CALLS TO ANALYSER ********
     ****************************************
     */
    public void taskFinished(int taskId) {
        if (TASK_ENABLED) {
            TASK_LOGGER.info("Task " + taskId + " has finished its execution.");
        }
        if (CHECKPOINTING_ENABLED) {
            CHECKPOINTING_LOGGER.info("Task " + taskId + " has finished its execution.");
        }
        Task endedTask = depGraph.get(taskId);
        endedTask.endsExecution(false);

        int block = taskToBlock.get(taskId);
        Parameter[] params = endedTask.getTaskParams().getParameters();
        int accessedData = 0;
        for (Parameter param : params) {
            if (param.getDirection() != Direction.IN && !(param instanceof BasicTypeParameter)) {
                RegisteredParameter rp = (RegisteredParameter) param;
                DataAccess da = rp.getDAccess();
                HashMap<Integer, DataInstance> blockData = this.blocksData.get(block);
                DataInstance finalData = blockData.get(da.getDataID());
                if (finalData == null) {
                    continue;
                }
                DataInstance daId;
                if (param.getDirection() == Direction.OUT) {
                    daId = ((WriteAccess) da).getWrittenDataInstance();
                } else {
                    daId = ((ReadWriteAccess) da).getWrittenDataInstance();
                }
                if (daId.getVersionId() == finalData.getVersionId()) {
                    if (CHECKPOINTING_ENABLED) {
                        CHECKPOINTING_LOGGER.info("Data " + daId + " is an output of task block " + block + " and has to be saved for checkpointing purposes.");
                    }
                    saveData(daId, endedTask);
                    accessedData++;
                }
            }
        }

        if (accessedData == 0) {
            if (TASK_ENABLED) {
                TASK_LOGGER.info("Task " + endedTask.getId() + " has completed its execution.");
            }
            checkCompleted(endedTask);
        }
    }

    private void saveData(DataInstance daId, Task endedTask) {
        String dataId = daId.getRenaming();
        savingData.put(dataId, new SavingData(daId, endedTask));
        rh.obtainDataAsFile(dataId, dataId, new SaveData(dataId));
    }

    public void savedData(String rename) {
        if (CHECKPOINTING_ENABLED) {
            CHECKPOINTING_LOGGER.info("Data " + rename + " has been saved for checkpointing purposes.");
        }
        SavingData sv = savingData.remove(rename);
        Task producer = sv.task;
        DataInstance daId = sv.data;
        Integer block = taskToBlock.get(producer.getId());

        if (block == null) {
            return;
        }

        LinkedList<GraphEdge<Task, DataAccess>> edges = new LinkedList<>();
        for (GraphEdge<Task, DataAccess> edge : producer.getSuccessors()) {
            DataAccess da = edge.getLabel();
            DataInstance label;
            if (da.getAction() == Action.READ) {
                label = ((ReadAccess) da).getReadDataInstance();
            } else {
                //Action==RW
                label = ((ReadWriteAccess) da).getReadDataInstance();
            }
            if (label.getRenaming().compareTo(daId.getRenaming()) == 0) {
                edges.add(edge);
            }
        }

        for (GraphEdge<Task, DataAccess> edge : edges) {
            edge.getOrigin().removeSuccessor(edge);
            edge.getTarget().removePredecessor(edge);
        }

        HashMap<Integer, DataInstance> datablock = blocksData.get(block);
        datablock.remove(daId.getDataId());
        checkCompleted(producer);
        if (datablock.isEmpty() && block != currentBlock) {
            blocksData.remove(block);
        }
    }

    private void checkCompleted(Task task) {
        if (task.isExecuted() && task.getSuccessors().isEmpty()) {
            if (CHECKPOINTING_ENABLED) {
                CHECKPOINTING_LOGGER.info("Task " + task.getId() + " has been completed and none its outputs won't be required to be created again.");
            }
            depGraph.removeNode(task.getId());
            taskToBlock.remove(task.getId());
            Iterator<GraphEdge<Task, DataAccess>> edges = task.getPredecessors().iterator();
            while (edges.hasNext()) {
                GraphEdge<Task, DataAccess> edge = edges.next();
                edge.getOrigin().removeSuccessor(edge);
                edges.remove();
                checkCompleted(edge.getOrigin());
            }
        }
    }

    private static class SavingData {

        private final DataInstance data;
        private final Task task;

        private SavingData(DataInstance daId, Task t) {
            data = daId;
            task = t;
        }
    }

    private class SaveData implements DataManager.DataOperationListener {

        private final String dataId;

        public SaveData(String dataId) {
            this.dataId = dataId;
        }

        @Override
        public void paused() {
        }

        @Override
        public void setSize(long value) {
        }

        @Override
        public void setValue(Class<?> type, Object value) {
            SavedData rte = new SavedData(dataId);
            addRequest(rte);
        }

        @Override
        public String toString() {
            return "checkpointing.";
        }
    }
}
