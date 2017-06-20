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
package es.bsc.mobile.utils;

import es.bsc.mobile.types.JobExecution;
import java.util.LinkedList;
import java.util.TreeSet;

public class JobSets {

    /**
     * List of tasks that can not run on the node
     */
    private final LinkedList<JobExecution> unable;

    /**
     * List of tasks whose dependencies haven't been checked
     */
    private LinkedList<JobExecution> unanalized;
    private final LinkedList<JobExecution> unanalizedBackup;
    /**
     * Set of tasks that can be executed on the node but there have some data
     * dependencies pending to be solved
     */
    private final TreeSet<JobExecution> pending;

    /**
     * Set of tasks whose input data is already created but are not present in
     * the worker
     */
    private TreeSet<JobExecution> dependencyFree;
    private final TreeSet<JobExecution> dependencyFreeBackup;

    /**
     * Set of tasks whose input data is already created and present in the
     * worker
     */
    private TreeSet<JobExecution> dataPresent;
    private final TreeSet<JobExecution> dataPresentBackup;

    /**
     * Set of tasks whose input data is already created and present in the
     * worker and ready to be used by the implementation
     */
    private TreeSet<JobExecution> dataReady;
    private final TreeSet<JobExecution> dataReadyBackup;

    /**
     * List of tasks that have already been executed
     */
    private LinkedList<JobExecution> executed;

    public JobSets() {
        unable = new LinkedList<>();
        unanalized = new LinkedList<>();
        unanalizedBackup = new LinkedList<>();
        pending = new TreeSet<>();
        dependencyFree = new TreeSet<>();
        dependencyFreeBackup = new TreeSet<>();
        dataPresent = new TreeSet<JobExecution>();
        dataPresentBackup = new TreeSet<JobExecution>();
        dataReady = new TreeSet<JobExecution>();
        dataReadyBackup = new TreeSet<JobExecution>();
        executed = new LinkedList<>();
    }

    public void newJob(JobExecution j) {
        synchronized (unanalizedBackup) {
            unanalized.add(j);
        }
    }

    public LinkedList<JobExecution> getUnanalizedJobs() {
        return unanalized;
    }

    public LinkedList<JobExecution> pollUnanalizedJobs() {
        LinkedList<JobExecution> old;
        synchronized (unanalizedBackup) {
            old = unanalized;
            unanalized = unanalizedBackup;
        }
        return old;
    }

    public void restoreUnanalizedJobs(LinkedList<JobExecution> oldNewJobs) {
        synchronized (unanalizedBackup) {
            unanalized = oldNewJobs;
            unanalized.addAll(unanalizedBackup);
            unanalizedBackup.clear();
        }
    }

    public void pendingJob(JobExecution j) {
        synchronized (pending) {
            pending.add(j);
        }
    }

    public void dependencyFreeJob(JobExecution j) {
        synchronized (dependencyFreeBackup) {
            dependencyFree.add(j);
        }
    }

    public void dependencyFreePendingJob(JobExecution j) {
        synchronized (pending) {
            synchronized (dependencyFreeBackup) {
                pending.remove(j);
                dependencyFree.add(j);
            }
        }
    }

    public TreeSet<JobExecution> getDependencyFreeJobs() {
        return dependencyFree;
    }

    public TreeSet<JobExecution> takeDependencyFreeJobs() {
        TreeSet<JobExecution> old;
        synchronized (dependencyFreeBackup) {
            old = dependencyFree;
            dependencyFree = dependencyFreeBackup;
        }
        return old;
    }

    public void restoreDependencyFreeJobs(TreeSet<JobExecution> oldPending) {
        synchronized (dependencyFreeBackup) {
            dependencyFree = oldPending;
            dependencyFree.addAll(dependencyFreeBackup);
            dependencyFreeBackup.clear();
        }
    }

    public void dataPresent(JobExecution je) {
        synchronized (dataPresentBackup) {
            dataPresent.add(je);
        }
    }

    public TreeSet<JobExecution> getDataPresentJobs() {
        return dataPresent;
    }

    public TreeSet<JobExecution> takeDataPresentJobs() {
        TreeSet<JobExecution> old;
        synchronized (dataPresentBackup) {
            old = dataPresent;
            dataPresent = dataPresentBackup;
        }
        return old;
    }

    public void restoreDataPresentJobs(TreeSet<JobExecution> oldPending) {
        synchronized (dataPresentBackup) {
            dataPresent = oldPending;
            dataPresentBackup.addAll(dataPresentBackup);
            dataPresentBackup.clear();
        }
    }

    public void dataReady(JobExecution je) {
        synchronized (dataReadyBackup) {
            dataReadyBackup.add(je);
        }
    }

    public TreeSet<JobExecution> getDataReadyJobs() {
        return dataReady;
    }

    public TreeSet<JobExecution> takeDataReadyJobs() {
        TreeSet<JobExecution> old;
        synchronized (dataReadyBackup) {
            old = dataReady;
            dataReady = dataReadyBackup;
        }
        return old;
    }

    public void restoreDataReadyJobs(TreeSet<JobExecution> oldPending) {
        synchronized (dataReadyBackup) {
            dataReady = oldPending;
            dataReadyBackup.addAll(dataReady);
            dataReadyBackup.clear();
        }
    }

    public void executingJob(JobExecution job) {
        synchronized (dependencyFreeBackup) {
            dependencyFree.remove(job);
        }
    }

    public void executedJob(JobExecution job) {
        synchronized (executed) {
            executed.add(job);
        }
    }

    public LinkedList<JobExecution> getExecutedJobs() {
        return executed;
    }

    public LinkedList<JobExecution> pollExecutedJobs() {
        LinkedList<JobExecution> old;
        synchronized (executed) {
            old = executed;
            executed = new LinkedList<>();
        }
        return old;
    }

    public String debug(String prefix) {

        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("Jobs unable to execute:").append("\n");
        for (JobExecution je : unable) {
            sb.append(prefix).append("-").append(je.getJob().getId()).append("\n");
        }
        sb.append(prefix).append("Jobs pending for data creations:").append("\n");
        for (JobExecution je : pending) {
            sb.append(prefix).append("-").append(je.getJob().getId()).append("\n");
        }
        synchronized (dependencyFree) {
            sb.append(prefix).append("Jobs ready to execute:").append("\n");
            for (JobExecution je : dependencyFree) {
                sb.append(prefix).append("-").append(je.getJob().getId()).append("\n");
            }

            sb.append(prefix).append("Jobs running:").append("\n");

            sb.append(prefix).append("Jobs already executed:").append("\n");
            for (JobExecution je : executed) {
                sb.append(prefix).append("-").append(je.getJob().getId()).append("\n");
            }
            sb.append(prefix).append("Jobs finalized:").append("\n");

        }
        return sb.toString();
    }

}
