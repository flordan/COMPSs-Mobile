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
package es.bsc.mobile.scheduler;

import es.bsc.mobile.exceptions.UnexpectedDirectionException;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobExecution;
import es.bsc.mobile.types.JobParameter;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

public class BasicScheduler extends JobScheduler implements Runnable {

    private static final String INPUT_ERR = "Error managing input dependencies.";

    private int runningTokens;
    private final PriorityQueue<JobExecution> READY_EXECUTIONS = new PriorityQueue<JobExecution>();

    private static final long PERIOD = 20000;
    private boolean sleeps;
    boolean areChanges = false;
    private boolean keepGoing;
    private final Semaphore runnableSem;
    private final LinkedList<JobExecution> runnableJobs = new LinkedList<>();

    public BasicScheduler(int parallelTokens) {
        super();
        keepGoing = true;
        runnableSem = new Semaphore(0);
        sleeps = false;
        runningTokens = parallelTokens;
    }

    @Override
    public void start() {
        Thread thread = new Thread(this);
        thread.setName("Basic Scheduler");
        thread.start();
    }

    @Override
    public void stop() {
        keepGoing = false;
        notifyChanges();
        synchronized (runnableJobs) {
            runnableJobs.clear();
            runnableSem.release(runnableSem.getQueueLength());
        }
    }

    @Override
    public void run() {
        while (keepGoing) {
            boolean isIdleTime = true;
            TreeSet<JobExecution> jobsToObtainData = new TreeSet<>();
            TreeSet<JobExecution> jobsToPrepareData = new TreeSet<>();

            do {
                areChanges = false;
                dealWithExecutedJobs();
                analyzeNewJobs();
                if (isIdleTime) {
                    TreeSet<JobExecution> dependencyFree = takeDependencyFreeJobs();
                    selectJobsToObtainData(dependencyFree, jobsToObtainData);
                    restoreDependencyFreeJobs(dependencyFree);
                    TreeSet<JobExecution> presentData = takeDataPresentJobs();
                    selectJobsToPrepareData(presentData, jobsToPrepareData);
                    restoreDataPresentJobs(presentData);
                    isIdleTime = isIdleTime();
                }
            } while (areChanges);

            for (JobExecution j : jobsToObtainData) {
                try {
                    obtainDataForJob(j);
                } catch (UnexpectedDirectionException e) {
                    LOGGER.error(INPUT_ERR, e);
                }
            }

            for (JobExecution j : jobsToPrepareData) {
                try {
                    prepareDataForJob(j);
                } catch (UnexpectedDirectionException e) {
                    LOGGER.error(INPUT_ERR, e);
                }
            }

            sleeps = true;
            if (!areChanges) {
                try {
                    synchronized (this) {
                        this.wait(PERIOD);
                    }
                } catch (InterruptedException ex) {
                }
            }
            sleeps = false;
        }
    }

    private void notifyChanges() {
        areChanges = true;
        if (sleeps) {
            synchronized (this) {
                this.notify();
            }
        }
    }

    @Override
    public void notifyJobArrival(JobExecution job) {
        notifyChanges();
    }

    @Override
    public void notifyParamValueExistence(Job job, JobParameter jp) {
        //Do nothing. Waits until all values are available to fetch them.
    }

    @Override
    public void notifyAllParamValuesExistence(JobExecution job) {
        notifyChanges();
    }

    @Override
    public void notifyAllValuesObtained(JobExecution job) {
        notifyChanges();
    }

    @Override
    public void notifyAllValuesReady(JobExecution job) {
        synchronized (READY_EXECUTIONS) {
            if (runningTokens > 0) {
                job.executeOn(null, job.getCompatibleImplementations().getFirst());
                runningTokens--;
            } else {
                READY_EXECUTIONS.add(job);
            }
        }
    }

    @Override
    public void notifyJobExecution(long threadId, JobExecution job) {
        synchronized (READY_EXECUTIONS) {
            JobExecution je = READY_EXECUTIONS.poll();
            if (je == null) {
                runningTokens++;
            } else {
                je.executeOn(null, je.getCompatibleImplementations().getFirst());
            }
        }
        notifyChanges();
    }

    @Override
    public void notifyCompletion(JobExecution job) {
        //Do nothing
    }

    private void analyzeNewJobs() {
        LinkedList<JobExecution> newJobs = pollUnanalyzedJobs();
        for (JobExecution j : newJobs) {
            this.analyzeDependencies(j);
        }
        newJobs.clear();
        restoreUnanalizedJobs(newJobs);
    }

    private void selectJobsToObtainData(TreeSet<JobExecution> candidates, TreeSet<JobExecution> selectedJobs) {
        JobExecution selectedJob = null;
        while (isIdleTime() && !candidates.isEmpty()) {
            selectedJob = candidates.pollFirst();
            selectedJobs.add(selectedJob);
            if (areChanges) {
                return;
            }
        }
    }

    /**
     * Obtains the input values required for running a task.
     *
     * If the data values are not present in the local node it asks for them to
     * the system.
     *
     * @param je JobExecution whose dependencies must be loaded
     *
     * @throws UnexpectedDirectionException
     */
    private void obtainDataForJob(JobExecution je) throws UnexpectedDirectionException {
        Job job = je.getJob();
        job.startTransfers();
        for (int i = 0; i < job.getParams().length; i++) {
            je.obtainJobParameter(i);
        }

        je.obtainTargetObject();

    }

    private void selectJobsToPrepareData(TreeSet<JobExecution> candidates, TreeSet<JobExecution> selectedJobs) {
        JobExecution selectedJob = null;
        while (isIdleTime() && !candidates.isEmpty()) {
            for (JobExecution j : candidates) {
                selectedJob = j;
                break;
            }
            selectedJobs.add(selectedJob);
            candidates.remove(selectedJob);
            if (areChanges) {
                return;
            }
        }
    }

    private void prepareDataForJob(JobExecution je) throws UnexpectedDirectionException {
        Job job = je.getJob();
        for (int i = 0; i < job.getParams().length; i++) {
            je.prepareJobParameter(i);
        }

        je.prepareTargetObject();
        je.prepareResult();
    }

    private void dealWithExecutedJobs() {
        LinkedList<JobExecution> executed = pollExecutedJobs();
        for (JobExecution job : executed) {
            storeDataDependencies(job);
        }
    }

    private void storeDataDependencies(JobExecution je) {
        Job job = je.getJob();
        //Output data notification
        for (int i = 0; i < job.getParams().length; i++) {
            je.storeJobParameter(i);
        }

        //Target object notification
        je.storeTarget();

        //Return object notification
        je.storeResult();

    }

    //Comproba si hi ha alguna CPU que no sigui utilitzada durant X temps
    private boolean isIdleTime() {
        return true;
    }

}
