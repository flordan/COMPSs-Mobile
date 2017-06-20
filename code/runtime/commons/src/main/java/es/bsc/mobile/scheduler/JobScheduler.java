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
import es.bsc.mobile.types.Implementation;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobExecution;
import es.bsc.mobile.types.JobParameter;
import es.bsc.mobile.utils.JobSets;
import java.util.LinkedList;
import java.util.TreeSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JobScheduler {

    private static final String INVALID_JOB = "Invalid job description.";

    public static final int TARGET_PARAM_IDX = -1;
    public static final int RESULT_PARAM_IDX = -2;

    protected static final Logger LOGGER = LogManager.getLogger("Runtime.Jobs");
    protected static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private final JobSets jobs;

    public JobScheduler() {
        this.jobs = new JobSets();
    }

    public abstract void start();

    public abstract void stop();

    public void newJob(JobExecution job) {
        jobs.newJob(job);
        notifyJobArrival(job);
    }

    protected abstract void notifyJobArrival(JobExecution job);

    protected LinkedList<JobExecution> pollUnanalyzedJobs() {
        return jobs.pollUnanalizedJobs();
    }

    protected void analyzeDependencies(JobExecution execution) {
        Job job = execution.getJob();
        jobs.pendingJob(execution);
        try {
            checkDependenciesExistence(execution);
        } catch (UnexpectedDirectionException ex) {
            LOGGER.error(INVALID_JOB, ex);
        }
    }

    protected void restoreUnanalizedJobs(LinkedList<JobExecution> oldNewJobs) {
        jobs.restoreUnanalizedJobs(oldNewJobs);
    }

    /**
     * Checks the existence of all the data inputs required to execute a job.
     *
     * @param execution Job to check
     * @return true if all dependencies are solved
     * @throws Exception
     */
    private void checkDependenciesExistence(JobExecution execution) throws UnexpectedDirectionException {
        //Job Parameter Analysis
        Job job = execution.getJob();
        for (int paramId = 0; paramId < job.getParams().length; paramId++) {
            execution.checkParameterExistence(paramId);
        }

        //Job Target Analysis
        execution.checkTargetExistence();
    }

    public abstract void notifyParamValueExistence(Job job, JobParameter jp);

    public void dependencyFreeJob(JobExecution je) {
        Job j = je.getJob();
        j.startProfiling();
        jobs.dependencyFreePendingJob(je);
        notifyAllParamValuesExistence(je);
    }

    protected abstract void notifyAllParamValuesExistence(JobExecution job);

    protected TreeSet<JobExecution> takeDependencyFreeJobs() {
        return jobs.takeDependencyFreeJobs();
    }

    protected void restoreDependencyFreeJobs(TreeSet<JobExecution> dependencyFree) {
        jobs.restoreDependencyFreeJobs(dependencyFree);
    }

    public void allValuesObtained(JobExecution je) {
        jobs.dataPresent(je);
        notifyAllValuesObtained(je);
    }

    protected abstract void notifyAllValuesObtained(JobExecution job);

    protected TreeSet<JobExecution> takeDataPresentJobs() {
        return jobs.getDataPresentJobs();
    }

    protected void restoreDataPresentJobs(TreeSet<JobExecution> presentData) {
        jobs.restoreDataPresentJobs(presentData);
    }

    public void allValuesReady(JobExecution je) {
        jobs.dataReady(je);
        notifyAllValuesReady(je);
    }

    protected abstract void notifyAllValuesReady(JobExecution job);

    protected void executesJob(JobExecution job) {
        jobs.executingJob(job);
    }

    public void executedJob(JobExecution execution) {
        jobs.executedJob(execution);
        Implementation impl = execution.getJob().getSelectedImplementation();
        long threadId = Thread.currentThread().getId();
        notifyJobExecution(threadId, execution);
    }

    public abstract void notifyJobExecution(long threadId, JobExecution job);

    protected LinkedList<JobExecution> pollExecutedJobs() {
        return jobs.pollExecutedJobs();
    }

    public void completedJob(JobExecution execution) {
        notifyCompletion(execution);
        Job job = execution.getJob();
        job.done();
        if (DEBUG_ENABLED) {
            LOGGER.debug(job.getJobProfile());
        }
    }

    public abstract void notifyCompletion(JobExecution job);

}
