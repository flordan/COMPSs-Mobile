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

import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobExecution;
import es.bsc.mobile.types.Method;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobExecutor implements Runnable {

    private final BlockingQueue<JobExecution> executionSource;

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Jobs");
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();
    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private static final String UNDEFINED_CLASS = "Undefined className.";
    private static final String UNDEFINED_METHOD = "Undefined methodName.";

    public JobExecutor(BlockingQueue<JobExecution> executionSource) {
        this.executionSource = executionSource;
    }

    @Override
    public void run() {
        long threadId = Thread.currentThread().getId();
        Thread.currentThread().setName("JobExecutor" + threadId);
        while (true) {
            JobExecution jobExecution = null;
            try {
                jobExecution = executionSource.take();
            } catch (InterruptedException ex) {
                continue;
            }
            if (jobExecution == null) {
                return;
            }
            Job job = jobExecution.getJob();
            if (INFO_ENABLED) {
                LOGGER.info("JobExecutor" + threadId + " runs job " + job.getId());
                if (DEBUG_ENABLED) {
                    LOGGER.debug(job);
                }
            }
            job.startExecution();
            Method impl = (Method) job.getSelectedImplementation();
            String className = impl.getDeclaringClass();
            if (className == null) {
                LOGGER.error(UNDEFINED_CLASS);
                continue;
            }
            String methodName = impl.getMethodName();
            if (methodName == null) {
                LOGGER.error(UNDEFINED_METHOD);
                continue;
            }

            // Use reflection to get the requested method
            Class<?> methodClass;
            java.lang.reflect.Method method;
            try {
                methodClass = Class.forName(className);
            } catch (ClassNotFoundException e) {
                LOGGER.error(e);
                continue;
            } catch (SecurityException e) {
                LOGGER.error(e);
                continue;
            }
            try {
                method = methodClass.getMethod(methodName, job.getParamTypes());
            } catch (NoSuchMethodException e) {
                LOGGER.error(e);
                continue;
            }
            job.startExecution();
            // Invoke the requested method
            try {
                Object o = method.invoke(job.getTargetValue(), job.getParamValues());
                job.setResultValue(o);
            } catch (Exception e) {
                jobExecution.failedExecution();
                LOGGER.error("Error invoking requested method", e);
                continue;
            }
            job.endsExecution();
            if (INFO_ENABLED) {
                LOGGER.info("Job " + job.getId() + " execution has finished.");
            }
            jobExecution.finishedExecution();
        }
    }

}
