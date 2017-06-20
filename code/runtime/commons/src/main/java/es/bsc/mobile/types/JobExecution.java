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
package es.bsc.mobile.types;

import es.bsc.mobile.annotations.Parameter;
import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.data.access.DaAccess;
import es.bsc.mobile.data.access.RAccess;
import es.bsc.mobile.data.access.RWAccess;
import es.bsc.mobile.data.access.WAccess;
import es.bsc.mobile.exceptions.UnexpectedDirectionException;
import java.util.LinkedList;

public abstract class JobExecution implements Comparable<JobExecution> {

    private static final String UNEXPECTED_DIRECTION = "Unexpected direction ";

    private static final String FOR_PARAM = " for parameter ";
    private static final String FOR_TARGET = " for target object";
    private static final String FULL_STOP = ".";

    private static final int BYTE_SIZE = 1;
    private static final int SHORT_SIZE = 2;
    private static final int CHAR_SIZE = 2;
    private static final int INT_SIZE = 4;
    private static final int FLOAT_SIZE = 4;
    private static final int LONG_SIZE = 8;
    private static final int DOUBLE_SIZE = 8;
    private static final int BOOL_SIZE = 1;

    public static final int TARGET_PARAM_IDX = -1;
    public static final int RESULT_PARAM_IDX = -2;

    private final Job job;

    public JobExecution(Job job) {
        this.job = job;
    }

    public final Job getJob() {
        return job;
    }

    public final void checkParameterExistence(int paramId) {
        DaAccess dataAccess;
        JobParameter jp = job.getParams()[paramId];
        switch (jp.getType()) {
            case OBJECT:
                JobParameter.ObjectJobParameter ojp = (JobParameter.ObjectJobParameter) jp;
                dataAccess = ojp.getDataAccess();
                break;
            case FILE:
                JobParameter.FileJobParameter fjp = (JobParameter.FileJobParameter) jp;
                dataAccess = fjp.getDataAccess();
                break;
            default:
                paramExists(jp);
                return;
        }
        String data;
        switch (jp.getDirection()) {
            case IN:
                data = ((RAccess) dataAccess).getReadDataInstance();
                break;
            case INOUT:
                data = ((RWAccess) dataAccess).getReadDataInstance();
                break;
            default:
                // case OUT
                paramExists(jp);
                return;
        }
        requestParamDataExistence(data, paramId, new ParameterExistence(jp, paramId));
    }

    public final void checkTargetExistence() throws UnexpectedDirectionException {
        JobParameter.ObjectJobParameter target = job.getTarget();
        if (target != null) {
            String data;
            DaAccess dataAccess = target.getDataAccess();
            switch (target.getDirection()) {
                case IN:
                    data = ((RAccess) dataAccess).getReadDataInstance();
                    break;
                case INOUT:
                    data = ((RWAccess) dataAccess).getReadDataInstance();
                    break;
                default:
                    // case OUT
                    throw new UnexpectedDirectionException("Unexpected direction " + target.getDirection() + " for target object ");
            }

            requestParamDataExistence(data, TARGET_PARAM_IDX, new ParameterExistence(target, TARGET_PARAM_IDX));
        } else {
            paramExists(target);
        }
    }

    protected abstract void requestParamDataExistence(String dataId, int paramId, DataManager.DataExistenceListener listener);

    public abstract LinkedList<Implementation> getCompatibleImplementations();

    private class ParameterExistence implements DataManager.DataExistenceListener {

        private final JobParameter jp;
        private final int paramId;

        public ParameterExistence(JobParameter jp, int paramId) {
            this.jp = jp;
            this.paramId = paramId;
        }

        @Override
        public void paused() {
        }

        @Override
        public void exists() {
            paramExists(jp);
        }

        @Override
        public void exists(Class<?> type, Object value) {
            if (paramId == TARGET_PARAM_IDX) {
                job.forwardTargetValue(value);
            } else {
                job.forwardParamValue(paramId, type, value);
            }
            paramExists(jp);
        }
    }

    private void paramExists(JobParameter jp) {
        paramDataExists(jp);
        if (job.createdParam()) {
            allParamDataExists();
        }
    }

    public abstract void paramDataExists(JobParameter jp);

    public abstract void allParamDataExists();

    public void obtainJobParameter(int paramId) throws UnexpectedDirectionException {
        JobParameter jp = job.getParams()[paramId];
        String daIdin;
        String daIdout = null;
        switch (jp.getType()) {
            case BOOLEAN:
                job.setParamSizeIn(paramId, BOOL_SIZE);
                job.setParamValue(paramId, boolean.class, ((JobParameter.BooleanJobParameter) jp).getValue(), true);
                break;
            case CHAR:
                job.setParamSizeIn(paramId, CHAR_SIZE);
                job.setParamValue(paramId, char.class, ((JobParameter.CharJobParameter) jp).getValue(), true);
                break;
            case STRING:
                String stringValue = ((JobParameter.StringJobParameter) jp).getValue();
                job.setParamSizeIn(paramId, stringValue.length());
                job.setParamValue(paramId, String.class, stringValue, true);
                break;
            case BYTE:
                job.setParamSizeIn(paramId, BYTE_SIZE);
                job.setParamValue(paramId, byte.class, ((JobParameter.ByteJobParameter) jp).getValue(), true);
                break;
            case SHORT:
                job.setParamSizeIn(paramId, SHORT_SIZE);
                job.setParamValue(paramId, short.class, ((JobParameter.ShortJobParameter) jp).getValue(), true);
                break;
            case INT:
                job.setParamSizeIn(paramId, INT_SIZE);
                job.setParamValue(paramId, int.class, ((JobParameter.IntegerJobParameter) jp).getValue(), true);
                break;
            case LONG:
                job.setParamSizeIn(paramId, LONG_SIZE);
                job.setParamValue(paramId, long.class, ((JobParameter.LongJobParameter) jp).getValue(), true);
                break;
            case FLOAT:
                job.setParamSizeIn(paramId, FLOAT_SIZE);
                job.setParamValue(paramId, float.class, ((JobParameter.FloatJobParameter) jp).getValue(), true);
                break;
            case DOUBLE:
                job.setParamSizeIn(paramId, DOUBLE_SIZE);
                job.setParamValue(paramId, double.class, ((JobParameter.DoubleJobParameter) jp).getValue(), true);
                break;
            case FILE:
                switch (jp.getDirection()) {
                    case IN:
                        daIdin = ((RAccess) ((JobParameter.FileJobParameter) jp).getDataAccess()).getReadDataInstance();
                        daIdout = daIdin;
                        break;
                    case INOUT:
                        daIdin = ((RWAccess) ((JobParameter.FileJobParameter) jp).getDataAccess()).getReadDataInstance();
                        daIdout = ((RWAccess) ((JobParameter.FileJobParameter) jp).getDataAccess()).getWrittenDataInstance();
                        break;
                    case OUT:
                        daIdin = ((WAccess) ((JobParameter.FileJobParameter) jp).getDataAccess()).getWrittenDataInstance();
                        daIdout = daIdin;
                        job.setParamSizeIn(paramId, 0);
                        job.setParamValue(paramId, String.class, daIdin, true);
                        return;
                    default:
                        daIdin = "";
                        daIdout = "";
                        throw new UnexpectedDirectionException(UNEXPECTED_DIRECTION + jp.getDirection() + FOR_PARAM + paramId);
                }
                if (job.isParamValueSet(paramId)) {
                    if (job.setParamValue(paramId, job.getParamTypes()[paramId], job.getParamValues()[paramId], true)) {
                        job.endsTransfers();
                        allDataPresent();
                    }
                    obtainDataSize(daIdin, paramId, new LoadParamData(paramId));
                } else {
                    obtainDataAsFile(daIdin, daIdout, paramId, new LoadParamData(paramId));
                }
                break;
            case OBJECT:
                switch (jp.getDirection()) {
                    case IN:
                        daIdin = ((RAccess) ((JobParameter.ObjectJobParameter) jp).getDataAccess()).getReadDataInstance();
                        daIdout = daIdin;
                        break;
                    case INOUT:
                        daIdin = ((RWAccess) ((JobParameter.ObjectJobParameter) jp).getDataAccess()).getReadDataInstance();
                        daIdout = ((RWAccess) ((JobParameter.ObjectJobParameter) jp).getDataAccess()).getWrittenDataInstance();
                        break;
                    case OUT:
                        daIdin = ((WAccess) ((JobParameter.ObjectJobParameter) jp).getDataAccess()).getWrittenDataInstance();
                        daIdout = daIdin;
                        job.setParamSizeIn(paramId, 0);
                        job.setParamValue(paramId, null, null, true);
                        break;
                    default:
                        daIdin = "";
                        daIdout = "";
                        throw new UnexpectedDirectionException(UNEXPECTED_DIRECTION + jp.getDirection() + FOR_PARAM + paramId + FULL_STOP);
                }
                if (job.isParamValueSet(paramId)) {
                    if (job.setParamValue(paramId, job.getParamTypes()[paramId], job.getParamValues()[paramId], true)) {
                        job.endsTransfers();
                        allDataPresent();
                    }
                    obtainDataSize(daIdin, paramId, new LoadParamData(paramId));
                } else {
                    obtainDataAsObject(daIdin, daIdout, paramId, new LoadParamData(paramId));
                }
                break;
            default:
        }
    }

    public void obtainTargetObject() throws UnexpectedDirectionException {
        if (job.isTargetValueSet()) {
            Object value = job.getTargetValue();
            if (job.setTargetValue(value, true)) {
                job.endsTransfers();
                allDataPresent();
            }
        }
        JobParameter.ObjectJobParameter targetParam = job.getTarget();
        if (targetParam != null) {
            String daIdin;
            String daIdout;
            switch (targetParam.getDirection()) {
                case IN:
                    daIdin = ((RAccess) targetParam.getDataAccess()).getReadDataInstance();
                    daIdout = daIdin;
                    break;
                case INOUT:
                    daIdin = ((RWAccess) targetParam.getDataAccess()).getReadDataInstance();
                    daIdout = ((RWAccess) targetParam.getDataAccess()).getWrittenDataInstance();
                    break;
                default:
                    throw new UnexpectedDirectionException(UNEXPECTED_DIRECTION + targetParam.getDirection() + FOR_TARGET + FULL_STOP);
            }
            obtainDataAsObject(daIdin, daIdout, TARGET_PARAM_IDX, new LoadParamData(TARGET_PARAM_IDX));
        } else {
            job.setTargetSizeIn(0);
            if (job.setTargetValue(null, true)) {
                job.endsTransfers();
                allDataPresent();
            }
        }
    }

    protected abstract void obtainDataSize(String dataId, int paramId, LoadParamData listener);

    protected abstract void obtainDataAsObject(String dataId, String dataRenaming, int paramId, LoadParamData listener);

    protected abstract void obtainDataAsFile(String dataId, String dataRenaming, int paramId, LoadParamData listener);

    public class LoadParamData implements DataManager.DataOperationListener {

        private final int paramId;
        private boolean toCount = true;

        public LoadParamData(int paramId) {
            this.paramId = paramId;
        }

        @Override
        public void paused() {
        }

        @Override
        public void setSize(long value) {
            int missingParams;
            if (paramId == TARGET_PARAM_IDX) {
                missingParams = job.setTargetSizeIn(value);
            } else {
                missingParams = job.setParamSizeIn(paramId, value);
            }
            if (missingParams == 0) {
                completed();
            }
        }

        public void skipLoadValue() {
            if (paramId == TARGET_PARAM_IDX) {
                if (job.setTargetValue(null, toCount)) {
                    allDataPresent();
                }
            } else if (job.setParamValue(paramId, null, null, toCount)) {
                allDataPresent();
            }
        }

        @Override
        public void setValue(Class<?> type, Object value) {
            if (paramId == TARGET_PARAM_IDX) {
                if (job.setTargetValue(value, toCount)) {
                    job.endsTransfers();
                    allDataPresent();
                }
            } else if (job.setParamValue(paramId, type, value, toCount)) {
                job.endsTransfers();
                allDataPresent();
            }
            toCount = false;
        }

        @Override
        public String toString() {
            switch (paramId) {
                case TARGET_PARAM_IDX:
                    return "input version of the target object for job " + job.getTaskId();
                default:
                    return "input version of the parameter " + paramId + " for job " + job.getTaskId();
            }
        }
    }

    public abstract void allDataPresent();

    public abstract void prepareJobParameter(int paramId);

    public abstract void prepareTargetObject();

    public abstract void prepareResult();

    public abstract void allDataReady();

    public abstract void executeOn(Object id, Implementation impl);

    public abstract void failedExecution();

    public abstract void finishedExecution();

    public void storeJobParameter(int paramId) {
        JobParameter jp = job.getParams()[paramId];
        DaAccess dataAccess;
        switch (jp.getType()) {
            case OBJECT:
                JobParameter.ObjectJobParameter ojp = (JobParameter.ObjectJobParameter) jp;
                dataAccess = ojp.getDataAccess();
                break;
            case FILE:
                JobParameter.FileJobParameter fjp = (JobParameter.FileJobParameter) jp;
                dataAccess = fjp.getDataAccess();
                break;
            default:
                job.setParamSizeOut(paramId, 0);
                return;
        }
        String data;
        switch (jp.getDirection()) {
            case IN:
                job.setParamSizeOut(paramId, 0);
                return;
            case INOUT:
                data = ((RWAccess) dataAccess).getWrittenDataInstance();
                break;
            default:
                // case OUT
                data = ((WAccess) dataAccess).getWrittenDataInstance();
                break;
        }
        if (jp.getType() == Parameter.Type.OBJECT) {
            storeObject(data, job.getParamValues()[paramId], new StoreDataListener(paramId));
        } else {
            storeFile(data, (String) job.getParamValues()[paramId], new StoreDataListener(paramId));
        }
    }

    public void storeTarget() {
        if (job.getTarget() != null) {
            if (job.getTarget().getDirection() == Parameter.Direction.INOUT) {
                String data = ((RWAccess) job.getTarget().getDataAccess()).getWrittenDataInstance();
                storeObject(data, job.getTargetValue(), new StoreDataListener(TARGET_PARAM_IDX));
            }
        } else {
            int missingSizes = job.setTargetSizeOut(0);
            if (missingSizes == 0) {
                completed();
            }
        }
    }

    public void storeResult() {
        if (job.getResult() != null) {
            String data = ((WAccess) job.getResult().getDataAccess()).getWrittenDataInstance();
            storeObject(data, job.getResultValue(), new StoreDataListener(RESULT_PARAM_IDX));
        } else {
            int missingSizes = job.setResultSize(0);
            if (missingSizes == 0) {
                completed();
            }
        }
    }

    protected abstract void storeObject(String dataId, Object value, DataManager.DataOperationListener listener);

    protected abstract void storeFile(String dataId, String location, DataManager.DataOperationListener listener);

    private class StoreDataListener implements DataManager.DataOperationListener {

        private final int param;

        public StoreDataListener(int param) {
            this.param = param;
        }

        @Override
        public void paused() {
        }

        @Override
        public void setSize(long value) {
            int missingSizes;
            switch (param) {
                case RESULT_PARAM_IDX:
                    missingSizes = job.setResultSize(value);
                    break;
                case TARGET_PARAM_IDX:
                    missingSizes = job.setTargetSizeOut(value);
                    break;
                default:
                    missingSizes = job.setParamSizeOut(param, value);
            }
            if (missingSizes == 0) {
                completed();
            }
        }

        @Override
        public void setValue(Class<?> type, Object value) {
        }

        @Override
        public String toString() {
            switch (param) {
                case RESULT_PARAM_IDX:
                    return "result object for job " + job.getId();
                case TARGET_PARAM_IDX:
                    return "output version of the target object for job " + job.getId();
                default:
                    return "output version of the parameter " + param + " for job " + job.getId();
            }
        }
    }

    public abstract void completed();

    @Override
    public int compareTo(JobExecution o) {
        return Integer.compare(this.job.getTaskId(), o.job.getTaskId());
    }

}
