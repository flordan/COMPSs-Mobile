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

import es.bsc.mobile.types.JobParameter.ObjectJobParameter;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

public class Job implements Externalizable, Comparable<Job> {

    private static final long serialVersionUID = 1L;

    private String id;
    private int taskId;
    private Implementation[] implementations;
    private Implementation selectedImpl;
    private int missingExistences;
    private JobParameter[] params;

    private int missingValues;
    private Class<?>[] types;
    private Object[] values;

    private ObjectJobParameter target;
    private Object targetValue;
    private ObjectJobParameter result;
    private Object resultValue;

    private int platformId;
    private JobProfile profile;

    public Job() {
    }

    public Job(int taskId, int platformId) {
        id = UUID.randomUUID().toString();
        this.taskId = taskId;
        this.platformId = platformId;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public int getTaskId() {
        return this.taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getPlatformId() {
        return platformId;
    }

    public Implementation[] getImplementations() {
        return implementations;
    }

    public void setImplementations(Implementation[] implementations) {
        this.implementations = implementations;
    }

    public Implementation getSelectedImplementation() {
        return this.selectedImpl;
    }

    public void selectImplementation(Implementation implementation) {
        this.selectedImpl = implementation;
        profile.setImplementationId(implementation.getImplementationId());
    }

    public JobParameter[] getParams() {
        return params;
    }

    public void setParams(JobParameter[] params) {
        this.params = params;
        int numParams = params.length;
        this.types = new Class<?>[numParams];
        this.values = new Object[numParams];
        this.missingExistences = numParams + 1;
        this.missingValues = numParams + 1;
    }

    public synchronized boolean setParamValue(int paramId, Class<?> type, Object value, boolean decrement) {
        types[paramId] = type;
        values[paramId] = value;
        if (decrement) {
            missingValues--;
        }
        return (missingValues == 0) && decrement;
    }

    public void forwardParamValue(int paramId, Class<?> type, Object value) {
        types[paramId] = type;
        values[paramId] = value;
    }

    public boolean isParamValueSet(int paramId) {
        return types[paramId] != null;
    }

    public Class<?>[] getParamTypes() {
        return types;
    }

    public Object[] getParamValues() {
        return values;
    }

    public ObjectJobParameter getTarget() {
        return target;
    }

    public void setTarget(ObjectJobParameter target) {
        this.target = target;
    }

    public Object getTargetValue() {
        return targetValue;
    }

    public boolean setTargetValue(Object targetValue, boolean decrement) {
        this.targetValue = targetValue;
        if (decrement) {
            missingValues--;
        }
        return (missingValues == 0) && decrement;
    }

    public void forwardTargetValue(Object value) {
        this.targetValue = value;
    }

    public boolean isTargetValueSet() {
        return targetValue != null;
    }

    public ObjectJobParameter getResult() {
        return result;
    }

    public void setResult(ObjectJobParameter result) {
        this.result = result;
    }

    public Object getResultValue() {
        return this.resultValue;
    }

    public void setResultValue(Object value) {
        this.resultValue = value;
    }

    public synchronized boolean createdParam() {
        missingExistences--;
        return missingExistences == 0;
    }

    @Override
    public int compareTo(Job j) {
        return id.compareTo(j.id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Job) {
            return ((Job) obj).id.compareTo(id) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (this.id == null) ? 0 : this.id.hashCode();
    }

    public JobProfile getJobProfile() {
        return profile;
    }

    public int setParamSizeIn(int param, long size) {
        return profile.setParamSize(true, param, size);
    }

    public int setParamSizeOut(int param, long size) {
        return profile.setParamSize(false, param, size);
    }

    public int setTargetSizeIn(long size) {
        return profile.setTargetSize(true, size);
    }

    public int setTargetSizeOut(long size) {
        return profile.setTargetSize(false, size);
    }

    public int setResultSize(long size) {
        return profile.setResultSize(size);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Job ");
        sb.append("id: ").append(id).append("\n");
        sb.append("\tImplementations\n");
        for (Implementation impl : implementations) {
            sb.append("\t\t ").append(impl).append("\n");
        }
        sb.append("\tParameters:\n");
        for (JobParameter jp : this.params) {
            sb.append("\t\t").append(jp).append("\n");
        }
        if (target != null) {
            sb.append("\t\t").append(target).append("\n");
        }
        if (result != null) {
            sb.append("\t\t").append(result).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(id);
        out.writeInt(taskId);
        out.writeInt(platformId);
        out.writeObject(implementations);
        out.writeObject(params);
        out.writeObject(target);
        out.writeObject(result);
        if (profile != null) {
            out.writeObject(profile);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readUTF();
        taskId = in.readInt();
        platformId = in.readInt();
        implementations = (Implementation[]) in.readObject();
        JobParameter[] params = (JobParameter[]) in.readObject();
        setParams(params);
        values = new Object[params.length];
        types = new Class<?>[params.length];
        target = (ObjectJobParameter) in.readObject();
        result = (ObjectJobParameter) in.readObject();
        if (in.available() > 0) {
            profile = (JobProfile) in.readObject();
        }
    }

    public void startProfiling() {
        this.profile = new JobProfile(this);
        this.profile.setParamsLength(values.length);
        this.profile.ready();
    }

    public void startTransfers() {
        this.profile.startsTransfers();
    }

    public void endsTransfers() {
        this.profile.endsTransfers();
    }

    public void startExecution() {
        this.profile.startsExecution();
    }

    public void startExecution(long timeStamp) {
        this.profile.startsExecution(timeStamp);
    }

    public void endsExecution() {
        this.profile.endsExecution();
    }

    public void done() {
        this.profile.done();
    }

    public void setConsumption(long consumption) {
        this.profile.setExecutionEnergy(consumption);
    }

}
