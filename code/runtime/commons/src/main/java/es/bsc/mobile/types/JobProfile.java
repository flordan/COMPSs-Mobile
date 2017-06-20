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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;

public class JobProfile implements Externalizable {

    private static final long serialVersionUID = 1L;
    
    private static final long MS_TO_NS = 1_000_000l;

    private static final int SIZES_PER_PARAM = 2;
    private static final int SIZES_PER_RESULT = 1;
    private static final String IN = " IN ";
    private static final String OUT = " OUT ";

    private String jobId;
    private int coreId;
    private int implementationId;
    private long tReady = -1;
    private long tTransferStart = -1;
    private long tTransferEnd = -1;
    private long tStart = -1;
    private long tEnd = -1;
    private long tDone = -1;
    private long[] paramsSizeIn;
    private long[] paramsSizeOut;
    private long targetSizeOut = 0L;
    private long targetSizeIn = 0L;
    private long resultSize = 0L;
    private AtomicInteger missingSizes = new AtomicInteger(SIZES_PER_PARAM + SIZES_PER_RESULT);
    private long energy = 0l;

    public JobProfile() {
    }

    public JobProfile(Job j) {
        this.jobId = j.getId();
        this.coreId = j.getImplementations()[0].getCoreElementId();
        this.implementationId = j.getImplementations()[0].getImplementationId();
    }

    public int getCoreId() {
        return coreId;
    }

    public int getImplementationId() {
        return this.implementationId;
    }

    public void setImplementationId(int implementationId) {
        this.implementationId = implementationId;
    }

    public String getJobId() {
        return jobId;
    }

    public long getTReady() {
        return tReady;
    }

    public int setParamsLength(int length) {
        this.paramsSizeIn = new long[length];
        this.paramsSizeOut = new long[length];
        return missingSizes.addAndGet(SIZES_PER_PARAM * length);
    }

    public int getParamsLength() {
        return this.paramsSizeIn.length;
    }

    public int setParamSize(boolean in, int param, long size) {
        if (in) {
            this.paramsSizeIn[param] = size;
        } else {
            this.paramsSizeOut[param] = size;
        }
        return missingSizes.decrementAndGet();
    }

    public long getParamsSize(boolean in, int param) {
        if (in) {
            return this.paramsSizeIn[param];
        } else {
            return this.paramsSizeOut[param];
        }
    }

    public int setTargetSize(boolean in, long size) {
        if (in) {
            this.targetSizeIn = size;
        } else {
            this.targetSizeOut = size;
        }
        return missingSizes.decrementAndGet();
    }

    public long getTargetSize(boolean in) {
        if (in) {
            return this.targetSizeIn;
        } else {
            return this.targetSizeOut;
        }
    }

    public int setResultSize(long size) {
        this.resultSize = size;
        return missingSizes.decrementAndGet();
    }

    public long getResultSize() {
        return this.resultSize;
    }

    public void ready() {
        tReady = System.currentTimeMillis() * MS_TO_NS;
    }

    public void startsTransfers() {
        tTransferStart = System.currentTimeMillis() * MS_TO_NS;
    }

    public void endsTransfers() {
        tTransferEnd = System.currentTimeMillis() * MS_TO_NS;
    }

    public void startsExecution() {
        tStart = System.currentTimeMillis() * MS_TO_NS;
    }

    public void startsExecution(long timeStamp) {
        tStart = timeStamp;
    }

    public void endsExecution() {
        tEnd = System.currentTimeMillis() * MS_TO_NS;
    }

    public long getTransferTime() {
        return tTransferEnd - tTransferStart;
    }

    public long getWaitingTime() {
        return tStart - tReady;
    }

    public long getExecutionTime() {
        return tEnd - tStart;
    }

    public void done() {
        tDone = System.currentTimeMillis() * MS_TO_NS;
    }

    public void setExecutionEnergy(long consumption) {
        this.energy = consumption;
    }

    public long getExecutionEnergy() {
        return this.energy;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(jobId);
        out.writeInt(coreId);
        out.writeLong(tReady);
        out.writeLong(tTransferStart);
        out.writeLong(tTransferEnd);
        out.writeLong(tStart);
        out.writeLong(tEnd);
        out.writeLong(tDone);
        out.writeLong(energy);
        out.writeInt(missingSizes.get());
        //Posar el tamany de les dades
        int paramsSize = paramsSizeIn.length;
        out.writeInt(paramsSize);
        for (int i = 0; i < paramsSize; i++) {
            out.writeLong(paramsSizeIn[i]);
            out.writeLong(paramsSizeOut[i]);
        }
        out.writeLong(targetSizeIn);
        out.writeLong(targetSizeOut);
        out.writeLong(resultSize);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = in.readUTF();
        coreId = in.readInt();
        tReady = in.readLong();
        tTransferStart = in.readLong();
        tTransferEnd = in.readLong();
        tStart = in.readLong();
        tEnd = in.readLong();
        tDone = in.readLong();
        energy = in.readLong();
        missingSizes = new AtomicInteger(in.readInt());
        //Llegir el tamany de les dades
        int paramsSize = in.readInt();
        paramsSizeIn = new long[paramsSize];
        paramsSizeOut = new long[paramsSize];
        for (int i = 0; i < paramsSize; i++) {
            paramsSizeIn[i] = in.readLong();
            paramsSizeOut[i] = in.readLong();
        }
        targetSizeIn = in.readLong();
        targetSizeOut = in.readLong();
        resultSize = in.readLong();

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JOB " + this.jobId + " PROFILE\n");
        sb.append("CoreId: ").append(coreId).append("\n");
        sb.append("Wait time: ").append(tStart - tReady).append(" (ready at ").append(tReady).append(" and started at ").append(tStart).append(")\n");
        sb.append("Transfer time: ").append(tTransferEnd - tTransferStart).append(" (started at ").append(tTransferStart).append(" and finished at ").append(tTransferEnd).append(")\n");
        sb.append("Execution time: ").append(tEnd - tStart).append("\n");
        sb.append("Execution Energy: ").append(energy).append("\n");

        long in = 0L;
        long out = 0L;
        StringBuilder params = new StringBuilder();

        for (int i = 0; i < paramsSizeIn.length; i++) {
            in += paramsSizeIn[i];
            out += paramsSizeOut[i];
            params.append("\t Param ").append(i).append(":" + IN).append(paramsSizeIn[i]).append(OUT).append(paramsSizeOut[i]).append("\n");
        }
        in += targetSizeIn;
        out += targetSizeOut + resultSize;
        params.append("\t Target:" + IN).append(targetSizeIn).append(OUT).append(targetSizeOut).append("\n");
        params.append("\t Result: ").append(resultSize);
        sb.append("Transfer Sizes :" + IN).append(in).append(OUT).append(out).append("\n");
        sb.append(params.toString());
        return sb.toString();
    }
}
