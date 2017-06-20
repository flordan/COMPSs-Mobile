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

public class Constraints implements Externalizable {

    private static final long serialVersionUID = 1L;

    public static final String UNASSIGNED = "[unassigned]";

    protected String processorArchitecture;
    protected int processorCoreCount;
    protected float memoryPhysicalSize;
    protected float storageElemSize;
    protected String operatingSystemType;

    public Constraints() {
        processorArchitecture = UNASSIGNED;
        processorCoreCount = 0;
        memoryPhysicalSize = 0;
        storageElemSize = 0;
        operatingSystemType = UNASSIGNED;
    }

    public Constraints(es.bsc.mobile.annotations.Constraints cts) {
        if (cts != null) {
            this.processorArchitecture = cts.processorArchitecture();
            this.processorCoreCount = cts.processorCoreCount();
            this.memoryPhysicalSize = cts.memoryPhysicalSize();
            this.storageElemSize = cts.storageElemSize();
            this.operatingSystemType = cts.operatingSystemType();
        }
    }

    public String processorArchitecture() {
        return this.processorArchitecture;
    }

    public int processorCoreCount() {
        return this.processorCoreCount;
    }

    public void setProcessorCoreCount(int coreCount) {
        this.processorCoreCount = coreCount;
    }

    public float memoryPhysicalSize() {
        return this.memoryPhysicalSize;
    }

    public float storageElemSize() {
        return this.storageElemSize;
    }

    public String operatingSystemType() {
        return this.operatingSystemType;
    }

    public void join(Constraints defaultConstraints) {
        if (defaultConstraints != null) {
            if (this.processorArchitecture.compareTo(UNASSIGNED) == 0) {
                this.processorArchitecture = defaultConstraints
                        .processorArchitecture();
            }
            if (this.processorCoreCount == 0) {
                this.processorCoreCount = defaultConstraints
                        .processorCoreCount();
            }
            if (this.memoryPhysicalSize == 0) {
                this.memoryPhysicalSize = defaultConstraints
                        .memoryPhysicalSize();
            }
            if (this.storageElemSize == 0) {
                this.storageElemSize = defaultConstraints.storageElemSize();
            }
            if (this.operatingSystemType.compareTo(UNASSIGNED) == 0) {
                this.operatingSystemType = defaultConstraints
                        .operatingSystemType();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Processor ").append("\n");
        sb.append("\t Architecture: ").append(this.processorArchitecture).append("\n");
        sb.append("\t CoreCount: ").append(this.processorCoreCount).append("\n");
        sb.append("Memory ").append("\n");
        sb.append("\t Size: ").append(this.memoryPhysicalSize).append("\n");
        sb.append("Storage ").append("\n");
        sb.append("\t Size: ").append(this.storageElemSize).append("\n");
        sb.append("OS: ").append(this.operatingSystemType).append("\n");
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(processorArchitecture);
        out.writeInt(processorCoreCount);
        out.writeFloat(memoryPhysicalSize);
        out.writeFloat(storageElemSize);
        out.writeUTF(operatingSystemType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        processorArchitecture = in.readUTF();
        processorCoreCount = in.readInt();
        memoryPhysicalSize = in.readFloat();
        storageElemSize = in.readFloat();
        operatingSystemType = in.readUTF();
    }

}
