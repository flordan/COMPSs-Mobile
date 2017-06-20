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
package es.bsc.opencl.wrapper.policies;

import es.bsc.opencl.wrapper.DeviceMemory;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import org.jocl.cl_command_queue;
import org.jocl.cl_context;

public class LastNotPendingAccess extends DeviceMemory {

    private final LinkedBlockingQueue<MemoryRegister> toRemove = new LinkedBlockingQueue<MemoryRegister>();

    public LastNotPendingAccess(long memoryFree, cl_context context, cl_command_queue queue) {
        super(memoryFree, context, queue);
    }

    @Override
    public DeviceMemory.MemoryRegister createRegister(String dataId, int size, boolean canWrite) {
        return new MemoryRegister(dataId, size, canWrite);
    }

    @Override
    public void pendingAccess(DeviceMemory.MemoryRegister reg) {
        MemoryRegister mr = (MemoryRegister) reg;
        if (!mr.isInvolvedInKernel()) {
            toRemove.remove(mr);
        }
        mr.involvedInKernel();
    }

    @Override
    public void performedAccess(DeviceMemory.MemoryRegister reg) {
        MemoryRegister mr = (MemoryRegister) reg;
        mr.kernelResolved();
        if (!mr.isInvolvedInKernel()) {
            toRemove.add(mr);
        }
    }

    @Override
    public DeviceMemory.MemoryRegister releaseRegister(long space) {
        Iterator<MemoryRegister> regIter = toRemove.iterator();
        while (regIter.hasNext()) {
            MemoryRegister reg = regIter.next();
            regIter.remove();
            return reg;
        }
        return null;
    }

    private class MemoryRegister extends DeviceMemory.MemoryRegister {

        private int involvedKernels;

        public MemoryRegister(String dataId, int size, boolean canWrite) {
            super(dataId, size, canWrite);
            involvedKernels = 0;
        }

        public final void involvedInKernel() {
            involvedKernels++;
        }

        public final void kernelResolved() {
            involvedKernels--;
        }

        public final boolean isInvolvedInKernel() {
            return involvedKernels > 0;
        }

        @Override
        public String toString() {
            return super.toString() + " is involved in " + involvedKernels + " kernels.";
        }
    }
}
