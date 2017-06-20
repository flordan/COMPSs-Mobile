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
package es.bsc.opencl.wrapper;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.jocl.CL;
import org.jocl.Pointer;
import org.jocl.Sizeof;
import org.jocl.cl_command_queue;
import org.jocl.cl_context;
import org.jocl.cl_event;
import org.jocl.cl_mem;

public abstract class DeviceMemory {

    private final cl_context context;
    private final cl_command_queue queue;

    private long memoryFree;
    private final HashMap<String, MemoryRegister> inMemoryValues = new HashMap<>();
    private final LinkedBlockingQueue<MemoryRequest> requestQueue = new LinkedBlockingQueue<>();

    public DeviceMemory(long memoryFree, cl_context context, cl_command_queue queue) {
        this.memoryFree = memoryFree;
        this.context = context;
        this.queue = queue;
    }

    public void setOnMemory(Device.OpenCLMemoryPrepare listener, String dataId, Object object, Class<?> objectClass, int size, boolean isRead, boolean canWrite) {
        MemoryRegister register = inMemoryValues.get(dataId);
        if (register == null) {
            int byteSize = baseSize(objectClass) * size;
            if (memoryFree < byteSize) {
                if (!releaseMemory(byteSize)) {
                    MemoryRequest mr = new MemoryRequest(listener, dataId, object, objectClass, size, isRead, canWrite);
                    requestQueue.add(mr);
                    //Operation will be performed when there's space enough
                    return;
                }
            }
            register = createRegister(dataId, byteSize, canWrite);
            if (isRead) {
                register.writeValue(objectClass, size, object);
            }
            inMemoryValues.put(dataId, register);
            memoryFree -= register.getSize();

        }
        pendingAccess(register);
        listener.completed(register);
    }

    public void replaceCreatorEvent(Object parValue, cl_event overridingEvent) {
        MemoryRegister register = (MemoryRegister) parValue;
        register.replaceLoadingEvent(overridingEvent);
    }

    public void retrieveValue(Object parValue, Object target, boolean wasWritten) {
        MemoryRegister register = (MemoryRegister) parValue;
        if (wasWritten) {
            int size = 1;
            Class<?> targetClass = target.getClass();
            Object sizeObject = target;
            while (targetClass.isArray()) {
                size *= Array.getLength(sizeObject);
                sizeObject = Array.get(sizeObject, 0);
                targetClass = targetClass.getComponentType();
            }
            register.retrieveValue(target, targetClass, size);
        }
        performedAccess(register);
    }

    protected abstract MemoryRegister createRegister(String dataId, int size, boolean canWrite);

    protected abstract void pendingAccess(MemoryRegister reg);

    protected abstract void performedAccess(MemoryRegister reg);

    private boolean releaseMemory(long space) {
        MemoryRegister register;
        while (space - memoryFree > 0) {
            register = this.releaseRegister(space - memoryFree);
            if (register == null) {
                return false;
            }
            inMemoryValues.remove(register.getDataId());
            memoryFree += register.getSize();
            register.destroy();
        }
        return true;
    }

    protected abstract MemoryRegister releaseRegister(long missingSpace);

    private static class MemoryRequest {

        private final Device.OpenCLMemoryPrepare listener;
        private final String valueRenaming;
        private final Object object;
        private final Class<?> objectClass;
        private final int size;
        private final boolean copyInput;
        private final boolean canWrite;

        public MemoryRequest(Device.OpenCLMemoryPrepare listener, String valueRenaming, Object object, Class<?> objectClass, int size, boolean copyInput, boolean canWrite) {
            this.listener = listener;
            this.valueRenaming = valueRenaming;
            this.object = object;
            this.objectClass = objectClass;
            this.size = size;
            this.copyInput = copyInput;
            this.canWrite = canWrite;
        }

        public Device.OpenCLMemoryPrepare getListener() {
            return listener;
        }

        public Object getObject() {
            return object;
        }

        public Class<?> getObjectClass() {
            return objectClass;
        }

        public int getSize() {
            return size;
        }

        public String getValueRenaming() {
            return valueRenaming;
        }

        public boolean isCanWrite() {
            return canWrite;
        }

        public boolean isCopyInput() {
            return copyInput;
        }
    }

    public abstract class MemoryRegister {

        private final cl_mem data;
        private final long size;
        private cl_event firstLoadingEvent;
        private cl_event loadingEvent;
        private final String dataId;

        public MemoryRegister(String dataId, int size, boolean canWrite) {
            this.size = size;
            int[] errCode = new int[1];
            this.data = CL.clCreateBuffer(context, canWrite ? CL.CL_MEM_READ_WRITE : CL.CL_MEM_READ_ONLY, size, null, errCode);
            this.dataId = dataId;
        }

        public final String getDataId() {
            return dataId;
        }

        public final long getSize() {
            return size;
        }

        public final cl_mem getBuffer() {
            return data;
        }

        public final void writeValue(Class<?> objectClass, int size, Object object) {
            int[] errout = new int[1];
            Object seq;
            if (object.getClass().getComponentType().isArray()) {
                seq = Array.newInstance(objectClass, size);
                flatten(object, seq, 0);
            } else {
                seq = object;
            }
            cl_event event = new cl_event();
            if (objectClass == boolean.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_char * size, Pointer.to((char[]) seq), 0, null, event);
            }
            if (objectClass == byte.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_char * size, Pointer.to((byte[]) seq), 0, null, event);
            }
            if (objectClass == char.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_short * size, Pointer.to((char[]) seq), 0, null, event);
            }
            if (objectClass == short.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_short * size, Pointer.to((short[]) seq), 0, null, event);
            }
            if (objectClass == int.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_int * size, Pointer.to((int[]) seq), 0, null, event);
            }
            if (objectClass == long.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_long * size, Pointer.to((long[]) seq), 0, null, event);
            }
            if (objectClass == float.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_float * size, Pointer.to((float[]) seq), 0, null, event);
            }
            if (objectClass == double.class) {
                CL.clEnqueueWriteBuffer(queue, data, CL.CL_FALSE, 0, Sizeof.cl_double * size, Pointer.to((double[]) seq), 0, null, event);
            }
            setLoadingEvent(event);
            CL.clFlush(queue);
        }

        public final void replaceLoadingEvent(cl_event overridingEvent) {
            this.firstLoadingEvent = this.loadingEvent;
            this.loadingEvent = overridingEvent;
        }

        public final void retrieveValue(Object target, Class<?> targetClass, int size) {
            CL.clFlush(queue);
            Object flatResult;
            if (target.getClass().getComponentType().isArray()) {
                flatResult = Array.newInstance(targetClass, size);
            } else {
                flatResult = target;
            }
            if (targetClass == boolean.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_char, Pointer.to((char[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == byte.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_char, Pointer.to((byte[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == char.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_short, Pointer.to((char[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == short.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_short, Pointer.to((short[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == int.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_int, Pointer.to((int[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == long.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_long, Pointer.to((long[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == float.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_float, Pointer.to((float[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            if (targetClass == double.class) {
                CL.clEnqueueReadBuffer(queue, data, CL.CL_TRUE, 0, size * Sizeof.cl_double, Pointer.to((double[]) flatResult), 1, new cl_event[]{loadingEvent}, null);
            }
            CL.clFlush(queue);
            if (target.getClass().getComponentType().isArray()) {
                expand(flatResult, target, 0);
            }
        }

        public final cl_event getLoadingEvent() {
            return loadingEvent;
        }

        public final void setLoadingEvent(cl_event event) {
            this.loadingEvent = event;
        }

        public final void destroy() {
            if (firstLoadingEvent != null) {
                CL.clReleaseEvent(firstLoadingEvent);
            }
            CL.clReleaseEvent(loadingEvent);
            CL.clReleaseMemObject(data);
        }

        @Override
        public String toString() {
            return "Data " + dataId + " (" + data + ") created by " + loadingEvent + " fills " + size + " bytes ";
        }

    }

    private static int baseSize(Class aClass) {
        if (aClass == boolean.class) {
            return Sizeof.cl_char;
        }
        if (aClass == byte.class) {
            return Sizeof.cl_char;
        }
        if (aClass == char.class) {
            return Sizeof.cl_char;
        }
        if (aClass == short.class) {
            return Sizeof.cl_short;
        }
        if (aClass == int.class) {
            return Sizeof.cl_int;
        }
        if (aClass == long.class) {
            return Sizeof.cl_long;
        }
        if (aClass == float.class) {
            return Sizeof.cl_float;
        }
        if (aClass == double.class) {
            return Sizeof.cl_double;
        }
        return Sizeof.POINTER;
    }

    private static int expand(Object seq, Object o, int offset) {
        int read;
        Class<?> oClass = o.getClass();
        if (oClass.isArray()) {
            if (oClass.getComponentType().isArray()) {
                read = 0;
                for (int componentId = 0; componentId < Array.getLength(o); componentId++) {
                    read += expand(seq, Array.get(o, componentId), offset + read);
                }
            } else {
                System.arraycopy(seq, offset, o, 0, Array.getLength(o));
                read = Array.getLength(o);
            }
        } else {
            Array.set(o, offset, seq);
            read = 1;
        }
        return read;
    }

    private static int flatten(Object o, Object seq, int offset) {
        int written;
        Class<?> oClass = o.getClass();
        if (oClass.isArray()) {
            if (oClass.getComponentType().isArray()) {
                written = 0;
                for (int componentId = 0; componentId < Array.getLength(o); componentId++) {
                    written += flatten(Array.get(o, componentId), seq, offset + written);
                }
            } else {
                System.arraycopy(o, 0, seq, offset, Array.getLength(o));
                written = Array.getLength(o);
            }
        } else {
            Array.set(seq, offset, o);
            written = 1;
        }
        return written;
    }

}
