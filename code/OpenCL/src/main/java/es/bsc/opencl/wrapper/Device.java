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

import es.bsc.opencl.wrapper.DeviceMemory.MemoryRegister;
import es.bsc.opencl.wrapper.policies.LastNotPendingAccess;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.LinkedList;
import org.jocl.CL;

import static org.jocl.CL.CL_COMPLETE;
import static org.jocl.CL.CL_DEVICE_ADDRESS_BITS;
import static org.jocl.CL.CL_DEVICE_ERROR_CORRECTION_SUPPORT;
import static org.jocl.CL.CL_DEVICE_GLOBAL_MEM_SIZE;
import static org.jocl.CL.CL_DEVICE_IMAGE2D_MAX_HEIGHT;
import static org.jocl.CL.CL_DEVICE_IMAGE2D_MAX_WIDTH;
import static org.jocl.CL.CL_DEVICE_IMAGE3D_MAX_DEPTH;
import static org.jocl.CL.CL_DEVICE_IMAGE3D_MAX_HEIGHT;
import static org.jocl.CL.CL_DEVICE_IMAGE3D_MAX_WIDTH;
import static org.jocl.CL.CL_DEVICE_IMAGE_SUPPORT;
import static org.jocl.CL.CL_DEVICE_LOCAL_MEM_SIZE;
import static org.jocl.CL.CL_DEVICE_LOCAL_MEM_TYPE;
import static org.jocl.CL.CL_DEVICE_MAX_CLOCK_FREQUENCY;
import static org.jocl.CL.CL_DEVICE_MAX_COMPUTE_UNITS;
import static org.jocl.CL.CL_DEVICE_MAX_CONSTANT_BUFFER_SIZE;
import static org.jocl.CL.CL_DEVICE_MAX_MEM_ALLOC_SIZE;
import static org.jocl.CL.CL_DEVICE_MAX_READ_IMAGE_ARGS;
import static org.jocl.CL.CL_DEVICE_MAX_WORK_GROUP_SIZE;
import static org.jocl.CL.CL_DEVICE_MAX_WORK_ITEM_DIMENSIONS;
import static org.jocl.CL.CL_DEVICE_MAX_WORK_ITEM_SIZES;
import static org.jocl.CL.CL_DEVICE_MAX_WRITE_IMAGE_ARGS;
import static org.jocl.CL.CL_DEVICE_NAME;
import static org.jocl.CL.CL_DEVICE_OPENCL_C_VERSION;
import static org.jocl.CL.CL_DEVICE_PREFERRED_VECTOR_WIDTH_CHAR;
import static org.jocl.CL.CL_DEVICE_PREFERRED_VECTOR_WIDTH_DOUBLE;
import static org.jocl.CL.CL_DEVICE_PREFERRED_VECTOR_WIDTH_FLOAT;
import static org.jocl.CL.CL_DEVICE_PREFERRED_VECTOR_WIDTH_INT;
import static org.jocl.CL.CL_DEVICE_PREFERRED_VECTOR_WIDTH_LONG;
import static org.jocl.CL.CL_DEVICE_PREFERRED_VECTOR_WIDTH_SHORT;
import static org.jocl.CL.CL_DEVICE_QUEUE_PROPERTIES;
import static org.jocl.CL.CL_DEVICE_SINGLE_FP_CONFIG;
import static org.jocl.CL.CL_DEVICE_TYPE;
import static org.jocl.CL.CL_DEVICE_VENDOR;
import static org.jocl.CL.CL_DEVICE_VERSION;
import static org.jocl.CL.CL_DRIVER_VERSION;

import org.jocl.EventCallbackFunction;
import org.jocl.Pointer;
import org.jocl.Sizeof;
import org.jocl.cl_command_queue;
import org.jocl.cl_context;
import org.jocl.cl_device_id;
import org.jocl.cl_event;
import org.jocl.cl_kernel;
import org.jocl.cl_mem;
import org.jocl.cl_platform_id;
import org.jocl.cl_program;

public class Device {

    private final cl_device_id deviceId;
    private final cl_context context;
    private final cl_command_queue queue;
    private final HashMap<String, cl_program> programs;
    private final HashMap<String, long[]> kernelSize;
    private final DeviceMemory memory;

    public Device(cl_platform_id platform, cl_device_id device) {
        this.deviceId = device;
        int[] errcode_ret = new int[1];
        this.context = CL.clCreateContext(null, 1, new cl_device_id[]{deviceId}, null, null, errcode_ret);
        this.queue = CL.clCreateCommandQueue(context, device, CL.CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE | CL.CL_QUEUE_PROFILING_ENABLE, errcode_ret);
        this.programs = new HashMap<>();
        this.kernelSize = new HashMap<>();
        this.memory = new LastNotPendingAccess(this.getGlobalMemSize(), context, queue);
    }

    public final String getName() {
        return getStringInfo(CL_DEVICE_NAME);
    }

    public final String getVersion() {
        return getStringInfo(CL_DEVICE_VERSION);
    }

    public final String getOpenCLVersion() {
        return getStringInfo(CL_DEVICE_OPENCL_C_VERSION);
    }

    public final String getVendor() {
        return getStringInfo(CL_DEVICE_VENDOR);
    }

    public final String getDriverVersion() {
        return getStringInfo(CL_DRIVER_VERSION);
    }

    public final long getDeviceType() {
        return getLongInfo(CL_DEVICE_TYPE);
    }

    public final int getMaxComputeUnits() {
        return getIntInfo(CL_DEVICE_MAX_COMPUTE_UNITS);
    }

    public final long getMaxWorkItemDimensions() {
        return getLongInfo(CL_DEVICE_MAX_WORK_ITEM_DIMENSIONS);
    }

    public final long[] getMaxWorkItemSizes() {
        return getSizesInfo(CL_DEVICE_MAX_WORK_ITEM_SIZES, 3);
    }

    public final long getMaxWorkGroupSize() {
        return getSizeInfo(CL_DEVICE_MAX_WORK_GROUP_SIZE);
    }

    public final long getMaxClockFrequency() {
        return getLongInfo(CL_DEVICE_MAX_CLOCK_FREQUENCY);
    }

    public final int getAddressBits() {
        return getIntInfo(CL_DEVICE_ADDRESS_BITS);
    }

    public final long getMaxMemAllocSize() {
        return getLongInfo(CL_DEVICE_MAX_MEM_ALLOC_SIZE);
    }

    public final long getGlobalMemSize() {
        return getLongInfo(CL_DEVICE_GLOBAL_MEM_SIZE);
    }

    public final int getErrorCorrectionSupport() {
        return getIntInfo(CL_DEVICE_ERROR_CORRECTION_SUPPORT);
    }

    public final int getLocalMemType() {
        return getIntInfo(CL_DEVICE_LOCAL_MEM_TYPE);
    }

    public final long getLocalMemSize() {
        return getLongInfo(CL_DEVICE_LOCAL_MEM_SIZE);
    }

    public final long geMaxConstantBufferSize() {
        return getLongInfo(CL_DEVICE_MAX_CONSTANT_BUFFER_SIZE);
    }

    public final long getQueueProperties() {
        return getLongInfo(CL_DEVICE_QUEUE_PROPERTIES);
    }

    public final int getImageSupport() {
        return getIntInfo(CL_DEVICE_IMAGE_SUPPORT);
    }

    public final int getMaxReadImageArgs() {
        return getIntInfo(CL_DEVICE_MAX_READ_IMAGE_ARGS);
    }

    public final int getMaxWriteImageArgs() {
        return getIntInfo(CL_DEVICE_MAX_WRITE_IMAGE_ARGS);
    }

    public final long getSingleFpConfig() {
        return getLongInfo(CL_DEVICE_SINGLE_FP_CONFIG);
    }

    public final long getImage2dMaxWidth() {
        return getSizeInfo(CL_DEVICE_IMAGE2D_MAX_WIDTH);
    }

    public final long getImage2dMaxHeight() {
        return getSizeInfo(CL_DEVICE_IMAGE2D_MAX_HEIGHT);
    }

    public final long getImage3dMaxWidth() {
        return getSizeInfo(CL_DEVICE_IMAGE3D_MAX_WIDTH);
    }

    public final long getImage3dMaxHeight() {
        return getSizeInfo(CL_DEVICE_IMAGE3D_MAX_HEIGHT);
    }

    public final long getImage3dMaxDepth() {
        return getSizeInfo(CL_DEVICE_IMAGE3D_MAX_DEPTH);
    }

    public final int getPreferredVectorWidthChar() {
        return getIntInfo(CL_DEVICE_PREFERRED_VECTOR_WIDTH_CHAR);
    }

    public final int getPreferredVectorWidthShort() {
        return getIntInfo(CL_DEVICE_PREFERRED_VECTOR_WIDTH_SHORT);
    }

    public final int getPreferredVectorWidthInt() {
        return getIntInfo(CL_DEVICE_PREFERRED_VECTOR_WIDTH_INT);
    }

    public final int getPreferredVectorWidthLong() {
        return getIntInfo(CL_DEVICE_PREFERRED_VECTOR_WIDTH_LONG);
    }

    public final int getPreferredVectorWidthFloat() {
        return getIntInfo(CL_DEVICE_PREFERRED_VECTOR_WIDTH_FLOAT);
    }

    public final int getPreferredVectorWidthDouble() {
        return getIntInfo(CL_DEVICE_PREFERRED_VECTOR_WIDTH_DOUBLE);
    }

    public final cl_program addProgramFromSource(String programName, InputStream input) throws IOException {
        cl_program program = programs.get(programName);
        if (program == null) {
            program = createProgramFromSource(input);
            programs.put(programName, program);
        }
        return program;
    }

    public final cl_program addProgramFromSource(String programName, String sourceCode) {
        cl_program program = programs.get(programName);
        if (program == null) {
            program = createProgramFromSource(sourceCode);
            programs.put(programName, program);
        }
        return program;
    }

    private cl_program createProgramFromSource(InputStream input) throws IOException {
        int size = input.available();
        byte[] content = new byte[size];
        input.read(content);
        String code = new String(content);
        return createProgramFromSource(code);
    }

    private cl_program createProgramFromSource(String sourceCode) {
        int[] errCode = new int[1];
        cl_program program = CL.clCreateProgramWithSource(context, 1, new String[]{sourceCode}, new long[]{sourceCode.length()}, errCode);
        CL.clBuildProgram(program, 1, new cl_device_id[]{deviceId}, null, null, errCode);
        return program;
    }

    public final cl_program addProgramFromBinary(String programName) throws IOException {
        cl_program program = programs.get(programName);
        if (program == null) {
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(programName);
            program = addProgramFromBinary(programName, in);
        }
        return program;
    }

    public final cl_program addProgramFromBinary(String programName, InputStream binary) throws IOException {
        cl_program program = programs.get(programName);
        if (program == null) {
            int size = binary.available();
            byte[] content = new byte[size];
            binary.read(content);
            String code = new String(content);
            program = addProgramFromSource(programName, code);
        }
        return program;
    }

    public final cl_program addProgramFromBinary(String programName, byte[] binary) {
        cl_program program = programs.get(programName);
        if (program == null) {

        }
        return program;
    }

    public final void prepareValueForKernel(OpenCLMemoryPrepare listener, String valueRenaming, Object object, Class<?> objectClass, int size, boolean copyInput, boolean canWrite) {
        memory.setOnMemory(listener, valueRenaming, object, objectClass, size, copyInput, canWrite);
    }

    public final Object runKernel(String programName, String methodName, boolean[] writes, Object[] params, long[] globalWorkSize, long[] localWorkSize, long[] offset, OpenCLExecutionListener executionListenner) {
        cl_program p = programs.get(programName);
        int[] errout = new int[1];
        cl_kernel kernel = CL.clCreateKernel(p, methodName, errout);
        if (localWorkSize.length == 0) {
            localWorkSize = kernelSize.get(programName + "." + methodName);
            if (localWorkSize == null) {
                localWorkSize = new long[globalWorkSize.length];

                long[] size = new long[1];
                CL.clGetKernelWorkGroupInfo(kernel, deviceId, CL.CL_KERNEL_WORK_GROUP_SIZE, Sizeof.size_t, Pointer.to(size), null);

                long variation = 1;
                long dimValue = 1;
                for (int dim = 0; dim < globalWorkSize.length; dim++) {
                    variation *= 2;
                }
                long total = 1;

                while (size[0] >= (variation * total)) {
                    total *= variation;
                    dimValue *= 2;
                }

                for (int dim = 0; dim < globalWorkSize.length; dim++) {
                    localWorkSize[dim] = dimValue;
                }
                kernelSize.put(programName + "." + methodName, localWorkSize);
            }
        }
        LinkedList<Object> prepareEvents = new LinkedList();
        for (int paramId = 0; paramId < params.length; paramId++) {
            if (params[paramId] instanceof MemoryRegister) {
                MemoryRegister mr = (MemoryRegister) params[paramId];
                cl_mem buffer = mr.getBuffer();
                cl_event event = mr.getLoadingEvent();
                if (event != null) {
                    prepareEvents.add(event);
                }
                CL.clSetKernelArg(kernel, paramId, Sizeof.cl_mem, Pointer.to(buffer));
            } else {
                addKernelBasicParam(kernel, paramId, params[paramId]);
            }
        }
        cl_event event = new cl_event();
        CL.clEnqueueNDRangeKernel(this.queue, kernel, globalWorkSize.length, offset, globalWorkSize, localWorkSize, prepareEvents.size(), prepareEvents.toArray(new cl_event[0]), event);

        for (int paramId = 0; paramId < params.length; paramId++) {
            if (writes[paramId]) {
                MemoryRegister mr = (MemoryRegister) params[paramId];
                mr.replaceLoadingEvent(event);
            }
        }
        if (executionListenner != null) {
            CL.clSetEventCallback(event, CL_COMPLETE, executionListenner, kernel);
        }
        CL.clFlush(queue);
        return event;
    }

    public final void collectValueFromKernel(Object source, Object target, boolean wasWritten) {
        memory.retrieveValue(source, target, wasWritten);
    }

    private void addKernelBasicParam(cl_kernel kernel, int paramId, Object param) {
        if (param instanceof java.lang.Byte) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_char, Pointer.to(new byte[]{((Byte) param)}));
        } else if (param instanceof java.lang.Character) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_short, Pointer.to(new char[]{((Character) param)}));
        } else if (param instanceof java.lang.Short) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_short, Pointer.to(new short[]{((Short) param)}));
        } else if (param instanceof java.lang.Integer) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_int, Pointer.to(new int[]{((Integer) param)}));
        } else if (param instanceof java.lang.Long) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_long, Pointer.to(new long[]{((Long) param)}));
        } else if (param instanceof java.lang.Float) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_float, Pointer.to(new float[]{((Float) param)}));
        } else if (param instanceof java.lang.Double) {
            CL.clSetKernelArg(kernel, paramId, Sizeof.cl_double, Pointer.to(new double[]{((Double) param)}));
        } else {

        }
    }

    /**
     * Returns the value of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @return The value
     */
    private int getIntInfo(int paramName) {
        return getIntsInfo(paramName, 1)[0];
    }

    /**
     * Returns the values of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @param numValues The number of values
     * @return The value
     */
    private int[] getIntsInfo(int paramName, int numValues) {
        int values[] = new int[numValues];
        CL.clGetDeviceInfo(deviceId, paramName, Sizeof.cl_int * numValues, Pointer.to(values), null);
        return values;
    }

    /**
     * Returns the value of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @return The value
     */
    private long getLongInfo(int paramName) {
        return getLongsInfo(paramName, 1)[0];
    }

    /**
     * Returns the values of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @param numValues The number of values
     * @return The value
     */
    private long[] getLongsInfo(int paramName, int numValues) {
        long values[] = new long[numValues];
        CL.clGetDeviceInfo(deviceId, paramName, Sizeof.cl_long * numValues, Pointer.to(values), null);
        return values;
    }

    /**
     * Returns the value of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @return The value
     */
    private String getStringInfo(int paramName) {
        // Obtain the length of the string that will be queried
        long size[] = new long[1];
        CL.clGetDeviceInfo(deviceId, paramName, 0, null, size);

        if (size[0] == 0) {
            return "";
        }
        // Create a buffer of the appropriate size and fill it with the info
        byte buffer[] = new byte[(int) size[0]];
        CL.clGetDeviceInfo(deviceId, paramName, buffer.length, Pointer.to(buffer), null);

        // Create a string from the buffer (excluding the trailing \0 byte)
        return new String(buffer, 0, buffer.length - 1);
    }

    /**
     * Returns the value of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @return The value
     */
    private long getSizeInfo(int paramName) {
        return getSizesInfo(paramName, 1)[0];
    }

    /**
     * Returns the values of the device info parameter with the given name
     *
     * @param paramName The parameter name
     * @param numValues The number of values
     * @return The value
     */
    private long[] getSizesInfo(int paramName, int numValues) {
        // The size of the returned data has to depend on 
        // the size of a size_t, which is handled here
        ByteBuffer buffer = ByteBuffer.allocate(numValues * Sizeof.size_t).order(ByteOrder.nativeOrder());
        CL.clGetDeviceInfo(deviceId, paramName, Sizeof.size_t * numValues, Pointer.to(buffer), null);
        long values[] = new long[numValues];
        if (Sizeof.size_t == 4) {
            for (int i = 0; i < numValues; i++) {
                values[i] = buffer.getInt(i * Sizeof.size_t);
            }
        } else {
            for (int i = 0; i < numValues; i++) {
                values[i] = buffer.getLong(i * Sizeof.size_t);
            }
        }
        return values;
    }

    /**
     * Returns the profiling timestampt for a given event
     *
     * @param event event to query
     * @param attr execution status id
     * @return Timestamp of the profile state
     */
    private static long getProfileInfo(cl_event event, int attr) {
        long values[] = new long[1];
        try {
            CL.clGetEventProfilingInfo(event, attr, Sizeof.cl_long, Pointer.to(values), null);
        } catch (Exception e) {
        }
        return values[0];
    }

    public static abstract class OpenCLMemoryPrepare {

        public abstract void completed(Object preparedValue);

        public abstract void failed();

    }

    public static abstract class OpenCLExecutionListener implements EventCallbackFunction {

        @Override
        public void function(cl_event event, int command_exec_callback_type, Object user_data) {
            try {
                cl_kernel kernel = (cl_kernel) user_data;
                switch (command_exec_callback_type) {
                    case CL_COMPLETE:
                        long start = getProfileInfo(event, CL.CL_PROFILING_COMMAND_START);
                        long end = getProfileInfo(event, CL.CL_PROFILING_COMMAND_END);
                        completed(start, end);
                        CL.clReleaseKernel(kernel);
                        break;
                    default:
                        failed();
                        break;
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        public abstract void completed(long start, long end);

        public abstract void failed();

    }
}
