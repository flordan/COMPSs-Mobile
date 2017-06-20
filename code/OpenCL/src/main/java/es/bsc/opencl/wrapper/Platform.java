package es.bsc.opencl.wrapper;

import org.jocl.CL;
import static org.jocl.CL.*;
import org.jocl.Pointer;
import org.jocl.cl_device_id;
import org.jocl.cl_platform_id;

public class Platform {

    private final cl_platform_id platformId;
    private final Device[] devices;

    protected Platform(cl_platform_id platformId) {
        this.platformId = platformId;

        String platformName = getPlatformInfo(CL_PLATFORM_NAME);
        // Obtain the number of devices for the current platform
        int numDevices[] = new int[1];
        CL.setExceptionsEnabled(CL_FALSE);

        int ret;
        int[] devicePerType = new int[4];
        int deviceCount = 0;

        ret = clGetDeviceIDs(platformId, CL.CL_DEVICE_TYPE_CPU, 0, null, numDevices);
        if (ret == CL_SUCCESS) {
            devicePerType[0] = numDevices[0];
            deviceCount += numDevices[0];
        }

        ret = clGetDeviceIDs(platformId, CL.CL_DEVICE_TYPE_GPU, 0, null, numDevices);
        if (ret == CL_SUCCESS) {
            devicePerType[1] = numDevices[0];
            deviceCount += numDevices[0];
        }

        ret = clGetDeviceIDs(platformId, CL.CL_DEVICE_TYPE_ACCELERATOR, 0, null, numDevices);
        if (ret == CL_SUCCESS) {
            devicePerType[2] = numDevices[0];
            deviceCount += numDevices[0];
        }

        ret = clGetDeviceIDs(platformId, CL.CL_DEVICE_TYPE_CUSTOM, 0, null, numDevices);
        if (ret == CL_SUCCESS) {
            devicePerType[3] = numDevices[0];
            deviceCount += numDevices[0];
        }

        devices = new Device[deviceCount];

        CL.setExceptionsEnabled(CL_TRUE);
        int totalDevices = 0;
        if (devicePerType[0] > 0) {
            cl_device_id devicesArray[] = new cl_device_id[devicePerType[0]];
            clGetDeviceIDs(platformId, CL_DEVICE_TYPE_CPU, devicePerType[0], devicesArray, null);
            for (cl_device_id deviceId : devicesArray) {
                Device device = new Device(platformId, deviceId);
                devices[totalDevices] = device;
                totalDevices++;
            }
        }

        if (devicePerType[1] > 0) {
            cl_device_id devicesArray[] = new cl_device_id[devicePerType[1]];
            clGetDeviceIDs(platformId, CL_DEVICE_TYPE_GPU, devicePerType[1], devicesArray, null);
            for (cl_device_id deviceId : devicesArray) {
                Device device = new Device(platformId, deviceId);
                devices[totalDevices] = device;
                totalDevices++;
            }
        }

        if (devicePerType[2] > 0) {
            cl_device_id devicesArray[] = new cl_device_id[devicePerType[2]];
            clGetDeviceIDs(platformId, CL_DEVICE_TYPE_ACCELERATOR, devicePerType[2], devicesArray, null);
            for (cl_device_id deviceId : devicesArray) {
                Device device = new Device(platformId, deviceId);
                devices[totalDevices] = device;
                totalDevices++;
            }
        }

        if (devicePerType[3] > 0) {
            cl_device_id devicesArray[] = new cl_device_id[devicePerType[3]];
            clGetDeviceIDs(platformId, CL_DEVICE_TYPE_CUSTOM, devicePerType[3], devicesArray, null);
            for (cl_device_id deviceId : devicesArray) {
                Device device = new Device(platformId, deviceId);
                devices[totalDevices] = device;
                totalDevices++;
            }
        }
    }

    private String getPlatformInfo(int attribute) {
        // Obtain the length of the string that will be queried
        long size[] = new long[1];
        clGetPlatformInfo(platformId, attribute, 0, null, size);

        if (size[0] == 0) {
            return "";
        }
        // Create a buffer of the appropriate size and fill it with the info
        byte buffer[] = new byte[(int) size[0]];
        clGetPlatformInfo(platformId, attribute, buffer.length, Pointer.to(buffer), null);

        // Create a string from the buffer (excluding the trailing \0 byte)
        return new String(buffer, 0, buffer.length - 1);
    }

    public String getName() {
        return getPlatformInfo(CL_PLATFORM_NAME);
    }

    public String getVersion() {
        return getPlatformInfo(CL_PLATFORM_VERSION);
    }

    public String getVendor() {
        return getPlatformInfo(CL_PLATFORM_VENDOR);
    }

    public String getProfile() {
        return getPlatformInfo(CL_PLATFORM_PROFILE);
    }

    public String[] getExtensions() {
        return getPlatformInfo(CL_PLATFORM_EXTENSIONS).split(" ");
    }

    public Device[] getDevices() {
        return devices;
    }
}
