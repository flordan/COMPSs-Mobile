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

import org.jocl.CL;
import static org.jocl.CL.*;
import org.jocl.cl_platform_id;

public class OpenCL {

    private static Platform[] platforms;

    static {
        /*try{
        main(new String[0]);
        }catch(Exception e){e.printStackTrace();}*/
        CL.setExceptionsEnabled(CL_TRUE);

        // Obtain the number of platforms
        int numPlatforms[] = new int[1];
        CL.clGetPlatformIDs(0, null, numPlatforms);

        OpenCL.platforms = new Platform[numPlatforms[0]];
        // Obtain the platform IDs
        cl_platform_id platforms[] = new cl_platform_id[numPlatforms[0]];
        CL.clGetPlatformIDs(platforms.length, platforms, null);

        // Collect all devices of all platforms
        for (int i = 0; i < platforms.length; i++) {
            Platform platform = new Platform(platforms[i]);
            OpenCL.platforms[i] = platform;
        }
    }

    public static Platform[] getPlatforms() {
        return platforms;
    }

    public static String dump() {
        StringBuilder dump = new StringBuilder();
        dump.append("Number of platforms: ").append(platforms.length).append("\n");

        for (Platform platform : platforms) {
            dump.append("Platform ").append(platform.getName()).append("\n");
            dump.append("\t Version: ").append(platform.getVersion()).append("\n");
            dump.append("\t Vendor: ").append(platform.getVendor()).append("\n");
            dump.append("\t Profile: ").append(platform.getProfile()).append("\n");
            StringBuilder extensions = new StringBuilder();
            for (String ext : platform.getExtensions()) {
                extensions.append(" ").append(ext);
            }
            dump.append("\t Extensions: ").append(extensions.toString()).append("\n");
            dump.append("\t Devices: \n");

            Device[] devices = platform.getDevices();
            for (Device device : devices) {
                dump.append("\t\t Device ").append(device.getName()).append("\n");
                dump.append("\t\t\t Version: ").append(device.getVersion()).append("\n");
                dump.append("\t\t\t Vendor: ").append(device.getVendor()).append("\n");
                dump.append("\t\t\t Driver version: ").append(device.getDriverVersion()).append("\n");
                dump.append("\t\t\t OpenCL version: ").append(device.getOpenCLVersion()).append("\n");
                dump.append("\t\t\t Device type: ").append(device.getDeviceType()).append("\n");
                dump.append("\t\t\t Max Compute Units: ").append(device.getMaxComputeUnits()).append("\n");
                dump.append("\t\t\t Max Workitem dimensions: ").append(device.getMaxWorkItemDimensions()).append("\n");
                long[] sizes = device.getMaxWorkItemSizes();
                String mwisString = "[" + device.getMaxWorkItemSizes()[0];
                for (int i = 1; i < sizes.length; i++) {
                    mwisString += ", " + sizes[i];
                }
                mwisString += "]";
                dump.append("\t\t\t Max Workitem sizes: ").append(mwisString).append("\n");
                dump.append("\t\t\t Max Workgroup size: ").append(device.getMaxWorkGroupSize()).append("\n");
                dump.append("\t\t\t Max clock frequency: ").append(device.getMaxClockFrequency()).append("\n");
                dump.append("\t\t\t Address bits: ").append(device.getAddressBits()).append("\n");
                dump.append("\t\t\t Max memory allocation size: ").append(device.getMaxMemAllocSize()).append("\n");
                dump.append("\t\t\t Global Memory Size: ").append(device.getGlobalMemSize()).append("\n");
                dump.append("\t\t\t Error correction support: ").append(device.getErrorCorrectionSupport()).append("\n");
                dump.append("\t\t\t Local memory type: ").append(device.getLocalMemType()).append("\n");
                dump.append("\t\t\t Local memory size: ").append(device.getLocalMemSize()).append("\n");
                dump.append("\t\t\t Max constant buffer size: ").append(device.geMaxConstantBufferSize()).append("\n");
                dump.append("\t\t\t Queue Properties: ").append(device.getQueueProperties()).append("\n");
                dump.append("\t\t\t Image support: ").append(device.getImageSupport()).append("\n");
                dump.append("\t\t\t Max read image arguments: ").append(device.getMaxReadImageArgs()).append("\n");
                dump.append("\t\t\t Max write image arguments: ").append(device.getMaxWriteImageArgs()).append("\n");
                dump.append("\t\t\t Single FP config: ").append(device.getSingleFpConfig()).append("\n");
                dump.append("\t\t\t 2D image  max width:").append(device.getImage2dMaxWidth()).append("\n");
                dump.append("\t\t\t 2D image  max height: ").append(device.getImage2dMaxHeight()).append("\n");
                dump.append("\t\t\t 3D image  max width: ").append(device.getImage3dMaxWidth()).append("\n");
                dump.append("\t\t\t 3D image  max height: ").append(device.getImage3dMaxHeight()).append("\n");
                dump.append("\t\t\t 3D image  max depth: ").append(device.getImage3dMaxDepth()).append("\n");
                dump.append("\t\t\t Preferred vector width for chars: ").append(device.getPreferredVectorWidthChar()).append("\n");
                dump.append("\t\t\t Preferred vector width for shorts: ").append(device.getPreferredVectorWidthShort()).append("\n");
                dump.append("\t\t\t Preferred vector width for ints: ").append(device.getPreferredVectorWidthInt()).append("\n");
                dump.append("\t\t\t Preferred vector width for longs: ").append(device.getPreferredVectorWidthLong()).append("\n");
                dump.append("\t\t\t Preferred vector width for floats: ").append(device.getPreferredVectorWidthFloat()).append("\n");
                dump.append("\t\t\t Preferred vector width for doubles: ").append(device.getPreferredVectorWidthDouble()).append("\n");
            }
        }
        return dump.toString();
    }
}
