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
package es.bsc.mobile.runtime.types.resources.gpuplatforms;

import es.bsc.mobile.runtime.types.resources.Resource;
import es.bsc.opencl.wrapper.Device;
import es.bsc.opencl.wrapper.OpenCL;
import es.bsc.opencl.wrapper.Platform;

public class OpenCLResource extends Resource {

    private final Platform platform;
    private final Device device;

    public OpenCLResource(int slots, String platformName, String deviceName) {
        super(slots);

        Device device = null;
        Platform platform = null;
        for (Platform pf : OpenCL.getPlatforms()) {
            if (pf.getName().compareTo(platformName) == 0) {
                platform = pf;
                for (Device d : pf.getDevices()) {
                    if (pf.getName().compareTo(platformName) == 0) {
                        device = d;
                    }
                }
            }
        }
        this.device = device;
        this.platform = platform;
    }

    @Override
    public String getName() {
        return device.getName() + "@" + platform.getName();
    }

    public Device getDevice() {
        return this.device;
    }

}
