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
package es.bsc.mobile.runtime.types.profile;

import es.bsc.mobile.types.JobProfile;
import es.bsc.mobile.types.calc.MinMax;

public class ImplementationProfile {

    private final int implId;
    private final MinMax executionTime;
    private final MinMax executionEnergy;

    public ImplementationProfile(int implId) {
        this.implId = implId;
        executionTime = new MinMax();
        executionEnergy = new MinMax();
    }

    public ImplementationProfile(int implId, String values) {
        this.implId = implId;
        String executionTimeString = values.substring(1, values.length() - 1);
        String[] valsArray = executionTimeString.split("]]");
        executionTime = new MinMax(valsArray[0].substring(1) + "]");
        executionEnergy = new MinMax(valsArray[1].substring(1) + "]");
    }

    public void registerProfiledJob(JobProfile jp) {
        executionTime.newValue(jp.getExecutionTime());
        executionEnergy.newValue(jp.getExecutionEnergy());
    }

    public MinMax getExecutionTime() {
        return executionTime;
    }

    public MinMax getEnergyConsumption() {
        return executionEnergy;
    }

    public String toStore() {
        return "[[" + executionTime.toStore() + "][" + executionEnergy.toStore() + "]]";
    }

}
