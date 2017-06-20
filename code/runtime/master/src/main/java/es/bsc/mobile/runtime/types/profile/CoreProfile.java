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

public class CoreProfile {

    private final int coreId;
    private final int numParams;
    private final MinMax[] inParamsSize;
    private final MinMax[] outParamsSize;
    private final MinMax inTargetSize;
    private final MinMax outTargetSize;
    private final MinMax resultSize;

    public CoreProfile(int coreId, int numParams) {
        this.coreId = coreId;
        this.numParams = numParams;
        inTargetSize = new MinMax();
        outTargetSize = new MinMax();
        resultSize = new MinMax();
        inParamsSize = new MinMax[numParams];
        outParamsSize = new MinMax[numParams];
        for (int i = 0; i < numParams; i++) {
            inParamsSize[i] = new MinMax();
            outParamsSize[i] = new MinMax();
        }
    }

    public int getCoreId() {
        return coreId;
    }

    public int getNumParams() {
        return numParams;
    }

    public MinMax getParamInSize(int paramId) {
        return inParamsSize[paramId];
    }

    public MinMax getParamOutSize(int paramId) {
        return outParamsSize[paramId];
    }

    public MinMax getTargetInSize() {
        return inTargetSize;
    }

    public MinMax getTargetOutSize() {
        return outTargetSize;
    }

    public MinMax getResultSize() {
        return resultSize;
    }

    public void registerProfiledJob(JobProfile jp) {
        for (int i = 0; i < numParams; i++) {
            inParamsSize[i].newValue(jp.getParamsSize(true, i));
            outParamsSize[i].newValue(jp.getParamsSize(false, i));
        }
        inTargetSize.newValue(jp.getTargetSize(true));
        outTargetSize.newValue(jp.getTargetSize(false));
        resultSize.newValue(jp.getResultSize());
    }

    public String dump(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("\t IN Sizes\n");
        for (int i = 0; i < numParams; i++) {
            sb.append(prefix).append("\t\t Param ").append(i).append("\t").append(inParamsSize[i]).append("\n");
        }
        sb.append(prefix).append("\t\tTarget \t").append(inTargetSize).append("\n");
        sb.append(prefix).append("\t OUT Sizes\n");
        for (int i = 0; i < numParams; i++) {
            sb.append(prefix).append("\t\t Param ").append(i).append("\t").append(outParamsSize[i]).append("\n");
        }
        sb.append(prefix).append("\t\tTarget \t").append(outTargetSize).append("\n");
        sb.append(prefix).append("\t\tResult \t").append(resultSize).append("\n");
        return sb.toString();
    }
}
