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
package es.bsc.mobile.runtime.types;

import es.bsc.mobile.annotations.JavaMethod;
import es.bsc.mobile.annotations.OpenCL;
import java.util.LinkedList;

import es.bsc.mobile.types.Constraints;
import java.io.Serializable;
import java.util.Arrays;

public class CEI implements Serializable {

    private final String[] ceSignatures;
    private final String[][] implSignatures;
    private final LinkedList<Implementation>[] coreElements;
    private final int[] coreParamsCount;

    public CEI(Class<?> itf) {
        int coreCount = itf.getDeclaredMethods().length;
        ceSignatures = new String[coreCount];
        coreElements = new LinkedList[coreCount];
        coreParamsCount = new int[coreCount];
        implSignatures = new String[coreCount][];
        int ceId = 0;
        for (java.lang.reflect.Method m : itf.getDeclaredMethods()) {
            if (m.isAnnotationPresent(es.bsc.mobile.annotations.CoreElement.class)) {
                coreElements[ceId] = loadCoreElement(m, ceId);
                coreParamsCount[ceId] = m.getParameterAnnotations().length;
            }
            ceId++;
        }
    }

    private LinkedList<Implementation> loadCoreElement(java.lang.reflect.Method m, int ceId) {
        LinkedList<Implementation> impls = new LinkedList<>();

        String methodName = m.getName();
        String methodSignature = Method.getSignature(m);
        ceSignatures[ceId] = methodSignature;
        es.bsc.mobile.annotations.CoreElement ceAnnot = m.getAnnotation(es.bsc.mobile.annotations.CoreElement.class);

        int implementationCount = ceAnnot.methods().length + ceAnnot.openclKernels().length;
        implSignatures[ceId] = new String[implementationCount];

        int implId = 0;
        for (JavaMethod mAnnot : ceAnnot.methods()) {
            String declaringClass = mAnnot.declaringClass();
            Constraints ctrs = new Constraints(mAnnot.constraints());
            if (ctrs.processorCoreCount() == 0) {
                ctrs.setProcessorCoreCount(1);
            }
            Method method = new Method(ceId, implId, methodName, declaringClass, ctrs);
            implSignatures[ceId][implId] = method.completeSignature(methodSignature);
            impls.add(method);
            implId++;
        }

        for (OpenCL oclAnnot : ceAnnot.openclKernels()) {
            String program = oclAnnot.kernel();
            Class<?> resultType = m.getReturnType();
            String[] resultSize = oclAnnot.resultSize();
            String[] workload = oclAnnot.workloadSize();
            String[] localSize = oclAnnot.localSize();
            String[] readOffset = oclAnnot.offset();
            String[] offset;
            if (readOffset.length == workload.length) {
                offset = readOffset;
            } else {
                offset = new String[workload.length];
                int i = 0;
                for (; i < offset.length && i < readOffset.length; i++) {
                    offset[i] = readOffset[i];
                }
                for (; i < offset.length; i++) {
                    offset[i] = "0";
                }
            }
            Kernel kernel = new Kernel(ceId, implId, program, methodName, resultType, resultSize, workload, localSize, offset);
            implSignatures[ceId][implId] = kernel.completeSignature(methodSignature);
            impls.add(kernel);
            implId++;
        }

        return impls;
    }

    public int getCoreCount() {
        return coreElements.length;
    }

    public String getCoreSignature(int coreIdx) {
        return ceSignatures[coreIdx];

    }

    public LinkedList<Implementation> getCoreImplementations(int coreIdx) {
        return coreElements[coreIdx];
    }

    public int getParamsCount(int ceiCE) {
        return coreParamsCount[ceiCE];
    }

    public String[] getAllSignatures() {
        LinkedList<String> signatures = new LinkedList<>();
        signatures.addAll(Arrays.asList(ceSignatures));
        for (String[] impls : implSignatures) {
            for (String sign : impls) {
                if (sign != null) {
                    signatures.addAll(Arrays.asList(impls));
                }
            }
        }
        String[] signArray = new String[signatures.size()];
        int i = 0;
        for (String signature : signatures) {
            signArray[i++] = signature;
        }
        return signArray;
    }
}
