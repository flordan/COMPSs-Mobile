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
package es.bsc.mobile.runtime.utils;

import java.util.HashMap;
import java.util.LinkedList;

import es.bsc.mobile.runtime.types.CEI;
import es.bsc.mobile.runtime.types.Implementation;
import es.bsc.mobile.runtime.types.profile.CoreProfile;
import java.util.Collection;

public class CoreManager {

    private static int coreCount = 0;
    private static Implementation[][] implementations;
    private static final HashMap<String, Integer> SIGNATURE_TO_ID = new HashMap<>();
    private static String[] idToSignature = new String[0];
    private static CoreProfile[] profiles;

    public static boolean isInitialized() {
        return !SIGNATURE_TO_ID.isEmpty();
    }

    private CoreManager() {
    }

    public static void registerSignature(String signature, int coreId) {
        SIGNATURE_TO_ID.put(signature, coreId);
    }

    public static int registerCEI(CEI cei) {
        int ceiCoreCount = cei.getCoreCount();
        int oldCoreCount = 0;
        if (implementations != null) {
            Implementation[][] oldImplementations = implementations;
            oldCoreCount = oldImplementations.length;
            implementations = new Implementation[ceiCoreCount + oldCoreCount][];
            System.arraycopy(oldImplementations, 0, implementations, 0, oldCoreCount);

            CoreProfile[] newParamsCount = new CoreProfile[ceiCoreCount + oldCoreCount];
            System.arraycopy(profiles, 0, newParamsCount, 0, coreCount);
            profiles = newParamsCount;

            String[] newId2Sig = new String[ceiCoreCount + oldCoreCount];
            System.arraycopy(idToSignature, 0, newId2Sig, 0, coreCount);
            idToSignature = newId2Sig;
        } else {
            implementations = new Implementation[ceiCoreCount][];
            profiles = new CoreProfile[ceiCoreCount];
            idToSignature = new String[ceiCoreCount];
        }

        for (int ceiCE = 0; ceiCE < ceiCoreCount; ceiCE++) {
            String ceiSignature = cei.getCoreSignature(ceiCE);
            LinkedList<Implementation> ceiImpls = cei.getCoreImplementations(ceiCE);
            Implementation[] impls = new Implementation[ceiImpls.size()];
            implementations[ceiCE + oldCoreCount] = impls;
            idToSignature[ceiCE + oldCoreCount] = ceiSignature;
            SIGNATURE_TO_ID.put(ceiSignature, coreCount);
            for (Implementation impl : ceiImpls) {
                impl.setCoreElementId(coreCount);
                String signature = impl.completeSignature(ceiSignature);
                SIGNATURE_TO_ID.put(signature, coreCount);
                impls[impl.getImplementationId()] = impl;
            }
            profiles[coreCount] = new CoreProfile(coreCount, cei.getParamsCount(ceiCE));
            coreCount++;
        }
        return coreCount;
    }

    public static int getCoreCount() {
        return coreCount;
    }

    public static Integer getCoreId(String signature) {
        return SIGNATURE_TO_ID.get(signature);
    }

    public static Collection<String> getAllSignatures() {
        return SIGNATURE_TO_ID.keySet();
    }

    public static String getSignature(int coreId) {
        return idToSignature[coreId];
    }

    public static Implementation[] getCoreImplementations(int coreId) {
        return implementations[coreId];
    }

    public static CoreProfile getCoreProfile(int coreId) {
        return profiles[coreId];
    }
}
