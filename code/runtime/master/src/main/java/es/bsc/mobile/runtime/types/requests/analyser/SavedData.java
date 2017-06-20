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
package es.bsc.mobile.runtime.types.requests.analyser;

import es.bsc.mobile.runtime.service.RuntimeHandler;
import es.bsc.mobile.runtime.components.Analyser;

public class SavedData extends AnalyserRequest {

    private String daId;

    public SavedData(String daId) {
        this.daId = daId;
    }

    public String getString() {
        return this.daId;
    }

    public void setDataInstance(String daId) {
        this.daId = daId;
    }

    @Override
    public void dispatch(RuntimeHandler rh, Analyser analyser) {
        analyser.savedData(daId);
    }
}
