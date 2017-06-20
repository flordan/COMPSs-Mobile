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
package es.bsc.mobile.types;

import es.bsc.mobile.data.DataManager.DataOperationListener;

public class Operation {

    private final String source;
    private final String rename;
    private final boolean object;
    private final DataOperationListener listener;

    public Operation(boolean isObject, String source, String rename, DataOperationListener listener) {
        this.object = isObject;
        this.source = source;
        if (rename == null) {
            this.rename = source;
        } else {
            this.rename = rename;
        }
        this.listener = listener;
    }

    public Operation(boolean isObject, String source, String rename, int param, Job job) {
        this.object = isObject;
        this.source = source;
        if (rename == null) {
            this.rename = source;
        } else {
            this.rename = rename;
        }
        this.listener = null;
    }

    public boolean isObject() {
        return object;
    }

    public String getSource() {
        return source;
    }

    public String getRename() {
        return rename;
    }

    public DataOperationListener getListener() {
        return listener;
    }

    @Override
    public String toString() {
        return (object ? "OBJECT" : "FILE") + " " + source + "->" + rename + " for " + listener;
    }
}
