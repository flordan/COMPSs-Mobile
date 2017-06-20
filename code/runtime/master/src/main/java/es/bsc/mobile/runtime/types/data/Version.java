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
package es.bsc.mobile.runtime.types.data;

public class Version {

    private int readers;
    private final DataInstance daId;
    private final RegisteredData value;

    public Version(int dataId, int versionId, RegisteredData value) {
        readers = 0;
        daId = new DataInstance(dataId, versionId);
        this.value = value;
    }

    public int willBeRead() {
        readers++;
        return readers;
    }

    public int hasBeenRead() {
        readers--;
        return readers;
    }

    public int getReaders() {
        return readers;
    }

    public DataInstance getDataInstance() {
        return daId;
    }

    public RegisteredData getValue() {
        return value;
    }

    public Object dump(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix);
        sb.append(value.dump());
        sb.append(" ").append(daId).append(" - ").append(value);
        return sb.toString();
    }
}
