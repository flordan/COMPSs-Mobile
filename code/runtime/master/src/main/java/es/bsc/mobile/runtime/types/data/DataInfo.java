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

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import es.bsc.mobile.runtime.types.data.RegisteredData.DataLocation;

public class DataInfo {

    private static final int FIRST_DATA_ID = 1;
    private static final int FIRST_VERSION_ID = 1;
    private static final AtomicInteger NEXT_DATA_ID = new AtomicInteger(FIRST_DATA_ID);

    private final int dataId;

    protected TreeMap<Integer, Version> versions;
    protected Version currentVersion;
    private int currentVersionId;

    public DataInfo(RegisteredData value) {
        this.dataId = NEXT_DATA_ID.getAndIncrement();
        this.versions = new TreeMap<>();
        currentVersionId = FIRST_VERSION_ID;
        Version firstVersion = new Version(dataId, currentVersionId, value);
        value.addLocation(DataLocation.LOCAL);
        versions.put(currentVersionId, firstVersion);
        currentVersion = firstVersion;
    }

    public int getDataId() {
        return dataId;
    }

    public Version getVersion(int versionId) {
        return versions.get(versionId);
    }

    public Version getCurrentVersion() {
        return currentVersion;
    }

    public int getCurrentVersionId() {
        return currentVersionId;
    }

    public Version addVersion(RegisteredData value) {
        currentVersionId++;
        Version newVersion = new Version(dataId, currentVersionId, value);
        versions.put(currentVersionId, newVersion);
        currentVersion = newVersion;
        return currentVersion;
    }

    public void removeVersion(int versionId) {
        versions.remove(versionId);
    }

    public String dump(String prefix) {
        StringBuilder sb = new StringBuilder();
        for (java.util.Map.Entry<Integer, Version> entry : versions.entrySet()) {
            sb.append(prefix).append("version:").append(entry.getKey())
                    .append("\n");
            sb.append(entry.getValue().dump(prefix + "\t")).append("\n");
        }
        return sb.toString();
    }

}
