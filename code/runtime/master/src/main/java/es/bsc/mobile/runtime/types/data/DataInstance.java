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

import java.io.Serializable;

public class DataInstance implements Comparable<DataInstance>, Serializable {

    // Time stamp
    protected static String timeStamp = Long.toString(System.currentTimeMillis());

    // File instance identifier fields
    protected int dataId;
    protected int versionId;

    // Renaming for this file version
    protected String renaming;

    public DataInstance() {
    }

    public DataInstance(int dataId, int versionId) {
        this.dataId = dataId;
        this.versionId = versionId;
        this.renaming = "d" + dataId + "v" + versionId + "_" + timeStamp + ".IT";
    }

    public DataInstance(int dataId, int versionId, String renaming) {
        this.dataId = dataId;
        this.versionId = versionId;
        this.renaming = renaming;
    }

    public int getDataId() {
        return dataId;
    }

    public int getVersionId() {
        return versionId;
    }

    public String getRenaming() {
        return renaming;
    }

    @Override
    public int compareTo(DataInstance o) {
        return this.renaming.compareTo(o.getRenaming());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DataInstance) {
            return (((DataInstance) o).renaming).equals(renaming);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((this.renaming == null) ? 0 : this.renaming.hashCode());
    }

    @Override
    public String toString() {
        return "d" + dataId + "v" + versionId;
    }

}
