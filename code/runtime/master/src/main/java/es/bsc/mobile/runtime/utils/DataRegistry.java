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

import java.io.IOException;
import java.util.HashMap;

import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.runtime.types.data.DataInfo;
import es.bsc.mobile.runtime.types.data.DataInstance;
import es.bsc.mobile.runtime.types.data.RegisteredData;
import es.bsc.mobile.runtime.types.data.Version;
import es.bsc.mobile.runtime.types.data.access.DataAccess;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.access.WriteAccess;

/**
 *
 * @author flordan
 *
 * @param <K> data Id type
 */
public class DataRegistry<K> {

    //Version structure
    private final HashMap<K, DataInfo> idToData;
    //Data values
    private final HashMap<String, RegisteredData> renameToValue;

    public DataRegistry() {
        idToData = new HashMap<>();
        renameToValue = new HashMap<>();
    }

    public DataInfo registerData(K dataId, RegisteredData dataValue) {
        DataInfo dataInfo = idToData.get(dataId);
        if (dataInfo == null) {
            dataInfo = new DataInfo(dataValue);
            idToData.put(dataId, dataInfo);
            String renaming = dataInfo.getCurrentVersion().getDataInstance().getRenaming();
            renameToValue.put(renaming, dataValue);
        }
        return dataInfo;
    }

    public DataInfo findData(K dataId) {
        return idToData.get(dataId);
    }

    public DataAccess registerRemoteDataAccess(Direction direction, DataInfo dataInfo) {
        Version currentVersion = dataInfo.getCurrentVersion();
        DataInstance writtenInstance;
        RegisteredData out;
        DataAccess da;
        switch (direction) {
            case IN:
                currentVersion.willBeRead();
                da = new ReadAccess(currentVersion.getDataInstance());
                break;
            case INOUT:
                currentVersion.willBeRead();
                DataInstance readInstance = currentVersion.getDataInstance();
                out = currentVersion.getValue().genInOutRemote();
                currentVersion = dataInfo.addVersion(out);
                writtenInstance = currentVersion.getDataInstance();
                renameToValue.put(writtenInstance.getRenaming(), out);
                da = new ReadWriteAccess(readInstance, writtenInstance);
                break;
            default: // OUT
                out = currentVersion.getValue().genInOutRemote();
                currentVersion = dataInfo.addVersion(out);
                writtenInstance = currentVersion.getDataInstance();
                renameToValue.put(writtenInstance.getRenaming(), out);
                da = new WriteAccess(writtenInstance);
        }
        return da;
    }

    public DataAccess registerLocalDataAccess(Direction direction, DataInfo dataInfo) throws IOException {
        DataAccess da;
        Version readVersion = dataInfo.getCurrentVersion();
        int readVersionId = dataInfo.getCurrentVersionId();
        DataInstance readInstance = readVersion.getDataInstance();
        switch (direction) {
            case IN:
                return new ReadAccess(readInstance);
            case INOUT:
                RegisteredData inValue = readVersion.getValue();
                if (readVersion.getReaders() == 0 && !inValue.isSavedForTransfer() && inValue.isLocal()) {
                    //The version is used only in the master
                    //We reuse the version
                    return new ReadWriteAccess(readInstance, readInstance);
                } else {
                    //Value has been used or will be in some task
                    //We create a new Version
                    if (readVersion.getReaders() > 0 && !inValue.isSavedForTransfer()) {
                        //If in value to be used by tasks and is not serilized
                        //serialize the object value
                        inValue.saveForTransfer();
                    }
                    //We create a new version with a the oldValue that will be rewritten
                    RegisteredData outValue = inValue.genInOutLocal();
                    Version writeVersion = dataInfo.addVersion(outValue);
                    DataInstance writtenInstance = writeVersion.getDataInstance();
                    renameToValue.put(writtenInstance.getRenaming(), outValue);
                    da = new ReadWriteAccess(readInstance, writtenInstance);
                    if (readVersion.getReaders() == 0) {
                        //old version can be removed
                        dataInfo.removeVersion(readVersionId);
                    }
                }
                break;
            default:
                da = null;
        }
        return da;
    }

    public RegisteredData getCurrentVersionValue(DataInfo dataInfo) {
        return dataInfo.getCurrentVersion().getValue();
    }

    public DataInfo deleteData(K dataId) {
        DataInfo dataInfo = idToData.remove(dataId);
        for (int i = 0; i < dataInfo.getCurrentVersionId(); i++) {
            Version v = dataInfo.getVersion(i);
            if (v != null) {
                String renaming = v.getDataInstance().getRenaming();
                renameToValue.remove(renaming);
            }
        }
        return dataInfo;
    }

    public String dump() {
        StringBuilder sb = new StringBuilder("Data stored in the DataRegistry:\n");
        for (java.util.Map.Entry<K, DataInfo> entry : idToData.entrySet()) {
            sb.append("ID:").append(entry.getKey()).append("(").append(entry.getValue().getDataId()).append(")" + "\n");
            sb.append(entry.getValue().dump("\t"));
        }
        return sb.toString();
    }

    public boolean checkExistence(K dataId) {
        return idToData.get(dataId) != null;
    }

    public RegisteredData getRegisteredData(String rename) {
        return renameToValue.get(rename);
    }
}
