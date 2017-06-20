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
package es.bsc.mobile.runtime.service;

import es.bsc.comm.nio.NIONode;
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.runtime.types.CEI;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.types.comm.Node;
import es.bsc.mobile.types.messages.runtime.DataExistenceRequest;
import es.bsc.mobile.types.messages.runtime.DataSourceRequest;
import es.bsc.mobile.utils.LoggerConfiguration;
import org.apache.logging.log4j.Level;

public class RuntimeServiceImpl extends RuntimeServiceItf.Stub {

    private RuntimeHandler rh = null;

    @Override
    public void start(CEI cei) {
        CoreManager.registerCEI(cei);
        if (rh == null) {
            rh = new RuntimeHandler(new NIONode(null, 43000));
            CommunicationManager.start(rh);
        }
        rh.resizeCoreStructures();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //Do nothing
        }
    }

    @Override
    public void notifyDataCreation(String dataId, Node hostNode) {
        rh.appDataCreation(dataId, hostNode);
    }

    @Override
    public void requestDataExistence(String dataId, Node querier) {
        rh.handleExistenceRequest(new DataExistenceRequest(dataId, querier));
    }

    @Override
    public void requestDataLocations(String dataId, Node querier) {
        rh.handleSourceRequest(new DataSourceRequest(dataId, querier));
    }

    @Override
    public void executeTask(Task t) {
        rh.executeTask(t);
    }

    @Override
    public int[] getCoreIds(String[] signatures) {
        int coreCount = signatures.length;
        int[] ids = new int[coreCount];
        for (int i = 0; i < coreCount; i++) {
            ids[i] = CoreManager.getCoreId(signatures[i]);
        }
        return ids;
    }

    public static void main(String[] args) throws Exception {

        LoggerConfiguration.configRootLogger(Level.OFF);

        //add appender to any Logger (here is root)
        /*
        LoggerConfiguration.configLogger("Communication", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Communications", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.DataSharing", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Checkpointing", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Offload", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Resources", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Jobs", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Serializer", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.P2P", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Service", Level.DEBUG);
        LoggerConfiguration.configLogger("Runtime.Tasks", Level.DEBUG);
         */
        RuntimeServiceImpl rss = new RuntimeServiceImpl();
        rss.start();
    }
}
