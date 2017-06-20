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
package es.bsc.mobile.worker;

import es.bsc.comm.Node;
import es.bsc.comm.nio.NIONode;
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.utils.LoggerConfiguration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Worker {

    private static final int DEFAULT_PORT_WORKER = 43001;
    private static final int DEFAULT_PORT_MASTER = 43000;
    private static final int DEFAULT_SLOTS = 0;
    private static final String DEFAULT_MASTER_IP = "";

    private static final int EXPECTED_PARAMS = 4;
    private static final int ARG_IDX_PORT_WORKER = 0;
    private static final int ARG_IDX_SLOTS = 1;
    private static final int ARG_IDX_IP_MASTER = 2;
    private static final int ARG_IDX_PORT_MASTER = 3;

    private static final String ARG_COUNT_ERR_TEXT = "Insufficient number of arguments: ";
    private static final int ARG_COUNT_ERR_CODE = 1;
    private static final String ARG_TYPE_ERR_TEXT = "Unexpected type of parameter: expected  OWN_PORT (int), SLOTS (int), MASTER_IP(String), MASTER_RUNTIME_PORT(int)";
    private static final int ARG_TYPE_ERR_CODE = 2;

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Worker");

    private Worker() {
    }

    public static void main(String[] args) throws Exception {

        LoggerConfiguration.configRootLogger(Level.OFF);
        LoggerConfiguration.configLogger("Communication", Level.OFF);
        LoggerConfiguration.configLogger("Runtime", Level.OFF);
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
        LoggerConfiguration.configLogger("Runtime.Worker", Level.DEBUG);

        int myPort = DEFAULT_PORT_WORKER;
        int slots = DEFAULT_SLOTS;
        String masterIP = DEFAULT_MASTER_IP;
        int masterRuntimePort = DEFAULT_PORT_MASTER;

        if (args.length < EXPECTED_PARAMS) {
            LOGGER.fatal(ARG_COUNT_ERR_TEXT + args.length + " (" + EXPECTED_PARAMS + " expected).");
            System.exit(ARG_COUNT_ERR_CODE);
        }

        try {
            myPort = Integer.parseInt(args[ARG_IDX_PORT_WORKER]);
            slots = Integer.parseInt(args[ARG_IDX_SLOTS]);
            masterIP = args[ARG_IDX_IP_MASTER];
            masterRuntimePort = Integer.parseInt(args[ARG_IDX_PORT_MASTER]);
        } catch (NumberFormatException e) {
            LOGGER.fatal(ARG_TYPE_ERR_TEXT);
            System.exit(ARG_TYPE_ERR_CODE);
        }

        final Node me = new NIONode(null, myPort);
        LOGGER.info("Opening server on " + me + " allowing " + slots + " tasks");

        final Node masterRuntime = new NIONode(masterIP, masterRuntimePort);
        LOGGER.info("Locating runtime at " + masterRuntime);

        RuntimeWorker rw = new RuntimeWorker(slots, me, masterRuntime);
        CommunicationManager.start(rw);

    }
}
