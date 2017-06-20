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
package es.bsc.mobile.parallelizer;

import es.bsc.mobile.parallelizer.commands.Clean;
import es.bsc.mobile.parallelizer.commands.Command;
import es.bsc.mobile.parallelizer.commands.Command.CommandExecutionException;
import es.bsc.mobile.parallelizer.commands.Instrument;
import es.bsc.mobile.parallelizer.configuration.Paths;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Parallelizer is the entry point to a suite of commands that modify a
 * regular Android application so it can be parallelized and distributed by
 * COMPSs.
 *
 * Currently, the suite offers 4 different commands:
 * <ul>
 * <li>Environment: modifies the application to include the COMPSs runtime
 * toolkit classes</li>
 * <li>Instrument: instruments the application classes to replace the CE
 * invocations by calls to the COMPSs runtime toolkit</li>
 * <li>Manifest: edits the Android Manifest so the application can contact with
 * the COMPSs runtime toolkit service</li>
 * <li>Clean: removes all the persistent modifications on the Android
 * application Eclipse project done by other commands</li>
 * </ul>
 *
 * @author flordan
 */
public class Parallelizer {

    private static final int ARGUMENT_SIZE = 2;
    private static final String INSTRUMENT = "instrument";
    private static final String INSTRUMENT_SHORT = " i";
    private static final String CLEAN = "clean";
    private static final String CLEAN_SHORT = "c";

    private static final int ERR_ARGUMENTS_COUNT_ID = 1;
    private static final String ERR_ARGUMENTS_COUNT = "Invalid number of parameters";

    private static final int ERR_INVALID_COMMAND_ID = 2;
    private static final String ERR_INVALID_COMMAND = "Invalid parameters value";

    private static final int ERR_FOLDER_ID = 3;
    private static final String ERR_FOLDER = "Invalid project path";

    private static final Logger LOGGER = Logger.getLogger(Parallelizer.class.getName());
    private static final String TAB = "\t";

    private Parallelizer() {
    }

    private static String getUsage() {
        StringBuilder sb = new StringBuilder("Usage: \n");
        sb.append(TAB).append("java ").append(Parallelizer.class.getCanonicalName()).append(" COMMAND projectDir \n");
        sb.append(TAB).append("Available Commands :");
        sb.append(TAB).append(TAB).append("* ").append(INSTRUMENT).append(", ").append(INSTRUMENT_SHORT).append(":");
        sb.append(TAB).append(TAB).append("* ").append(CLEAN).append(", ").append(CLEAN_SHORT).append(":");
        return sb.toString();
    }

    /**
     * Main method to launch the suite
     *
     * @param args array of arguments passed to the suite. The first argument is
     * the command to be launched. Currently, the suite offers 4 different
     * commands:
     * <ul>
     * <li>Environment: modifies the application to include the COMPSs runtime
     * toolkit classes</li>
     * <li>Instrument: instruments the application classes to replace the CE
     * invocations by calls to the COMPSs runtime toolkit</li>
     * <li>Manifest: edits the Android Manifest so the application can contact
     * with the COMPSs runtime toolkit service</li>
     * <li>Clean: removes all the persistent modifications on the Android
     * application Eclipse project done by other commands</li>
     * </ul>.
     *
     * The second parameter
     */
    public static void main(String[] args) {

        if (args.length < ARGUMENT_SIZE) {
            LOGGER.log(Level.SEVERE, ERR_ARGUMENTS_COUNT);
            System.exit(ERR_ARGUMENTS_COUNT_ID);
        }

        Logger.getLogger("INSTRUMENTER").setLevel(Level.ALL);

        Command com = null;
        if (args[0].equals(INSTRUMENT) || args[0].equals(INSTRUMENT_SHORT)) {
            com = new Instrument();
        } else if (args[0].equals(CLEAN) || args[0].equals(CLEAN_SHORT)) {
            com = new Clean();
        } else {
            LOGGER.log(Level.SEVERE, ERR_INVALID_COMMAND);
            LOGGER.log(Level.INFO, getUsage());
            System.exit(ERR_INVALID_COMMAND_ID);
        }

        String projectDir = args[1];
        File f = new File(projectDir);
        if (!f.exists() || !f.isDirectory()) {
            LOGGER.log(Level.SEVERE, ERR_FOLDER);
            System.exit(ERR_FOLDER_ID);
        }

        try {
            com.execute(projectDir, new Paths.Eclipse());
        } catch (CommandExecutionException ex) {
            LOGGER.log(Level.SEVERE, "Error running command", ex);
        }
    }

}
