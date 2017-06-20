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
package es.bsc.mobile.parallelizer.commands;

import es.bsc.mobile.parallelizer.configuration.Paths;

/**
 * The clean command encapsulates the removal all the persistent modifications
 * on the Android application Eclipse project done by other commands.
 *
 * @author flordan
 */
public class Clean implements Command {

    private static final String LOAD_MANIFEST_ERROR = "Error loading Android Manifest";
    private static final String SAVE_MANIFEST_ERROR = "Error writing down the Android Manifest into a file";

    @Override
    public void execute(String projectDir, Paths paths) throws CommandExecutionException {
    }

}
