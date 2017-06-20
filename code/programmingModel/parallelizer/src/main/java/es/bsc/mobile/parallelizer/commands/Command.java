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
 * The Command interface represents a command that will be executed on an
 * Android application Eclipse project to modify the application.
 *
 * @author flordan
 */
public interface Command {

    /**
     * Performs the operation of the command
     *
     * @param projectDir Absolute path to the Android Application Eclipse
     * Project
     * @param paths Relative paths were to find the project's classes and
     * resources.
     * @throws CommandExecutionException if any error happens during the command
     * execution.
     */
    void execute(String projectDir, Paths paths) throws CommandExecutionException;

    /**
     * The CommandExecutionException is a generic exception produced during the
     * execution of a command.
     */
    class CommandExecutionException extends Exception {

        /**
         * Constructs a new Command Execution Exception raised by some abnormal
         * condition detected during the command execution.
         *
         * @param errorMessage Description of the error
         */
        CommandExecutionException(String errorMessage) {
            super(errorMessage);
        }

        /**
         * Constructs a new Command Execution Exception raised by an internal
         * Exception.
         *
         * @param errorMessage Description of the error
         * @param cause Exception that originates the Command Execution
         * Exception
         */
        CommandExecutionException(String errorMessage, Exception cause) {
            super(errorMessage, cause);
        }
    }
}
