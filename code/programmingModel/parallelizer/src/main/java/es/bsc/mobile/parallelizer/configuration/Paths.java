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
package es.bsc.mobile.parallelizer.configuration;

import java.io.File;

/**
 * The Paths class is used for encapsulating the important paths used during the
 * instrumentation process.
 *
 *
 * @author flordan
 */
public interface Paths {

    /**
     *
     * @return Relative path for compiled classes folder
     */
    String compiledClassesDir();

    public static class Maven implements Paths {

        @Override
        public String compiledClassesDir() {
            return File.separator + "classes";
        }

    }

    public static class Eclipse implements Paths {

        @Override
        public String compiledClassesDir() {
            return File.separator + "bin" + File.separator + "classes";
        }

    }
}
