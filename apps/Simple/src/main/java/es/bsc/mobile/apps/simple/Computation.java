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
package es.bsc.mobile.apps.simple;

import es.bsc.mobile.annotations.Orchestration;

@Orchestration
public class Computation {

    public static int increment(int value) {
        return value + 1;
    }

    public static String start(int initialValue) {
        return Integer.toString(increment(initialValue));
    }

    public static void main(String args[]) {
        int initialValue = Integer.parseInt(args[0]);
        String endValue = start(initialValue);
        System.out.println("Initial value was " + initialValue + " end value is " + endValue);
    }
}
