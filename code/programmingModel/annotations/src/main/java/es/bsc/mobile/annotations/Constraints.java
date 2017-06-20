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
package es.bsc.mobile.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;

/**
 * The Constraints annotation declares the requirements of a method to run on a
 * node.
 *
 *
 * Programmers add this annotation to methods defined on an interface to
 * describe the minimal requirements to host the computing of that method.
 * Currently, the programmer can restrict the number of cores in the main
 * processor of the resource, the ISA of the processor, the amount of physical
 * memory, the amount of storage space and the operative system of the node.
 *
 * Example of usage of this annotation:
 *
 * <p>
 * <blockquote><pre>
 * {@literal @}Constraints(
 *      processorArchitecture="x86-64",
 *      processorCoreCount=4,
 *      operatingSystemType="Linux"
 *  )
 * void foo();
 * </pre></blockquote>
 * <p>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Constraints {

    static final String UNASSIGNED = "[unassigned]";

    /**
     * Returns the required processor architecture for the resource to run the
     * CE.
     *
     * @return the required architecture for the processor to run the CE
     */
    String processorArchitecture() default UNASSIGNED;

    /**
     * Returns the required number of CPUs for the host to run the CE.
     *
     * @return the required number of CPUs for the host to run the CE.
     */
    int processorCoreCount() default 0;

    /**
     * Returns the required physical memory size in GBs for the host to run the
     * CE.
     *
     * @return the required physical memory size in GBs for the host to run the
     * CE.
     */
    float memoryPhysicalSize() default 0;

    /**
     * Returns the amount of required storage space in GB for the host to run
     * the CE.
     *
     * @return the amount of required storage space in GB for the host to run
     * the CE.
     */
    float storageElemSize() default 0;

    /**
     * Returns the required operative system for the resource to run the CE.
     *
     * @return the required operative system for the resource to run the CE.
     */
    String operatingSystemType() default UNASSIGNED;

}
