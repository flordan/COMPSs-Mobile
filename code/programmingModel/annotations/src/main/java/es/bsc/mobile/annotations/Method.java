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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Method annotation selects which implementations of that method will be
 * considered as a Core Element by the COMPSs runtime toolkit.
 *
 * Programmers add this annotation to methods defined on an interface to select
 * which implementations of that method will be considered as a Core Element by
 * the runtime toolkit.
 *
 * Example of usage of this annotation:
 *
 * <p>
 * <blockquote><pre>
 *  {@literal @}Method(
 *      declaringClass = {
 *          package.bar.ImplementingClass,
 *          package.foo.AlternativeClass
 *  )
 * String getName();
 * </pre></blockquote>
 * <p>
 *
 * Every call to the getName() implementation of the ImplementingClass and
 * AlternativeClass classes will be considered as a CE invocation and, thus,
 * potentially will be executed in a remote node.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Method {

    /**
     * Set of classes implementing the method.
     *
     * @return an array of Strings containing the names of all the classes
     * implementing the method.
     */
    String[] declaringClass();
}
