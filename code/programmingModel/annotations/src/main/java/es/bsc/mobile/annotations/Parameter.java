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
 * The Parameter annotation on a method parameter describes to the COMPSs
 * runtime toolkit the type of data it deals with and which action the method
 * will perform on the data.
 *
 * For the runtime toolkit to determine data dependencies among data accesses,
 * the developer must describe the action that the method will perform on the
 * data it deals with. Thus, they have to annotate each method parameter with a
 * Parameter annotation.
 *
 * The Parameter annotation allows the programmer to indicate the type of data
 * and the action that the method will perform on it. The following example
 * describes a method that reads an A instance and an integer and updates a B
 * object.
 * <p>
 * <blockquote><pre>
 * {@literal @}Method(
 *      declaringClass = {
 *          package.bar.ImplementingClass,
 *          package.foo.AlternativeClass
 *  )
 * void foo(
 * {@literal @}Parameter(type=Type.OBJECT, direction=Direction.IN) A a,
 * {@literal @}Parameter(type=Type.INT, direction=Direction.IN) int value,
 * {@literal @}Parameter(type=Type.OBJECT, direction=Direction.INOUT) B b );
 * </pre></blockquote>
 * <p>
 *
 * When a method operates on a file, the parameter value has to be a String
 * containing the path to the file. For instance, a method that creates a new
 * file will look like:
 * <p>
 * <blockquote><pre>
 * {@literal @}Method(
 *      declaringClass = {
 *          package.bar.ImplementingClass,
 *          package.foo.AlternativeClass
 *  )
 * void bar(
 * {@literal @}Parameter(type=Type.FILE, direction=Direction.OUT) String filePath );
 * </pre></blockquote>
 * <p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Parameter {

    /**
     * Type of data that the method deals with
     */
    public enum Type {

        /**
         * The method operates on the file whose path matches with the parameter
         * value.
         */
        FILE,
        /**
         * The data type is a boolean value.
         */
        BOOLEAN,
        /**
         * The data type is a character.
         */
        CHAR,
        /**
         * The data type is a concatenation of char values.
         */
        STRING,
        /**
         * The data type is an 8-bits number.
         */
        BYTE,
        /**
         * The data type is a 16-bits integer number.
         */
        SHORT,
        /**
         * The data type is an integer number.
         */
        INT,
        /**
         * The data type is a double-precision integer number.
         */
        LONG,
        /**
         * The data type is a floating point number.
         */
        FLOAT,
        /**
         * The data type is a double-precision floating point number.
         */
        DOUBLE,
        /**
         * The data type is an object.
         */
        OBJECT,
        /**
         * The data type is not specified.
         */
        UNSPECIFIED;
    }

    /**
     * Action that the method will perform on the data
     */
    public enum Direction {

        /**
         * The method only reads the input value
         */
        IN,
        /**
         * The method will read and modify the current data instance
         */
        INOUT,
        /**
         * The method will create a new instance of the data
         */
        OUT;
    }

    /**
     * Indicates the type of data of the annotated parameter.
     *
     * @return
     */
    Type type() default Type.UNSPECIFIED;

    /**
     * Indicates the action that the method will perform on the data. By
     * default, methods only read the data.
     *
     * @return Direction indicating the action to perform on the data
     */
    Direction direction() default Direction.IN;

}
