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
package es.bsc.mobile.parallelizer.utils;

import es.bsc.mobile.annotations.JavaMethod;
import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

/**
 * The InstrumentationAssistant class is an utility class to ease the
 * instrumentation of the application. It collects all the classes of the
 * application and relates them to the methods defined in the Core Element
 * Interface.
 *
 * @author flordan
 */
public class InstrumentationAssistant {

    private static final int CLASS_EXTENSION_LENGTH = ".class".length();
    private static String classesDir;
    private static final HashMap<String, CtMethod[]> methods = new HashMap<String, CtMethod[]>();
    private static final HashSet<String> mainClasses = new HashSet<String>();

    private InstrumentationAssistant() {
    }

    /**
     * Looks for all the classes in a directory and registers the ones that are
     * annotated by an {@literal @}Orchestration annotation.
     *
     * @param directory Absolute path of the directory where to look for classes
     * @throws ClassNotFoundException if the given class is not loaded in the
     * classpath.
     * @throws NotFoundException if the given class is not loaded in the
     * Javassist class pool.
     */
    public static void load(String directory) throws ClassNotFoundException, NotFoundException {
        classesDir = directory;
        for (String className : FileUtils.listClasses(directory)) {
            className = fileToClass(className);
            Class<?> clss = Class.forName(className);
            if (clss.isAnnotationPresent(es.bsc.mobile.annotations.Orchestration.class)) {
                CtClass clssClass = ClassPool.getDefault().get(className);
                methods.put(className, clssClass.getDeclaredMethods());
            }
            try {
                Method main = clss.getMethod("main", String[].class);
                if (main != null) {
                    mainClasses.add(className);
                }
            } catch (NoSuchMethodException ex) {
                //No need to do anything
            } catch (SecurityException ex) {
                //Not a public method. No need to do anything
            }
        }
    }

    private static String fileToClass(String file) {
        String className = file.substring(classesDir.length() + 1, file.length() - CLASS_EXTENSION_LENGTH);
        return className.replace(File.separator, ".");
    }

    /**
     * Looks for a method in the Core Element Interface
     *
     * @param method Method detected by the Javassist library
     * @param ceiMethods All methods declared in the CEI
     * @return returns the method from the ceiMethods array that matches with
     * the Javassist method. {@literal null} if the method is not in the CEI.
     * @throws NotFoundException the javassist method has no parameters
     */
    public static Method checkRemote(CtMethod method, Method[] ceiMethods)
            throws NotFoundException {
        for (Method ceiMethod : ceiMethods) {
            if (ceiMethod.isAnnotationPresent(es.bsc.mobile.annotations.CoreElement.class) && isSelectedMethod(method, ceiMethod)) {
                return ceiMethod;
            }
        }
        return null;
    }

    private static boolean isSelectedMethod(CtMethod method, Method remote) throws NotFoundException {

        // Check if methods have the same name
        if (!remote.getName().equals(method.getName())) {
            return false;
        }

        es.bsc.mobile.annotations.CoreElement coreElement = remote.getAnnotation(es.bsc.mobile.annotations.CoreElement.class);

        // Check if methods belong to the same class
        boolean matchesClass = false;
        JavaMethod[] ceiMethods = coreElement.methods();
        for (int i = 0; i < ceiMethods.length && !matchesClass; i++) {
            String remoteDeclaringClass = ceiMethods[i].declaringClass();
            matchesClass = remoteDeclaringClass.compareTo(method.getDeclaringClass().getName()) == 0;
        }
        if (!matchesClass) {
            return false;
        }

        // Check that methods have the same number of parameters
        CtClass[] paramClassCurrent = method.getParameterTypes();
        Class<?>[] paramClassRemote = remote.getParameterTypes();
        if (paramClassCurrent.length != paramClassRemote.length) {
            return false;
        }

        // Check that parameter types match
        for (int i = 0; i < paramClassCurrent.length; i++) {
            if (!paramClassCurrent[i].getName().equals(
                    paramClassRemote[i].getCanonicalName())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Gets all the classes that have been annotated with an
     * {@literal @}Orchestration annotation.
     *
     * @return list of Canonical class names that have been annotated with an
     * {@literal @}Orchestration annotation.
     */
    public static Set<String> getOrchestrationClasses() {
        return methods.keySet();
    }

    public static boolean isOrchestratonClass(String className) {
        return methods.containsKey(className);
    }

    /**
     * Gets all the classes that have a main(String[]) method.
     *
     * @return list of Canonical class names that a main(String[]) method.
     */
    public static Set<String> getMainClasses() {
        return mainClasses;
    }

    public static boolean isMainClass(String className) {
        return mainClasses.contains(className);
    }

    /**
     * Checks if the method gives as a parameter is declared has been
     * instrumented.
     *
     * @param method Method to check
     * @return{@literal true} if is declared in a class annotated with
     * {@literal @}Orchestration
     */
    public static boolean isEdited(CtMethod method) {
        String className = method.getDeclaringClass().getName();
        CtMethod[] edMethods = methods.get(className);
        if (edMethods != null) {
            for (CtMethod edMethod : edMethods) {
                if (method.equals(edMethod)) {
                    return true;
                }
            }
        }
        return false;
    }
}
