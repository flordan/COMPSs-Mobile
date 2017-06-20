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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;

/**
 * The FileUtils class is an utility class to ease the operations with files and
 * folders.
 *
 * @author flordan
 */
public class FileUtils {

    private FileUtils() {
    }

    /**
     * Copies a file / directory (recursively) from src to dest.
     *
     * @param src File To be copied
     * @param dest Directory where to clone the src file/directory
     * @throws IOException if an IO error occurs during the copy process
     */
    public static void copy(final File src, final File dest) throws IOException {
        if (!src.isDirectory()) {
            Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } else {
            if (!dest.exists()) {
                if (!dest.mkdir()) {
                    throw new IOException("Can not create directory " + dest.getAbsolutePath());
                }
            }
            String[] files = src.list();
            for (String file : files) {
                File srcFile = new File(src, file);
                File destFile = new File(dest, file);
                copy(srcFile, destFile);
            }
        }
    }

    /**
     * Lists all the .class files contained in a given directory
     *
     * @param directory Path of the folder to inspect
     * @return list of paths of files containing java bytecode in that directory
     */
    public static LinkedList<String> listClasses(String directory) {
        LinkedList<String> result = new LinkedList<String>();
        File f = new File(directory);
        if (!f.isDirectory()) {
            String extension = f.getName();
            int idx = extension.indexOf('.');
            while (idx > -1) {
                extension = extension.substring(idx + 1);
                idx = extension.indexOf('.');
            }
            if (extension.compareTo("class") == 0) {
                result.add(directory);
            }
        } else {
            String[] files = f.list();
            for (String file : files) {
                result.addAll(listClasses(directory + File.separator + file));
            }
        }
        return result;
    }
}
