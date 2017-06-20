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
package es.bsc.mobile.parallelizer.resources;

import es.bsc.mobile.parallelizer.utils.XMLUtils;
import java.io.IOException;
import java.util.HashMap;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * The Strings class is an utility to read and modify the Strings.xml file of an
 * Android application.
 *
 * @author flordan
 */
public class Strings {

    private final Document stringsDoc;
    private final HashMap<String, String> values = new HashMap<String, String>();
    private final Node resources;

    /**
     * Loads a given strings.xml file and parses all the values.
     *
     * @param stringLocation Absolute path where the string file is located.
     *
     * @throws IOException If any IO errors occur.
     * @throws SAXException If any parse errors occur.
     * @throws ParserConfigurationException during the loading of the manifest
     * file, a DocumentBuilder cannot be created which satisfies the
     * configuration requested.
     */
    public Strings(String stringLocation) throws ParserConfigurationException, SAXException, IOException {
        stringsDoc = XMLUtils.load(stringLocation);
        Node resNode = null;
        NodeList nl = stringsDoc.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeName().compareTo("resources") == 0) {
                resNode = n;
                parseResources(n);
            }
        }
        resources = resNode;
    }

    private void parseResources(Node resources) {
        NodeList nl = resources.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (XMLUtils.isNodeName(n, "string")) {
                parseString(n);
            }
        }
    }

    private void parseString(Node string) {
        String name = XMLUtils.getAttributeValue(string, "name");
        String value = string.getTextContent();
        values.put(name, value);
    }

    /**
     * Adds a new key-value pair to the string file
     *
     * @param name Name that identifies that string
     * @param value Value associated to that name
     */
    public void addValue(String name, String value) {
        if (values.get(name) != null) {
            return;
        }
        Element n = stringsDoc.createElement("string");
        n.setAttribute("name", name);
        n.setTextContent(value);
        resources.appendChild(n);
        values.put(name, value);
    }

    /**
     * Removes a key-value pair from the string file
     *
     * @param name Name that identifies the string to be removed
     * @return returns the value associated to the removed key
     */
    public String removeValue(String name) {
        String val = values.remove(name);

        if (val != null) {
            NodeList nl = resources.getChildNodes();
            Node pair = null;
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);
                if (n.getAttributes() != null
                        && n.getAttributes().getNamedItem("name") != null
                        && name.equals(XMLUtils.getAttributeValue(n, "name"))) {
                    pair = n;
                }
            }
            resources.removeChild(pair);
        }
        return val;
    }

    /**
     * Makes persistent all the changes performed to the string file by writing
     * them down into a file
     *
     * @param location Absolute path where to store the file containing all the
     * key-value pairs.
     * @throws TransformerException If an unrecoverable error occurs during the
     * course of the transformation.
     */
    public void saveModifications(String location) throws TransformerException {
        XMLUtils.save(stringsDoc, location);
    }
}
