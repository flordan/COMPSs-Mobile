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
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * XMLUtils is an utility class to ease the management of XML files.
 *
 * @author flordan
 */
public class XMLUtils {

    private XMLUtils() {
    }

    /**
     * Loads an XML document
     *
     * @param location
     * @return
     *
     * @throws IOException If any IO errors occur.
     * @throws SAXException If any parse errors occur.
     * @throws ParserConfigurationException during the loading of the file, a
     * DocumentBuilder cannot be created which satisfies the configuration
     * requested.
     */
    public static Document load(String location) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);
        return docFactory.newDocumentBuilder().parse(location);
    }

    /**
     * Queries the value of an attribute of a given xml node
     *
     * @param n xml node
     * @param attribute name of the attribute
     * @return assigned value for the attribute in that node
     */
    public static String getAttributeValue(Node n, String attribute) {
        return n.getAttributes().getNamedItem(attribute).getNodeValue();
    }

    /**
     * Checks if the name of a node matches with a given string
     *
     * @param n node whose name has to be checked
     * @param name supposed name for the node
     * @return {@literal true} if the name of the node matches the name passed
     * as a parameter
     */
    public static boolean isNodeName(Node n, String name) {
        return n.getNodeName().compareTo(name) == 0;
    }

    /**
     * Stores an XMl document into a file.
     *
     * @param doc xml document to be stored
     * @param location absolute path where to write down the document
     * @throws TransformerException If an unrecoverable error occurs during the
     * course of the transformation.
     */
    public static void save(Document doc, String location) throws TransformerException {
        doc.normalize();
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult streamResult = new StreamResult(new File(location));
        transformer.transform(source, streamResult);
    }

}
