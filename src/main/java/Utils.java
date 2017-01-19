import org.apache.storm.tuple.Values;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.logging.Logger;

/**
 * Utilities class.
 */
public class Utils {

    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    /**
     * Generates XML message file from given name and text.
     *
     * @param name message sender.
     * @param text message content.
     * @return XML string.
     */
    public static String generateXmlMessage(String name, String text) {

        String xmlString = null;

        try {
            // Create XMLStreamWriter
            StringWriter stringWriter = new StringWriter();
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(stringWriter);

            // Start document
            xmlStreamWriter.writeStartDocument();
            xmlStreamWriter.writeStartElement("message");

            // Write name
            xmlStreamWriter.writeStartElement("name");
            xmlStreamWriter.writeCharacters(name);
            xmlStreamWriter.writeEndElement();

            // Write text
            xmlStreamWriter.writeStartElement("text");
            xmlStreamWriter.writeCharacters(text);
            xmlStreamWriter.writeEndElement();

            // End document
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndDocument();

            // Flush and close writer
            xmlStreamWriter.flush();
            xmlStreamWriter.close();

            xmlString = stringWriter.getBuffer().toString();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }

        return xmlString;
    }

    /**
     * Converts given XML message to tuple.
     *
     * @param xmlString XML message to be converted.
     * @return tuple built from given XML message.
     */
    public static Values xmlMsgToTuple(String xmlString) {

        String name = null;
        String text = null;

        try {
            // Create document instance to parse given string
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse((new InputSource(new StringReader(xmlString))));
            document.getDocumentElement().normalize();

            // Get message contents
            NodeList nList = document.getElementsByTagName("message");
            Node node = nList.item(0);
            Element element = (Element) node;
            name = element.getElementsByTagName("name").item(0).getTextContent();
            text = element.getElementsByTagName("text").item(0).getTextContent();
        } catch (Exception e) {
            LOGGER.info("Exception occurred: " + e.getMessage());
        }

        return new Values(name, text);
    }
}
