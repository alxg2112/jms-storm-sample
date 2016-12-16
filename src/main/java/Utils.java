import org.apache.storm.tuple.Values;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

/**
 * Utilities class.
 */
public class Utils {
    public static Values jmsMsgToTuple(Message message) throws JMSException {
        MapMessage mapMessage = (MapMessage)message;
        Values values = new Values(mapMessage.getString("from"), mapMessage.getString("text"));
        return values;
    }
}
