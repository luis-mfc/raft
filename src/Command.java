import java.io.Serializable;

public class Command implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum OP_TYPE {
        PUT, GET, DEL, LIST, CAS
    }

    private String key;
    private String value;
    private String prevValue;
    private OP_TYPE type;

    public Command(OP_TYPE type, String key, String value, String prevValue) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.prevValue = prevValue;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getPrevValue() {
        return prevValue;
    }

    public OP_TYPE getType() {
        return type;
    }

    @Override
    public String toString() {
        switch (type) {
        case PUT:
            return "PUT [key=" + key + ", value=" + value + "]";
        case GET:
            return "GET [key=" + key + "]";
        case DEL:
            return "DEL [key=" + key + "]";
        case CAS:
            return "CAS [key=" + key + ", value=" + value + ", prevValue=" + prevValue + "]";
        default:
            return "LIST";
        }
    }

    public String toLoggingString() {
        switch (type) {
        case PUT:
            return "[type=PUT,key=" + key + ",value=" + value + "]";
        case GET:
            return "[type=GET,key=" + key + "]";
        case DEL:
            return "[type=DEL,key=" + key + "]";
        case CAS:
            return "[type=CAS,key=" + key + ",value=" + value + ",prevValue=" + prevValue + "]";
        default:
            return "[type=LIST]";
        }
    }
}