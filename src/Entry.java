import java.io.Serializable;

public class Entry implements Serializable {

    private static final long serialVersionUID = 1L;
    private int term;
    private int index;
    private Request request;

    public Entry(int term, int index, Request request) {
        this.term = term;
        this.index = index;
        this.request = request;
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public Request getRequest() {
        return request;
    }

    @Override
    public String toString() {
        return "Entry [index=" + index + ", request=" + request + ", term=" + term + "]";
    }

    public String toLoggingString() {
        return "[index=" + index + ",term=" + term + "," + request.toLoggingString() + "]";
    }
}