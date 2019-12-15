import java.io.Serializable;

public class Response implements Serializable {

    private static final long serialVersionUID = 1L;
    private int term;
    private boolean success;

    public Response(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public boolean getSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "Response [success=" + success + ", term=" + term + "]";
    }
}