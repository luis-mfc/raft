import java.io.Serializable;

public class ElectionResult implements Serializable {

    private static final long serialVersionUID = 1L;
    private int term;
    private boolean voteGranted;

    public ElectionResult(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean getVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "ElectionResult [term=" + term + ", voteGranted=" + voteGranted + "]";
    }
}