import java.io.Serializable;

public class RequestVote implements Serializable {

	private static final long serialVersionUID = 5393480540557907015L;

	private int term;
	private int candidateId;
	private int lastLogIndex;
	private int lastLogTerm;

	public RequestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
		this.term = term;
		this.candidateId = candidateId;
		this.lastLogIndex = lastLogIndex;
		this.lastLogTerm = lastLogTerm;
	}

	public int getTerm() {
		return term;
	}

	public void setTerm(int term) {
		this.term = term;
	}

	public int getCandidateId() {
		return candidateId;
	}

	public void setCandidateId(int candidateId) {
		this.candidateId = candidateId;
	}

	public int getLastLogIndex() {
		return lastLogIndex;
	}

	public void setLastLogIndex(int lastLogIndex) {
		this.lastLogIndex = lastLogIndex;
	}

	public int getLastLogTerm() {
		return lastLogTerm;
	}

	public void setLastLogTerm(int lastLogTerm) {
		this.lastLogTerm = lastLogTerm;
	}

	@Override
	public String toString() {
		return "RequestVote [candidateId=" + candidateId + ", lastLogIndex=" + lastLogIndex + ", lastLogTerm="
				+ lastLogTerm + ", term=" + term + "]";
	}
}
