import java.io.Serializable;
import java.util.List;

public class AppendEntries implements Serializable {

	private static final long serialVersionUID = 5393480540557907015L;

	private int term;
	private int leaderId;
	private int prevLogIndex;
	private int prevLogTerm;
	private List<Entry> entries;
	private int leaderCommit;

	public AppendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<Entry> entries,
			int leaderCommit) {
		this.term = term;
		this.leaderId = leaderId;
		this.prevLogIndex = prevLogIndex;
		this.prevLogTerm = prevLogTerm;
		this.entries = entries;
		this.leaderCommit = leaderCommit;
	}

	public int getTerm() {
		return term;
	}

	public int getLeaderId() {
		return leaderId;
	}

	public int getPrevLogIndex() {
		return prevLogIndex;
	}

	public int getPrevLogTerm() {
		return prevLogTerm;
	}

	public List<Entry> getEntries() {
		return entries;
	}

	public int getLeaderCommit() {
		return leaderCommit;
	}

	@Override
	public String toString() {
		return "AppendEntries [entries=" + entries + ", leaderCommit=" + leaderCommit + ", leaderId=" + leaderId
				+ ", prevLogIndex=" + prevLogIndex + ", prevLogTerm=" + prevLogTerm + ", term=" + term + "]";
	}
}
