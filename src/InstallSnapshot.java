import java.io.Serializable;

public class InstallSnapshot implements Serializable {

	private static final long serialVersionUID = 1L;
	private int term;
	private int leaderId;
	private int lastIncludedIndex;
	private int lastIncludedTerm;
	private int offset;
	private byte[] data;
	private boolean done;

	public InstallSnapshot(int term, int leaderId, int lastIncludedIndex, int lastIncludedTerm, byte[] data, int offset,
			boolean done) {
		this.term = term;
		this.leaderId = leaderId;
		this.lastIncludedIndex = lastIncludedIndex;
		this.lastIncludedTerm = lastIncludedTerm;
		this.offset = offset;
		this.data = data;
		this.done = done;
	}

	public int getTerm() {
		return term;
	}
	public int getLeaderId() {
		return leaderId;
	}

	public int getLastIncludedIndex() {
		return lastIncludedIndex;
	}

	public int getLastIncludedTerm() {
		return lastIncludedTerm;
	}

	public byte[] getData() {
		return data;
	}

	public int getOffset() {
		return offset;
	}

	public boolean isDone() {
		return done;
	}

	@Override
	public String toString() {
		return "InstallSnapshot [dataSize=" + data.length + ", done=" + done + ", lastIncludedIndex=" + lastIncludedIndex
				+ ", lastIncludedTerm=" + lastIncludedTerm + ", leaderId=" + leaderId + ", offset=" + offset + ", term="
				+ term + "]";
	}
}
