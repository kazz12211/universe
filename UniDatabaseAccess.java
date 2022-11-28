package universe;

public abstract class UniDatabaseAccess {

	UniObjectsSession session;

	protected UniDatabaseAccess(UniObjectsSession session) {
		this.session = session;
	}
	
	
	protected boolean isConnected() {
		return session.isConnected();
	}
	
	public UniObjectsSession session() {
		return session;
	}
	
	
}
