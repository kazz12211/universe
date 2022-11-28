package universe;

public class UniEntityID {

	private String entityName;
	private Object primaryKey;
	private int hashCode;

	public UniEntityID(String entityName, Object primaryKey) {
		this.entityName = entityName;
		this.primaryKey = primaryKey;
		this.hashCode = _hashCode();
	}

	public Object primaryKey() {
		return primaryKey;
	}

	public String entityName() {
		return entityName;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof UniEntityID) {
			return (entityName.equals(((UniEntityID)obj).entityName) && primaryKey.equals(((UniEntityID)obj).primaryKey));
		}
		return false;
	}
	
	private int _hashCode() {
        int code = entityName.hashCode();
        if(primaryKey != null)
        	if(primaryKey instanceof Number)
        		code ^= ((Number) primaryKey).intValue();
        	else
        		code ^= (primaryKey.toString()).hashCode();
        return code == 0 ? 42 : code;
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}

}
