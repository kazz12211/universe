package universe;

public class UniFieldRef {
	String fieldName;
	UniAssociation association;
	UniEntity entity;
	
	public UniFieldRef(UniEntity entity, UniAssociation association, String fieldName) {
		this.entity = entity;
		this.association = association;
		this.fieldName = fieldName;
	}
	
	public UniEntity entity() {
		return entity;
	}
	
	public UniAssociation association() {
		return association;
	}
	
	public String fieldName() {
		return fieldName;
	}
	
}
