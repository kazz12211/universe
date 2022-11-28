package universe;

import java.util.ArrayList;
import java.util.List;

import universe.util.UniLogger;
import ariba.util.core.ListUtil;

public class UniAssociation {
	String name;
	String key;
	boolean isToMany;
	Class<?> associationClass;
	UniFieldRef[] fields;
	UniEntity entity;
	boolean shouldPrefetch = false;
	
	public UniAssociation(UniEntity entity, String name, String key, boolean isToMany, Class<?> associationClass) {
		this.entity = entity;
		this.name = name;
		this.key = key;
		this.isToMany = isToMany;
		this.associationClass = associationClass;
	}
	
	public String name() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String key() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public boolean isToMany() {
		return isToMany;
	}
	public void setToMany(boolean isToMany) {
		this.isToMany = isToMany;
	}
	public Class<?> associationClass() {
		return associationClass;
	}
	public void setAssociationClass(Class<?> associationClass) {
		this.associationClass = associationClass;
	}
	public UniFieldRef[] fields() {
		return fields;
	}
	public void setFields(UniFieldRef[] fields) {
		this.fields = fields;
	}
	public UniEntity entity() {
		return entity;
	}
	public void setEntity(UniEntity entity) {
		this.entity = entity;
	}
	public boolean shouldPrefetch() {
		return shouldPrefetch;
	}

	public List<UniField> fieldsInEntity(UniEntity entity) {
		List<UniField> fieldList = new ArrayList<UniField>();
		for(UniFieldRef ref : fields) {
			UniField field = entity.fieldWithColumnName(ref.fieldName());
			UniLogger.universe_dev.debug(field);
			fieldList.add(field);
		}
		return fieldList;
	}
	
	public String toString() {
		List<String> s = new ArrayList<String>();
		List<UniField> list = this.fieldsInEntity(entity);
		for(UniField field : list) {
			s.add(field.toString());
		}
		String ss = ListUtil.listToString(s, ", ");
		
		return "association {name=" + name + "; key=" + key + "; associationClass=" + associationClass + "(" + ss + ")}";
	}
	
}
