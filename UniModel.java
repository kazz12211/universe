package universe;

import java.util.HashMap;
import java.util.Map;

public class UniModel {
	String name;
	Map<Class<?>, UniEntity> entities = new HashMap<Class<?>, UniEntity>();
	UniConnectionInfo connectionInfo;
	
	public static UniModel modelNamed(String modelName) {
		return UniModelGroup.modelNamed(modelName);
	}
	
	public Map<Class<?>, UniEntity> entities() {
		return entities;
	}
	
	public void addEntity(Class<?> objectClass, UniEntity entity) {
		entities.put(objectClass, entity);
	}
	
	public UniEntity entityForClass(Class<?> objectClass) {
		return entities.get(objectClass);
	}
	
	public UniEntity entityNamed(String entityName) {
		for(UniEntity entity : entities.values()) {
			if(entity.entityName().equals(entityName))
				return entity;
		}
		return null;
	}
	
	public UniConnectionInfo connectionInfo() {
		return connectionInfo;
	}
	
	public String name() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String toString() {
		return "{name="+name+"; entities=" + entities.values().toString() + "; connectionInfo=" + connectionInfo.toString() + "}";
	}
}
