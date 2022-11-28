package universe;

import java.util.HashMap;
import java.util.Map;

public class UniEntityCache {

	Map<UniEntityID, Object> cache = new HashMap<UniEntityID, Object>();
	
	public UniEntityID add(Object object) {
		UniEntity entity = UniContext.get().entityForObject(object);
		if(entity == null)
			return null;
		Object primaryKey = entity.primaryKeyForObject(object);
		if(primaryKey == null)
			return null;
		if(this.contains(entity.entityName(), primaryKey)) {
			return replace(entity.entityName(), primaryKey, object);
		} else {
			UniEntityID entityID = new UniEntityID(entity.entityName(), primaryKey);
			cache.put(entityID, object);
			return entityID;
		}
	}
	
	private UniEntityID replace(String entityName, Object primaryKey, Object object) {
		UniEntityID entityID = lookupEntityID(entityName, primaryKey);
		if(entityID != null) {
			cache.put(entityID, object);
		}
		return entityID;
	}
	
	public Object get(String entityName, Object primaryKey) {
		UniEntityID entityID = lookupEntityID(entityName, primaryKey);
		if(entityID != null) {
			return cache.get(entityID);
		}
		return null;
	}
	
	public Object get(UniEntityID entityID) {
		return this.get(entityID.entityName(), entityID.primaryKey());
	}
	
	public UniEntityID lookupEntityID(Object entityName, Object primaryKey) {
		for(UniEntityID entityID : cache.keySet()) {
			if(entityID.entityName().equals(entityName) && entityID.primaryKey().equals(primaryKey))
				return entityID;
		}
		return null;
	}
	
	public boolean contains(String entityName, Object primaryKey) {
		return lookupEntityID(entityName, primaryKey) != null;
	}

	public boolean contains(Object object) {
		UniEntity entity = UniContext.get().entityForObject(object);
		if(entity == null)
			return false;
		Object primaryKey = entity.primaryKeyForObject(object);
		if(primaryKey == null)
			return false;
		return contains(entity.entityName(), primaryKey);
	}
	
	public void remove(Object object) {
		UniEntityID found = null;
		UniEntity entity = UniContext.get().entityForObject(object);
		if(entity == null)
			return;
		Object primaryKey = entity.primaryKeyForObject(object);
		if(primaryKey == null)
			return;
		found = lookupEntityID(entity.entityName(), primaryKey);
		if(found != null)
			cache.remove(found);
	}
	
	public void clear() {
		cache.clear();
	}
}
