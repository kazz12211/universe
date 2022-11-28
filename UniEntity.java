package universe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import universe.object.UniFaultingList;
import universe.util.UniLogger;
import util.Accessor;
import ariba.util.core.ClassUtil;
import ariba.util.core.ListUtil;
import ariba.util.fieldvalue.FieldValue;

public class UniEntity {

	public static final String OptimisticLock = "optimistic";
	public static final String PessimisticLock = "pessimistic";
	public static final String FileLock = "file";
	
	public enum LockingStrategy {
		Optimistic, Pessimistic, File
	}

	UniModel model;
	String filename;
	String entityName;
	Class<?> entityClass;
	UniField[] fields;
	UniAssociation[] associations;
	UniPrimaryKeyGenerator pkGenerator;
	LockingStrategy lockingStrategy = LockingStrategy.Optimistic;
	List<UniFieldDefinition> definitions;
	
	public UniEntity(UniModel model, Class<?> entityClass, String entityName, String filename, UniField[] fields) {
		this.entityClass = entityClass;
		this.entityName = entityName;
		this.filename = filename;
		this.model = model;
		this.fields = fields;
	}
	
	public UniEntity(UniModel model, Class<?> entityClass, String entityName, String filename) {
		this.entityClass = entityClass;
		this.entityName = entityName;
		this.filename = filename;
		this.model = model;
	}

	public void setModel(UniModel model) {
		this.model = model;
	}
	public UniModel model() {
		return model;
	}
	public Object primaryKeyForObject(Object object) {
		UniField pkField = primaryKeyField();
		if(pkField != null)
			return FieldValue.getFieldValue(object, pkField.key());
		return null;
	}
	public void setPrimaryKeyForObject(Object pk, Object object) {
		UniField pkField = primaryKeyField();
		if(pkField != null)
			FieldValue.setFieldValue(object, pkField.key(), pk);
	}
	
	public String entityName() {
		return entityName;
	}
	public Class<?> entityClass() {
		return entityClass;
	}
	public UniField[] fields() {
		return fields;
	}
	public String filename() {
		return filename;
	}
	public UniAssociation[] associations() {
		return associations;
	}
	
	public LockingStrategy lockingStrategy() {
		return lockingStrategy;
		
	}
	
	public List<UniFieldDefinition> dictionaryDefinitions() {
		return definitions;
	}
	public void setDictionaryDefinitions(List<UniFieldDefinition> definitions) {
		this.definitions = definitions;
	}

	public UniField primaryKeyField() {
		for(UniField field : fields) {
			if(field.isPrimaryKey)
				return field;
		}
		return null;
	}
	
	public UniField fieldNamed(String fieldName) {
		for(UniField field : fields) {
			if(field.key().equals(fieldName))
				return field;
		}
		return null;
	}
	
	public UniField fieldWithColumnName(String columnName) {
		for(UniField field : fields) {
			if(field.columnName().equals(columnName))
				return field;
		}
		return null;
	}

	public UniAssociation associationNamed(String assocName) {
		for(UniAssociation assoc : associations) {
			if(assoc.name.equals(assocName))
				return assoc;
		}
		return null;
	}
	
	public void initObject(Object object, Map<String, Object> row, UniContext context) throws Exception {
		for(UniField field : fields) {
			if(field.isAssociated())
				continue;
			String columnName = field.columnName();
			String key = field.key();
			Object value = row.get(columnName);
			if(Accessor.newSetAccessor(object.getClass(), key) == null) {
				UniLogger.universe.warn("Instance of entity '" + field.entity().entityName() + "' does not have property '" + key + "'");
				continue;
			}
			try {
				FieldValue.setFieldValue(object, key, field.coerceValue(value));
			} catch (Exception e) {
				UniLogger.universe.error("UniEntity.initObject(): could not set value '" + value + "' to field '" + key + "'.\n value is " + 
			(value != null ? value.getClass().getName() : "null") + " expected value is " + field.valueClass().getName(), e);
				throw e;
			}
		}
		
		for(UniAssociation assoc : associations) {
			_initAssociation(object, row, assoc);
		}
		
		context.didFetch(object);
	}
	
	private void _initAssociation(Object object, Map<String, Object> record, UniAssociation assoc) throws Exception {
		if(Accessor.newSetAccessor(object.getClass(), assoc.key()) == null) {
			UniLogger.universe.warn("Entity '" + assoc.entity().entityName() + "' does not have property '" + assoc.key() + "'");
			return;
		}
		if(!assoc.shouldPrefetch()) {
			UniFaultingList faultingList = new UniFaultingList(object, assoc);
			faultingList.setFault(true);
			FieldValue.setFieldValue(object, assoc.key(), faultingList);
			return;
		}
		try {
			UniEntity entity = assoc.entity();
			Class<?> assocClass = assoc.associationClass();
			List<Map<String, String>> rows = (List<Map<String, String>>) record.get(assoc.key());
			List subRows = new ArrayList();
			for(Map<String, String> row : rows) {
				Object obj = ClassUtil.newInstance(assocClass);
				for(String key : row.keySet()) {
					UniField f = entity.fieldNamed(key);
					FieldValue.setFieldValue(obj, key, f.coerceValue(row.get(key)));
				}
				subRows.add(obj);
			}
			FieldValue.setFieldValue(object, assoc.key(), subRows);
		} catch (Exception e) {
			UniLogger.universe.error("_initAssociation(): could not set value '" + record.get(assoc.key()) + "' to field '" + assoc.key() + "'", e);
			throw e;
		}
	}
	
	public void initAssocation(Object object, List<Map<String, String>> rows,
			UniAssociation assoc) throws Exception {
		if(Accessor.newSetAccessor(object.getClass(), assoc.key()) == null) {
			UniLogger.universe.warn("initAssociation(): Entity '" + assoc.entity().entityName() + "' does not have property '" + assoc.key() + "'");
			return;
		}
		try {
			UniEntity entity = assoc.entity();
			Class<?> assocClass = assoc.associationClass();
			List subRows = new ArrayList();
			for(Map<String, String> row : rows) {
				Object obj = ClassUtil.newInstance(assocClass);
				for(String key : row.keySet()) {
					UniField f = entity.fieldNamed(key);
					FieldValue.setFieldValue(obj, key, f.coerceValue(row.get(key)));
				}
				subRows.add(obj);
			}
			FieldValue.setFieldValue(object, assoc.key(), subRows);
		} catch (Exception e) {
			UniLogger.universe.error("initAssociation(): could not set value '" + rows + "' to field '" + assoc.key() + "'", e);
			throw e;
		}
	}

	public List resolveAssocation(Object object, List<Map<String, String>> rows, UniAssociation assoc) throws Exception {
		if(Accessor.newSetAccessor(object.getClass(), assoc.key()) == null) {
			UniLogger.universe.warn("resolveAssocation(): Entity '" + assoc.entity().entityName() + "' does not have property '" + assoc.key() + "'");
			return null;
		}
		try {
			UniEntity entity = assoc.entity();
			Class<?> assocClass = assoc.associationClass();
			List subRows = new ArrayList();
			for(Map<String, String> row : rows) {
				Object obj = ClassUtil.newInstance(assocClass);
				for(String key : row.keySet()) {
					UniField f = entity.fieldNamed(key);
					FieldValue.setFieldValue(obj, key, f.coerceValue(row.get(key)));
				}
				subRows.add(obj);
			}
			return subRows;
		} catch (Exception e) {
			UniLogger.universe.error("resolveAssocation(): could not set value '" + rows + "' to field '" + assoc.key() + "'", e);
			throw e;
		}
	}

	@Override
	public String toString() {
		return "{entityName=" + entityName + "; filename=" + filename + "; entityClass=" + entityClass.getName() + "; fields=" + fieldsDescription() + "}";
	}

	private String fieldsDescription() {
		List<String> descs = new ArrayList<String>();
		for(UniField field : fields) {
			descs.add(field.toString());
		}
		return "(" + ListUtil.listToString(descs, ", ") + ")";
	}
	
	public UniPrimaryKeyGenerator pkGenerator() {
		return pkGenerator;
	}
	public void setPkGenerator(UniPrimaryKeyGenerator pkGenerator) {
		this.pkGenerator = pkGenerator;
	}
	
	public int fieldNameToLocation(String fieldName) {
		UniFieldDefinition def = this.fieldDefinitionWithFieldName(fieldName);
		if(def != null)
			return def.location();
		UniField field = this.fieldNamed(fieldName);
		if(field != null)
			return field.location();
		return -1;
	}

	public UniFieldDefinition fieldDefinitionForField(UniField field) {
		return fieldDefinitionWithFieldName(field.columnName());
	}
	public UniFieldDefinition fieldDefinitionWithFieldName(String fieldName) {
		if(definitions != null) {
			for(UniFieldDefinition def : definitions) {
				if(fieldName.equals(def.fieldName())) {
					return def;
				}
						
			}
		}
		return null;
	}

}
