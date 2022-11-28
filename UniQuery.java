package universe;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import universe.util.UniDictionaryUtil;
import universe.util.UniLogger;
import universe.util.UniStringUtil;
import asjava.uniclientlibs.UniDataSet;
import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniException;
import asjava.uniclientlibs.UniRecord;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniCommand;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniSelectList;

public class UniQuery extends UniDatabaseAccess {

	
	public UniQuery(UniObjectsSession session) {
		super(session);
	}
	
	public List<Map<String, Object>> executeQuery(UniObjectsSession.Select select, UniEntity entity) throws Exception {
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		
		UniFile file = null;
		UniCommand command = null;
		
		try {
			session().establishConnection();
			UniCommandGeneration generator = new UniCommandGeneration(session());
			if(entity.dictionaryDefinitions() == null) {
				entity.setDictionaryDefinitions(UniDictionaryUtil.uniFieldDefinitions(select.filename(), session()));
			}
			file = session().getFile(select.filename());
			command = select.uniCommand(generator);
			session().executeUniCommand(command);
			UniSelectList list = session().selectList(select.listNumber());
			for(UniString recordId = list.next(); !list.isLastRecordRead(); recordId = list.next()) {
				file.setRecordID(recordId);
				Map<String, Object> row = new HashMap<String, Object>();
				row.put("@ID", UniStringUtil.coerceToString(recordId));
				for(UniField field: entity.fields()) {
					if(field.isMultiValue() && !field.isAssociated()) {
						List<String> values = this.readDynArray(file, recordId, field.columnName());
						UniLogger.universe_dev.debug(field.columnName() + "=" + values);
						row.put(field.columnName(), values);
					} else if(!field.isMultiValue()) {
						UniString value = file.readNamedField(recordId, field.columnName());
						UniLogger.universe_dev.debug(field.columnName() + "=" + value);
						row.put(field.columnName(), UniStringUtil.coerceToString(value));
					}
				}
				
				for(UniAssociation assoc : entity.associations()) {
					if(assoc.shouldPrefetch()) {
						UniLogger.universe_test.debug("Prefetching association " + assoc.name());
						List<Map<String, String>> subRows = this.loadAssociations(file, recordId, entity, assoc);
						String assocKey = assoc.key();
						row.put(assocKey, subRows);
					}
				}
				results.add(row);
			}
		} finally {
			if(command != null)
				command.cancel();
		}
		
		return results;
	}
	
	private List<String> readDynArray(UniFile file, UniString recordId, String fieldName) throws UniException {
		UniDataSet rowSet = new UniDataSet();
		rowSet.append(UniStringUtil.coerceToString(recordId));
		return this.readDynArray(file, rowSet, fieldName);
	}
	
	private List<String> readDynArray(UniFile file, UniDataSet rowSet, String fieldName) throws UniException {
		UniDataSet dataSet = file.readNamedField(rowSet, fieldName);
		UniRecord record = dataSet.getUniRecord();
		UniDynArray array = new UniDynArray(record.getRecord());
		int size = array.dcount(1);
		List<String> result = new ArrayList<String>();
		for(int i = 1; i <= size; i++) {
			UniString value = array.extract(1, i);
			//UniLogger.universe_test.debug("     Extract(" + i + ") " + value);
			result.add(UniStringUtil.coerceToString(value));
		}
		return result;
	}
	
	private String readDynArray(UniFile file, UniString pk , UniField field, int valueLocation) throws UniException {
		UniString string = file.readField(pk, field.location(), valueLocation);
		return string.toString();
	}
	
	private List<Map<String, String>> loadAssociations(UniFile file, UniString recordId, UniEntity entity, UniAssociation assoc) throws Exception {
		Map<String, List<String>> valuesByField = new HashMap<String, List<String>>();
		List<UniField> fields = assoc.fieldsInEntity(entity);
		int numValues = 0;
		for(UniField field : fields) {
			UniLogger.universe_dev.debug("loadAssociations: reading field '" + field.columnName() + "' in association '" + assoc.name() + "'");
			List<String> values = this.readDynArray(file, recordId, field.columnName());
			//UniLogger.universe_test.debug(field.columnName() + "=" + values);
			if(numValues < values.size())
				numValues = values.size();
			valuesByField.put(field.key(), values);
		}
		List<Map<String, String>> subRows = new ArrayList<Map<String, String>>();
		for(int i = 0; i < numValues; i++) {
			Map<String, String> subRow = new HashMap<String, String>();
			for(String key : valuesByField.keySet()) {
				List<String> values = valuesByField.get(key);
				if(values.size() <= i)
					subRow.put(key, null);
				else
					subRow.put(key, values.get(i));
			}
			subRows.add(subRow);
		}
		return subRows;
	}


	private List<Map<String, String>> loadAssociation(Object object, UniEntity entity, UniAssociation assoc) throws Exception {
		Object pk = entity.primaryKeyForObject(object);
		List<UniField> fields = assoc.fieldsInEntity(entity);
		UniDataSet rowSet = new UniDataSet();
		rowSet.append(pk.toString());
		Map<String, List<String>> valuesByField = new HashMap<String, List<String>>();
		int numValues = 0;
		List<Map<String, String>> subRows = new ArrayList<Map<String, String>>();

		UniFile file = null;
		
		try {
			file = session().getFile(entity.filename());
			
			for(UniField field : fields) {
				UniLogger.universe_dev.debug("loadAssociation: reading field '" + field.columnName() + "' in association '" + assoc.name() + "'");
				List<String> values = this.readDynArray(file, rowSet, field.columnName());
				UniLogger.universe_dev.debug(field.columnName() + "=" + values);
				if(numValues < values.size())
					numValues = values.size();
				valuesByField.put(field.key(), values);
			}
	
			for(int i = 0; i < numValues; i++) {
				Map<String, String> subRow = new HashMap<String, String>();
				for(String key : valuesByField.keySet()) {
					List<String> values = valuesByField.get(key);
					if(values.size() > i)
						subRow.put(key, values.get(i));
					else
						subRow.put(key, "");
				}
				subRows.add(subRow);
			}
		} finally {
			if(file != null) {
				session().closeFile(file);
			}
		}
		return subRows;
	}
	
	public void resolveAssociation(Object object, UniAssociation assoc) {
		UniEntity entity = session().model().entityForClass(object.getClass());
		try {
			List<Map<String, String>> rows = this.loadAssociation(object, entity, assoc);
			entity.initAssocation(object, rows, assoc);
		} catch (Exception e) {
			UniLogger.universe.error("UniQuery failed to resolve association '" + assoc.name() + "'", e);
		}
		
	}
		
	public List loadObjectsInAssociation(Object object, UniAssociation assoc) {
		UniLogger.universe_test.debug("Loading stored values of association " + assoc.name());
		List objects = new ArrayList();
		UniEntity entity = assoc.entity();
		try {
			List<Map<String, String>> rows = this.loadAssociation(object, entity, assoc);
			List values = entity.resolveAssocation(object, rows, assoc);
			if(values != null)
				objects.addAll(values);
		} catch (Exception e) {
			UniLogger.universe.error("UniQuery failed to load objects in association '" + assoc.name() + "'", e);
		}	
		return objects;
	}

	public <T> List<T> executeQuery(UniQuerySpecification spec, UniContext uniContext) {
		UniObjectsSession.Select select = new UniObjectsSession.Select(spec, 0);
		List<Map<String, Object>> rows = null;
		try {
			rows = this.executeQuery(select, spec.entity());
		} catch (Exception e) {
			UniLogger.universe.error("UniQuery failed to fetch", e);
		}
		List list = new ArrayList();
		if(rows != null) {
			UniEntity entity = session().model().entityForClass(spec.entityClass());
			for(Map<String, Object> row : rows) {
				Object object = null;
				try {
					object = entity.entityClass().newInstance();
					entity.initObject(object, row, uniContext);
					list.add(object);
				} catch (Exception e) {
					UniLogger.universe.error("UniQuery: error while initializing object of '" + entity.entityClass().getName() + "' from database row of table '" + entity.entityName() + "'", e);
				}
			}
		}
		
		return list;
		
	}
	
	private static boolean _isDigit(char ch) {
		return (ch >= '0' && ch <= '9') || ch == '-' || ch == '.';
	}
	
	private static String[] _values(String line, int numValues) {
		int index = 0;
		String[] values = new String[numValues];
		StringBuffer buffer = new StringBuffer();
		boolean begin = true;
		for(int i = 0; i < line.length(); i++) {
			char ch = line.charAt(i);
			if(_isDigit(ch)) {
				if(!begin)
					begin = true;
				if(begin)
					buffer.append(ch);
			} else {
				if(begin) {
					begin = false;
					values[index++] = buffer.toString();
					buffer.setLength(0);
					if(index == numValues)
						break;
				}
			}
			if(i == line.length() - 1) {
				if(_isDigit(ch) && begin) {
					values[index++] = buffer.toString();
					buffer.setLength(0);
					if(index == numValues)
						break;
				}
			}
		}
		return values;
	}

	private static int _numberLine(String[] lines) {
		for(int i = lines.length - 1; i >= 0; i--) {
			if(lines[i].startsWith("=="))
				return i+1;
		}
		return -1;
	}
	
	// sum, min, max, avg
	public Map<String, Number> executeAggregateFunctions(String key, UniQuerySpecification spec, UniContext uniContext) throws Exception {
		UniObjectsSession.AggregateFunctions functions = new UniObjectsSession.AggregateFunctions(key, spec);
		UniCommand command = null;
		Map<String, Number> values = new HashMap<String, Number>();
		try {
			session().establishConnection();
			UniCommandGeneration generator = new UniCommandGeneration(session());
			command = functions.uniCommand(generator);
			String response = session().executeUniCommand(command);
			String lines[] = response.split("\n");
			int numberLine = _numberLine(lines);
			if(numberLine != -1) {
				String valueLine = lines[numberLine].trim();
				String columns[] = _values(valueLine, 4);
				values.put("SUM", new Double(columns[0]));
				values.put("MAX", new Double(columns[1]));
				values.put("MIN", new Double(columns[2]));
				values.put("AVG", new Double(columns[3]));
			}
		} finally {
			if(command != null)
				command.cancel();
		}
		return values;
	}
	
	public Number executeAggregateFunction(String key, String funcName, UniQuerySpecification spec, UniContext uniContext) throws Exception {
		UniObjectsSession.AggregateFunction function = new UniObjectsSession.AggregateFunction(key, funcName, spec);
		UniCommand command = null;
		Number value = null;
		try {
			session().establishConnection();
			UniCommandGeneration generator = new UniCommandGeneration(session());
			command = function.uniCommand(generator);
			String response = session().executeUniCommand(command);
			String lines[] = response.split("\n");
			String line6 = lines[5].trim();
			//String columns[] = values(line6, 1);
			UniEntity entity = spec.entity();
			UniField field = entity.fieldNamed(key);
			try {
				Class<?> valueClass = field.valueClass();
				if(Number.class.isAssignableFrom(valueClass)) {
					Constructor<Number> constructor = (Constructor<Number>) valueClass.getConstructor(String.class);
					value = constructor.newInstance(line6);
				}
			} catch (Exception ignore) {}
		} finally {
			if(command != null)
				command.cancel();
		}
		return value;
	}
	
	// count
	public int executeCountFunction(UniQuerySpecification spec, UniContext uniContext) throws Exception {
		UniObjectsSession.Count function = new UniObjectsSession.Count(spec);
		UniCommand command = null;
		int count = -1;
		try {
			session().establishConnection();
			UniCommandGeneration generator = new UniCommandGeneration(session());
			command = function.uniCommand(generator);
			String response = session().executeUniCommand(command);
			String columns[] = response.trim().split(" ");
			try {
				Integer value = new Integer(columns[0]);
				count = value.intValue();
			} catch (Exception ignore) {}
		} finally {
			if(command != null)
				command.cancel();
		}
		return count;
	}
	

	public <T> T  findOne(UniQuerySpecification spec, UniContext uniContext) {
		List<T> objects = this.executeQuery(spec, uniContext);
		if(objects.size() > 0)
			return objects.get(0);
		return null;
	}
	
	public <T> T  findOne(Class<T> entityClass, Map<String, Object> fieldValues, UniContext uniContext) {
		UniEntity entity = session().model().entityForClass(entityClass);
		UniQuerySpecification spec = new UniQuerySpecification(entity, UniPredicate.Util.createPredicateFromFieldValues(fieldValues));
		return findOne(spec, uniContext);
	}
	
	public <T> T  find(Class<T> entityClass, Object primaryKey, UniContext uniContext) {
		UniEntity entity = session().model().entityForClass(entityClass);
		UniField pkField = entity.primaryKeyField();
		if(pkField == null) {
			UniLogger.universe.error("UniQuery: no primary key field in entity '" + entity.entityName() + "'");
			return null;
		}
		Map<String, Object> fieldValues = new HashMap<String, Object>();
		fieldValues.put(pkField.key(), primaryKey);
		return findOne(entityClass, fieldValues, uniContext);
	}

}
