package universe;

import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import ariba.ui.widgets.XMLUtil;
import ariba.util.core.ClassUtil;
import universe.util.UniLogger;
import util.CharacterSet;
import util.XMLConfig;

public class UniModelGroup {

	private static UniModelGroup _defaultModelGroup = null;
	Map<String, UniModel> models = new HashMap<String, UniModel>();

	public static UniModelGroup defaultModelGroup() {
		if(_defaultModelGroup == null) {
			_defaultModelGroup = new UniModelGroup();
			_defaultModelGroup.initWithFile("UniModel.xml");
		}
		return _defaultModelGroup;
	}
	
	private UniModel _modelNamed(String modelName) {
		return models.get(modelName);
	}
	
	private Map<String, UniModel> _models() {
		return models;
	}
	
	private void _addModel(UniModel model) {
		models.put(model.name(), model);
	}
	
	public static UniModel modelNamed(String modelName) {
		return defaultModelGroup()._modelNamed(modelName);
	}
	
	public static Map<String, UniModel> models() {
		return defaultModelGroup()._models();
	}
	
	public static void addModel(UniModel model) {
		defaultModelGroup()._addModel(model);
	}
	
	private void initWithFile(String fileName) {
		Parser parser = new Parser();
		try {
			parser.parse(fileName);
		} catch (Exception e) {
			UniLogger.universe.error("UniModelGroup : could not parse model file '" + fileName + "'", e);
		}
	}
	
	protected Iterator<UniModel> modelIterator() {
		return models.values().iterator();
	}
	
	public UniEntity entityNamed(String entityName) {
		UniEntity entity = null;
		for(Iterator<UniModel> iter = modelIterator(); iter.hasNext(); ) {
			UniModel model = iter.next();
			entity = model.entityNamed(entityName);
			if(entity != null)
				return entity;
		}
		return null;
	}

	public UniEntity entityForClass(Class<?> aClass) {
		UniEntity entity = null;
		for(Iterator<UniModel> iter = modelIterator(); iter.hasNext(); ) {
			UniModel model = iter.next();
			entity = model.entityForClass(aClass);
			if(entity != null)
				return entity;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return models.toString();
	}
	
	static class Parser extends XMLConfig {
		
		public void parse(String filename) throws Exception {
			String path = super.getResourcePath(filename);
			UniLogger.universe.debug("UniModelGroup : loading models from '" + path + "'");
			URL url = new URL(path);
			Element docElement = XMLUtil.document(url, false, false, null).getDocumentElement();
			if("models".equals(docElement.getNodeName())) {
				Element[] modelElements = this.elementsNamed(docElement, "model");
				if(modelElements == null || modelElements.length == 0) {
					UniLogger.universe.warn("UniModelGroup : no model definitions in '" + path + "'");
				} else {
					for(Element element : modelElements) {
						UniModel model = new UniModel();
						initWithElement(model, element);
						if(model.name() != null) {
							UniLogger.universe_dev.debug("Adding model '" + model.name() + "'");
							UniModelGroup.addModel(model);
						}
					}
				}
			}
		}

		private void initWithElement(UniModel model, Element element) {
			String modelName = element.getAttribute("name");
			model.setName(modelName);
			Element[] entityElements = this.elementsNamed(element, "entity");
			if(entityElements == null || entityElements.length == 0) {
				UniLogger.universe.warn("UniModelGroup : no entity definitions in model '" + modelName + "'");
			} else {
				for(Element elem : entityElements) {
					parseEntity(model, elem);
				}
			}
			Element connElem = this.elementNamed(element, "connection");
			if(connElem != null) {
				paserConnectionInfo(model, connElem);
			} else {
				UniLogger.universe.warn("UniModelGroup : no connection info definitions in model '" + modelName + "'");
			}
		}

		private void parseEntity(UniModel model, Element elem) {
			String entityName = elem.getAttribute("name");
			String filename = elem.getAttribute("file");
			String entityClass = elem.getAttribute("class");
			String lockingStrategy = elem.getAttribute("lockingStrategy");
			UniEntity entity = new UniEntity(model, ClassUtil.classForName(entityClass), entityName, filename);
			
			List<UniField> fields = new ArrayList<UniField>();
			Element[] fieldElements = this.elementsNamed(elem, "field");
			if(fieldElements == null || fieldElements.length == 0) {
				UniLogger.universe.warn("UniModelGroup : no field definitions of entity '" + entityName + "' in model '" + model.name() + "'");
			} else {
				for(Element fieldElem : fieldElements) {
					UniField field = parseField(entity, fieldElem);
					if(field != null)
						fields.add(field);
				}
			}
			
			entity.fields = new UniField[fields.size()];
			for(int i = 0; i < fields.size(); i++) {
				UniField field = fields.get(i);
				entity.fields[i] = field;
				UniLogger.universe_dev.debug("Field added to entity'" + entity.entityName() + "' " + field.toString());
			}
			
			List<UniAssociation> assocs = new ArrayList<UniAssociation>();
			Element[] assocElements = this.elementsNamed(elem, "association");
			if(assocElements != null && assocElements.length > 0) {
				for(Element assocElem : assocElements) {
					UniAssociation assoc = parseAssociation(entity, assocElem);
					if(assoc != null)
						assocs.add(assoc);
					
				}
			}
			
			entity.associations = new UniAssociation[assocs.size()];
			//UniLogger.universe_dev.debug(assocs);
			for(int i = 0; i < assocs.size(); i++) {
				UniAssociation assoc = assocs.get(i);
				entity.associations[i] = assoc;
				UniLogger.universe_dev.debug("Field added to entity'" + entity.entityName() + "' " + assoc.toString());
			}
			
			Element pkGenElement = this.elementNamed(elem, "primary-key-generator");
			if(pkGenElement != null) {
				parsePkGenerator(entity, pkGenElement);
			} else {
				entity.setPkGenerator(new UniPrimaryKeyGenerator.SequenceTableKeyGenerator());
			}
			
			if(UniEntity.OptimisticLock.equals(lockingStrategy))
				entity.lockingStrategy = UniEntity.LockingStrategy.Optimistic;
			else if(UniEntity.PessimisticLock.equals(lockingStrategy))
				entity.lockingStrategy = UniEntity.LockingStrategy.Pessimistic;
			else if(UniEntity.FileLock.equals(lockingStrategy))
				entity.lockingStrategy = UniEntity.LockingStrategy.File;
			UniLogger.universe_dev.debug("Adding entity '" + entity.entityName() + "' to model '" + model.name() + "'");
			model.addEntity(entity.entityClass(), entity);
		}

	
		private void parsePkGenerator(UniEntity entity, Element element) {
			String className = element.getAttribute("class");
			if(nullOrEmpty(className))
				return;
			UniPrimaryKeyGenerator pkGenerator = (UniPrimaryKeyGenerator) ClassUtil.newInstance(className, UniPrimaryKeyGenerator.class.getName());
			if(pkGenerator == null) {
				UniLogger.universe.error("Could not instantiate primary key generator '" + className + "'");
				return;
			}
			DictionaryParser parser = new DictionaryParser();
			Map<String, Object> dict = null;
			try {
				dict = parser.parseDictionary(element);
			} catch (Exception e) {
				UniLogger.universe.warn("Could not read configuraion of primary key generator '" + className + "'");
			}
			pkGenerator.setConfig(dict);
			entity.setPkGenerator(pkGenerator);
		}

		private UniField parseField(UniEntity entity, Element fieldElem) {
			String columnName = fieldElem.getAttribute("name");
			String key = fieldElem.getAttribute("key");
			String valueClass = fieldElem.getAttribute("valueClass");
			String dateFormat = fieldElem.getAttribute("dateFormat");
			String primaryKey = fieldElem.getAttribute("primaryKey");
			String multiValue = fieldElem.getAttribute("multiValue");
			String associationName = fieldElem.getAttribute("association");
			String readOnly = fieldElem.getAttribute("readOnly");
			String lock = fieldElem.getAttribute("lock");
			String locationStr = fieldElem.getAttribute("location");
			
			boolean isPrimaryKey = (nullOrEmpty(primaryKey)) ? false : ("true".equalsIgnoreCase(primaryKey) ? true : false);
			boolean isMultiValue = (nullOrEmpty(multiValue)) ? false : ("true".equalsIgnoreCase(multiValue) ? true : false);
			boolean isReadOnly = (nullOrEmpty(readOnly)) ? false : ("true".equalsIgnoreCase(readOnly) ? true : false);
			boolean lockKey = (nullOrEmpty(lock)) ? false : ("true".equalsIgnoreCase(lock) ? true : false);
			
			if(nullOrEmpty(columnName)) {
				UniLogger.universe.warn("UniModelGroup : no column name in entity '" + entity.entityName() + "'");
				return null;
			}
			if(nullOrEmpty(key)) {
				UniLogger.universe.warn("UniModelGroup : no key in entity of column '" + columnName + "' in entity '" + entity.entityName() + "'");
			}
			if(nullOrEmpty(valueClass)) {
				UniLogger.universe.warn("UniModelGroup : no value class in entity of column '" + columnName + "' in entity '" + entity.entityName() + "'");
			}

			Class<?> validValueClass = validateValueClassName(valueClass);
			UniField field = new UniField(entity, columnName, key, validValueClass, isPrimaryKey);
			
			field.dateFormat = nullOrEmpty(dateFormat) ? null : dateFormat;
			field.isMultiValue = isMultiValue;
			field.associationName = nullOrEmpty(associationName) ? null : associationName;
			field.isReadOnly = isReadOnly;
			field.lockKey = lockKey;
			if(!nullOrEmpty(locationStr)) {
				int location = -1;
				try {
					location = Integer.parseInt(locationStr);
				} catch (NumberFormatException e) {
					UniLogger.universe.error("UniModelGroup : location attribute must be integer number of '" + columnName + "' in entity '" + entity.entityName() + "'");
				}
				field.location = location;
			}
						
			return field;
		}
		
		private UniFieldRef parseFieldRef(UniEntity entity, UniAssociation assoc, Element fieldElem) {
			String fieldName = fieldElem.getAttribute("name");
						
			if(nullOrEmpty(fieldName)) {
				UniLogger.universe.warn("UniModelGroup : no fieldName in field-ref '" + entity.entityName() + "'");
				return null;
			}

			UniFieldRef field = new UniFieldRef(entity, assoc, fieldName);
			
			return field;
		}



		private UniAssociation parseAssociation(UniEntity entity, Element assocElem) {
			String name = assocElem.getAttribute("name");
			String key = assocElem.getAttribute("key");
			String toMany = assocElem.getAttribute("toMany");
			String className = assocElem.getAttribute("class");
			String prefetch = assocElem.getAttribute("prefetch");
			boolean isToMany = (nullOrEmpty(toMany)) ? true : ("true".equalsIgnoreCase(toMany) ? true : false);
			boolean shouldPrefetch = (nullOrEmpty(prefetch)) ? false : ("true".equalsIgnoreCase(prefetch) ? true : false);
			
			if(nullOrEmpty(name)) {
				UniLogger.universe.warn("UniModelGroup : no name in association '" + entity.entityName() + "'");
				return null;
			}
			if(nullOrEmpty(key)) {
				UniLogger.universe.warn("UniModelGroup : no key in association '" + name + "' in entity '" + entity.entityName() + "'");
			}
			if(nullOrEmpty(className)) {
				UniLogger.universe.warn("UniModelGroup : no class in association '" + name + "' in entity '" + entity.entityName() + "'");
			}
			Class<?> assocClass = ClassUtil.classForName(className);
			if(assocClass == null) {
				UniLogger.universe.warn("UniModelGroup : invalid class in association '" + name + "' in entity '" + entity.entityName() + "'. class name is '" + className + "'");
			}
			
			UniAssociation assoc = new UniAssociation(entity, name, key, isToMany, assocClass);
			List<UniFieldRef> fields = new ArrayList<UniFieldRef>();
			Element[] fieldElements = this.elementsNamed(assocElem, "field-ref");
			if(fieldElements != null && fieldElements.length > 0) {
				for(Element fieldElem : fieldElements) {
					UniFieldRef field = this.parseFieldRef(entity, assoc, fieldElem);
					if(field != null) {
						fields.add(field);
					}
				}
			}
			assoc.fields = new UniFieldRef[fields.size()];
			for(int i = 0; i < fields.size(); i++) {
				UniFieldRef field = fields.get(i);
				assoc.fields[i] = field;
			}
			
			assoc.shouldPrefetch = shouldPrefetch;
			
			return assoc;
		}

		private Class<?> validateValueClassName(String valueClass) {
			for(int i = UniField.AffordableValueClassNames.length - 1; i >= 0; i--) {
				String className = UniField.AffordableValueClassNames[i];
				if(className.equalsIgnoreCase(valueClass))
					return UniField.AffordableValueClasses[i];
			}
			return java.lang.String.class;
		}

		private void paserConnectionInfo(UniModel model, Element connElem) {
			String host = connElem.getAttribute("host");
			String portNum = connElem.getAttribute("port");
			String username = connElem.getAttribute("username");
			String password = connElem.getAttribute("password");
			String accountPath = connElem.getAttribute("accountPath");
			if(!nullOrEmpty(host) && !nullOrEmpty(username) && !nullOrEmpty(password) && !nullOrEmpty(accountPath)) {
				UniConnectionInfo info = new UniConnectionInfo();
				info.setHost(host);
				info.setUsername(username);
				info.setPassword(password);
				info.setAccountPath(accountPath);
				if(nullOrEmpty(portNum)) {
					info.setPort(Integer.parseInt(portNum));
				}
				model.connectionInfo = info;
			}
		}
		
		private boolean nullOrEmpty(String str) {
			return (str == null || str.length() == 0);
		}

	}

	public static class DictionaryParser {
		
		public static final String DICT_TYPE = "dict";
		public static final String ARRAY_TYPE = "array";
		public static final String STRING_TYPE = "string";
		public static final String NUMBER_TYPE = "number";
		public static final String DATE_TYPE = "date";
		public static final String BOOLEAN_TYPE = "boolean";
		private static final String LONG_DATE_FORMAT_STRING = "yyyy/MM/dd HH:mm:SS";
		private static final String SHORT_DATE_FORMAT_STRING = "yyyy/MM/dd";
		private static final DateFormat LONG_DATE_FORMAT = new SimpleDateFormat(LONG_DATE_FORMAT_STRING);
		private static final DateFormat SHORT_DATE_FORMAT = new SimpleDateFormat(SHORT_DATE_FORMAT_STRING);

		public Map<String, Object> parseDictionary(Element parent) throws Exception {
			Map<String, Object> keyValues = new HashMap<String, Object>();
			
			Element elements[] = XMLUtil.getAllChildren(parent, null);
			int size = elements.length;
			for(int i = 0; i < size; ) {
				Element keyElement = elements[i];
				Element valueElement = elements[i+1];
				if(keyElement.getNodeName().equals("key") && isValidValueElement(valueElement)) {
					String key = XMLUtil.getText(keyElement, null);
					Object value = objectValue(valueElement);
					if(key != null && value != null) {
						keyValues.put(key, value);
					}
				}
				i += 2;
			}
			return keyValues;
		}
		

		private Object objectValue(Element element) throws Exception {
			String type = element.getNodeName();
			if(DICT_TYPE.equals(type)) {
				return parseDictionary(element);
			} else if(ARRAY_TYPE.equals(type)) {
				return parseArray(element);
			} else if(STRING_TYPE.equals(type)) {
				return XMLUtil.getText(element, null);
			} else if(NUMBER_TYPE.equals(type)) {
				return parseNumber(element);
			} else if(DATE_TYPE.equals(type)) {
				return parseDate(element);
			} else if(BOOLEAN_TYPE.equals(type)) {
				return parseBoolean(element);
			}
			return null;
		}

		private List<Object> parseArray(Element element) throws Exception {
			Element elements[] = XMLUtil.getAllChildren(element, null);
			List<Object> list = new ArrayList<Object>();
			int size = elements.length;
			for(int i = 0; i < size; i++) {
				Object value = this.objectValue(elements[i]);
				if(value != null) {
					list.add(value);
				}
			}
			return list;
		}
		
		private String numberString(String string) {
			StringBuffer b = new StringBuffer();
			for(int i = 0; i < string.length(); i++) {
				char ch = string.charAt(i);
				if(CharacterSet.numberCharacterSet.contains(ch))
					b.append(ch);
			}
			return b.toString();
		}
		
		private Double toDouble(String string, double defaultValue) {
			if(string == null)
				return new Double(defaultValue);
			String s = string.trim();
			boolean minus = false;
			if(s.charAt(0) == '-') {
				minus = true;
				s = s.substring(1);
			}
			String numberStr = numberString(s);
			if(numberStr.length() > 0) {
				double val = Double.parseDouble(numberStr);
				if(minus)
					return new Double(-val);
				else
					return new Double(val);
			}
			return new Double(defaultValue);
		}

		private Number parseNumber(Element element) {
			String value = XMLUtil.getText(element, null);
			if(value == null)
				return null;
			return toDouble(value, 0.0);
		}
		
		private Date parseDate(Element element) throws ParseException {
			String value = XMLUtil.getText(element, null);
			if(value == null) {
				return null;
			}
			Date date = null;
			if(value.length() == LONG_DATE_FORMAT_STRING.length())
				date = LONG_DATE_FORMAT.parse(value);
			else if(value.length() == SHORT_DATE_FORMAT_STRING.length())
				date = SHORT_DATE_FORMAT.parse(value);
			return date;
		}
		
		private Boolean parseBoolean(Element element) {
			String value = XMLUtil.getText(element, null);
			if(value == null) {
				return null;
			}
			if("true".equalsIgnoreCase(value) || "yes".equalsIgnoreCase(value))
				return new Boolean(true);
			else if("false".equalsIgnoreCase(value) || "no".equalsIgnoreCase(value))
				return new Boolean(false);
			return null;
		}

		private boolean isValidValueElement(Element element) {
			String type = element.getNodeName();
			if(DICT_TYPE.equals(type) || 
					ARRAY_TYPE.equals(type) || 
					STRING_TYPE.equals(type) || 
					NUMBER_TYPE.equals(type) || 
					DATE_TYPE.equals(type) || 
					BOOLEAN_TYPE.equals(type))
				return true;
			return false;
		}

	}
}
