package universe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import universe.util.UniLogger;
import util.Accessor;
import util.CharacterSet;

public class UniField {

	public static Class<?> AffordableValueClasses[] = {
		java.lang.String.class,
		java.lang.Integer.class,
		java.lang.Long.class,
		java.lang.Short.class,
		java.lang.Float.class,
		java.lang.Double.class,
		java.lang.Boolean.class,
		java.util.Date.class,
		java.math.BigDecimal.class,
		java.math.BigInteger.class,
		java.util.List.class
	};
	
	public static String AffordableValueClassNames[] = {
		"String",
		"Integer",
		"Long",
		"Short",
		"Float",
		"Double",
		"Boolean",
		"Date",
		"BigDecimal",
		"BigInteger",
		"List"
	};
	
	protected static Class<?> DateClasses[] = {
		java.util.Date.class
	};
	protected static Class<?> NumberClasses[] = {
		Integer.class,
		java.lang.Long.class,
		java.lang.Short.class,
		java.lang.Float.class,
		java.lang.Double.class,
		java.lang.Boolean.class,
		java.math.BigDecimal.class,
		java.math.BigInteger.class
	};
	protected static Class<?> StringClasses[] = {
		java.lang.String.class,
	};

	String columnName;
	String key;
	boolean isPrimaryKey;
	Class<?> valueClass;
	UniEntity entity;
	String dateFormat;
	boolean isMultiValue;
	private DateFormat _dateFormat;
	String associationName;
	boolean isReadOnly;
	boolean lockKey;
	int location = -1;
	
	public UniField(UniEntity entity, String columnName, String key) {
		this.entity = entity;
		this.columnName = columnName;
		this.key = key;
		this.isPrimaryKey = false;
	}
	public UniField(UniEntity entity, String columnName, String key, Class<?> valueClass, boolean isPK) {
		this.entity = entity;
		this.columnName = columnName;
		this.key = key;
		this.valueClass = valueClass;
		this.isPrimaryKey = isPK;
	}
	public UniField(UniEntity entity, String columnName, String key, String dateFormat) {
		this.entity = entity;
		this.columnName = columnName;
		this.key = key;
		this.dateFormat = dateFormat;
		this._dateFormat = new SimpleDateFormat(dateFormat);
	}
	
	public String columnName() {
		return columnName;
	}
	public String key() {
		return key;
	}
	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}
	public Class<?> valueClass() {
		return valueClass;
	}
	
	public DateFormat dateFormat() {
		if(dateFormat == null)
			return null;
		if(_dateFormat == null)
			_dateFormat = new SimpleDateFormat(dateFormat);
		return _dateFormat;
	}
	public boolean isMultiValue() {
		return isMultiValue;
	}
	
	public String associationName() {
		return associationName;
	}
	
	public boolean isReadOnly() {
		return (isReadOnly || location() == -1);
	}
	
	public int location() {
		if(location == -1) {
			UniFieldDefinition def = entity.fieldDefinitionForField(this);
			if(def != null)
				location = def.location();
		}
		return location;
	}
	
	private Number coerceToNumber(Object value) {
		if(value == null)
			return null;
	
		String str = value.toString();
		
		if(str == null || str.length() == 0)
			return null;
		
		Number result = null;
		try {
			if(valueClass.equals(Integer.class))
				result = new Integer(str);
			if(valueClass.equals(Short.class))
				result = new Short(str);
			if(valueClass.equals(Long.class))
				result = new Long(str);
			if(valueClass.equals(Float.class))
				result = new Float(str);
			if(valueClass.equals(Double.class))
				result = new Double(str);
			if(valueClass.equals(BigDecimal.class))
				result = new BigDecimal(str);
			if(valueClass.equals(BigInteger.class))
				result = new BigInteger(str);
		} catch (Exception e) {
			UniLogger.universe.warn("coerceToNumber(): The value " + value + " seems not to be a number");
		}
		return result;
	}
	
	public Object coerceValue(Object value) {
		if(value == null)
			return null;
		if(valueClass == null)
			return value.toString();
		
		if(value instanceof Collection) {
			List<Object> values = new ArrayList<Object>();
			for(Object obj : ((Collection)value)) {
				values.add(this.coerceValue(obj));
			}
			return values;
		}
		
		if(valueClass.isAssignableFrom(value.getClass()))
			return value;
		
		if(valueClass.equals(java.lang.String.class))
			return value.toString();
		
		if(valueClass.equals(java.lang.Integer.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new Integer(((Number) value).intValue());
		}
		if(valueClass.equals(java.lang.Short.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new Short(((Number) value).shortValue());
		}
		if(valueClass.equals(java.lang.Long.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new Long(((Number) value).longValue());
		}
		if(valueClass.equals(java.lang.Float.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new Float(((Number) value).floatValue());
		}
		if(valueClass.equals(java.lang.Double.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new Double(((Number) value).doubleValue());
				
		}
		if(valueClass.equals(java.math.BigInteger.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new BigInteger(value.toString());
		}
		if(valueClass.equals(java.math.BigDecimal.class)) {
			if(!(value instanceof Number))
				return coerceToNumber(value);
			else
				return new BigDecimal(value.toString());
		}
		if(valueClass.equals(java.util.Date.class)) {
			if(value == null || value.toString().length() == 0)
				return null;
			try {
				return this.dateFormat().parseObject(value.toString());
			} catch (ParseException e) {
				UniLogger.universe.warn("UniField coerceValue() could not parse date string with format '" + dateFormat + "'", e);
				return null;
			}
		}
		if(valueClass.equals(java.lang.Boolean.class)) {
			if(!(value instanceof Boolean))
				return new Boolean(((Boolean) value).booleanValue());
			else {
				String s = value.toString();
				if("true".equalsIgnoreCase(s) || "yes".equalsIgnoreCase(s) || "1".equals(s))
					return new Boolean(true);
				else
					return new Boolean(false);
			}
		}
		return value;

	}
	
	public UniEntity entity() {
		return entity;
	}
	
	protected Class<?> valueClassFromEntity() {
		Class<?> entitClass = entity.entityClass();
		Accessor getter = Accessor.newGetAccessor(entitClass, key);
		return getter.getReturnType();
	}
	
	@Override
	public String toString() {
		return "field {columnName=" + columnName + "; key=" + key + "; isPrimaryKey=" + isPrimaryKey + "; valueClass=" + valueClass.getName() + "; dateFormat=" + dateFormat + "; isMultiValue=" + isMultiValue + "; location=" + location() + "; lock=" + lockKey + "; isReadOnly:" +isReadOnly + "}";
	}
		
	public boolean isAssociated() {
		return (associationName != null && associationName.length() > 0);
	}
	public boolean isDate() {
		for(int i = 0; i < DateClasses.length; i++)
			if(valueClass.equals(DateClasses[i]))
				return true;
		return false;
	}
	
	public boolean isNumber() {
		for(int i = 0; i < NumberClasses.length; i++) {
			if(valueClass.equals(NumberClasses[i]))
				return true;
		}
		return false;
	}
	public String convertToString(Object value) {
		if(value == null)
			return "";
		return value.toString();
	}
}
