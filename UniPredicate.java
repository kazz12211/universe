package universe;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ariba.util.core.ListUtil;

public interface UniPredicate {

	public enum Operator {
		Eq, Neq, Gt, Gte, Lt, Lte, Like, IsNull, IsNotNull, StartsWith, EndsWith, Contains
	};
	public static String[] UniOps = {
		"EQ",
		"NE",
		"GT",
		"GE",
		"LT",
		"LE",
		"LIKE",
		"IS.NULL",
		"IS.NOT.NULL",
		"LIKE",
		"LIKE",
		"LIKE"
	};
	
	public abstract String generateString(UniEntity entity);
	
	public class KeyValue implements UniPredicate {
		String _key;
		Object _value;
		Operator _operator;
		
		
		public KeyValue(String key, Object value) {
			this(key, value, Operator.Eq);
		}
		
		public KeyValue(String key, Object value, Operator eq) {
			_key = key;
			_value = value;
			_operator = eq;
		}

		public String key() {
			return _key;
		}
		public Object value() {
			return _value;
		}
		public Operator operator() {
			return _operator;
		}
		
		@Override
		public String generateString(UniEntity entity) {
			UniField field = entity.fieldNamed(_key);
			String opString = operatorString(field);
			if(_operator == Operator.IsNotNull || _operator == Operator.IsNull)
				return field.columnName + " " + opString;
			String opcString = valueString(entity, field);
			return field.columnName() + " " + opString + " " + opcString;
		}

		private String operatorString(UniField field) {
			return UniOps[_operator.ordinal()];
		}
		
		private String valueString(UniEntity entity, UniField field) {
			if(_value == null)
				return "NULL";
			if(_value instanceof String) {
				if(_operator == Operator.Contains)
					return "\"..." + _value.toString() + "...\"";
				if(_operator == Operator.StartsWith)
					return "\"" + _value.toString() + "...\"";
				if(_operator == Operator.EndsWith)
					return "\"..." + _value.toString() + "\"";
				return "\"" + _value.toString() + "\"";
			}	
			if(_value instanceof Number)
				return _value.toString();
			if(_value instanceof Boolean)
				return ((Boolean) _value).booleanValue() ? "TRUE" : "FALSE";
			if(_value instanceof Date) {
				DateFormat formatter = entity.fieldNamed(_key).dateFormat();
				if(formatter == null)
					return _value.toString();
				return "\"" + formatter.format((Date) _value) + "\"";
			}
			return _value.toString();
		}
	}
	
	public class KeyKey implements UniPredicate {
		String _leftKey;
		String _rightKey;
		Operator _operator;

		public KeyKey(String leftKey, String rightKey) {
			this(leftKey, rightKey, Operator.Eq);
		}
		
		public KeyKey(String leftKey, String rightKey, Operator operator) {
			_leftKey = leftKey;
			_rightKey = rightKey;
			_operator = operator;
		}
		
		public String leftKey() {
			return _leftKey;
		}
		
		public String rightKey() {
			return _rightKey;
		}
		
		public Operator operator() {
			return _operator;
		}

		private String operatorString() {
			return UniOps[_operator.ordinal()];
		}

		@Override
		public String generateString(UniEntity entity) {
			UniField leftField = entity.fieldNamed(_leftKey);
			UniField rightField = entity.fieldNamed(_rightKey);
			String opString = operatorString();
			return leftField.columnName() + " " + opString + " " + rightField.columnName();
		}

	}
	
	public abstract class Junction implements UniPredicate {
		List<UniPredicate> _predicates;
		
		public Junction(List<UniPredicate> predicates) {
			this._predicates = predicates;
		}
		
		protected Junction(UniPredicate[] predicates) {
			this._predicates = Arrays.asList(predicates);
		}
		
		protected List<String> listGeneratedStrings(UniEntity entity) {
			List<String> list = new ArrayList<String>();
			for(UniPredicate predicate : _predicates) {
				list.add(predicate.generateString(entity));
			}
			return list;
		}
		
		public List<UniPredicate> predicates() {
			return _predicates;
		}
	}
	
	public class And extends Junction {

		public And(List<UniPredicate> predicates) {
			super(predicates);
		}
		public And(UniPredicate[] predicates) {
			super(predicates);
		}
		
		@Override
		public String generateString(UniEntity entity) {
			List<String> strings = listGeneratedStrings(entity);
			if(strings.size() == 0)
				return null;
			if(strings.size() == 1)
				return strings.get(0);
			String junction = ListUtil.listToString(strings, " AND ");
			return "(" + junction + ")";
		}
		
	}
	
	public class Not implements UniPredicate {
		UniPredicate _predicate;
		
		public Not(UniPredicate predicate) {
			_predicate = predicate;
		}
		
		public UniPredicate predicate() {
			return _predicate;
		}

		@Override
		public String generateString(UniEntity entity) {
			return "NOT " + _predicate.generateString(entity);
		}
	}

	public class Or extends Junction {

		public Or(List<UniPredicate> predicates) {
			super(predicates);
		}
		public Or(UniPredicate[] predicates) {
			super(predicates);
		}
		
		@Override
		public String generateString(UniEntity entity) {
			List<String> strings = listGeneratedStrings(entity);
			if(strings.size() == 0)
				return null;
			if(strings.size() == 1)
				return strings.get(0);
			String junction = ListUtil.listToString(strings, " OR ");
			return "(" + junction + ")";
		}
	}
		
	public static class Util {

		public static UniPredicate createPredicateFromFieldValues( Map<String, Object> fieldValues) {
			if(fieldValues.size() == 0)
				return null;
			if(fieldValues.size() == 1) {
				Map.Entry<String, Object> entry = fieldValues.entrySet().iterator().next();
				return new UniPredicate.KeyValue(entry.getKey(), entry.getValue());
			} else {
				List<UniPredicate> predicates = new ArrayList<UniPredicate>();
				for(Map.Entry<String, Object> entry : fieldValues.entrySet()) {
					predicates.add(new UniPredicate.KeyValue(entry.getKey(), entry.getValue()));
				}
				return new UniPredicate.And(predicates);
			}
		}
		
	}

}
