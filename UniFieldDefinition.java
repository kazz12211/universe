package universe;

import java.util.Map;

public class UniFieldDefinition {

	private Map<String, Object> _uniDictionaryMap;
	private int location = -1;
	private boolean isMultiValue = false;
	private boolean isAssociation = false;
	
	public UniFieldDefinition(Map<String, Object> def) {
		this._uniDictionaryMap = def;
		if(loc() != null && loc().length() > 0) {
			try {
				location = Integer.parseInt(loc());
			} catch (Exception ignore) {}
		}
		if(sm() != null && sm().equals("M"))
			isMultiValue = true;
		if(assoc() != null && assoc().length() > 0)
			isAssociation = true;
	}
	
	public String fieldName() {
		return (String) this._uniDictionaryMap.get("fieldName");
	}
	public String loc() {
		return (String) this._uniDictionaryMap.get("loc");
	}
	public int location() {
		return location;
	}
	public String type() {
		return (String) this._uniDictionaryMap.get("type");
	}
	public String displayName() {
		return (String) this._uniDictionaryMap.get("displayName");
	}
	public String format() {
		return (String) this._uniDictionaryMap.get("format");
	}
	public String conversion() {
		return (String) this._uniDictionaryMap.get("conversion");
	}
	public String sm() {
		return (String) this._uniDictionaryMap.get("sm");
	}
	public boolean isMultiValue() {
		return isMultiValue;
	}
	public String assoc() {
		return (String) this._uniDictionaryMap.get("assoc");
	}
	public boolean isAssociation() {
		return isAssociation;
	}
	public String sqlType() {
		return (String) this._uniDictionaryMap.get("sqlType");
	}
}
