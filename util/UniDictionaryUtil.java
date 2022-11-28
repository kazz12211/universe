package universe.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import universe.UniFieldDefinition;
import universe.UniObjectsSession;
import util.CharacterSet;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniDictionary;
import asjava.uniobjects.UniSelectList;
import asjava.uniobjects.UniSession;

public class UniDictionaryUtil {
	
	public static List<Map<String, Object>> uniDictionaryToDefinitions(UniDictionary dict, UniObjectsSession session) {
		List<Map<String, Object>> definitions = new ArrayList<Map<String, Object>>();
		try {
			UniSelectList selectList = session.uniSession().selectList(0);
			selectList.select(dict);
			for(UniString recordId = selectList.next(); !selectList.isLastRecordRead(); recordId = selectList.next()) {
				Map<String, Object> def = new HashMap<String, Object>();
				def.put("fieldName", recordId.toString());
				String str = dict.getLoc(recordId).toString();
				def.put("loc", str);
				if(str != null && str.length() > 0) {
					try {
						def.put("location", new Integer(str));
					} catch (Exception ignore) {}
				}
				def.put("type", dict.getType(recordId).toString());
				def.put("displayName", dict.getName(recordId).toString());
				def.put("format", dict.getFormat().toString());
				def.put("conversion", dict.getConv(recordId).toString());
				str = dict.getSM(recordId).toString();
				def.put("sm", str);
				if("M".equalsIgnoreCase(str))
					def.put("multiValue", new Boolean(true));
				else
					def.put("multiValue", new Boolean(false));
				def.put("assoc", dict.getAssoc(recordId).toString());
				def.put("sqlType", dict.getSQLType(recordId).toString());
				definitions.add(def);
			}
		} catch (Exception e) {
			UniLogger.universe.error("UniDictionaryUtil: Could not load UniDictionary", e);
		}
		return definitions;
	}

	public static List<UniFieldDefinition> uniDictionaryToUniFieldDefinitions(UniDictionary dict, UniObjectsSession session) {
		
		List<Map<String, Object>> definitions = uniDictionaryToDefinitions(dict, session);
		List<UniFieldDefinition> fieldDefinitions = new ArrayList<UniFieldDefinition>();
		for(Map<String, Object> def : definitions) {
			UniFieldDefinition fieldDef = new UniFieldDefinition(def);
			fieldDefinitions.add(fieldDef);
		}
		return fieldDefinitions;
	}
	
	public static List<UniFieldDefinition> uniFieldDefinitions(String dictName, UniObjectsSession session) {
		UniDictionary dict = session.getDict(dictName);
		return uniDictionaryToUniFieldDefinitions(dict, session);
	}
}
