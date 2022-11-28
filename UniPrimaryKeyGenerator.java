package universe;

import java.util.Map;

import universe.util.UniFileUtil;
import universe.util.UniLogger;
import asjava.uniclientlibs.UniDataSet;
import asjava.uniclientlibs.UniRecord;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import asjava.uniobjects.UniObjectsTokens;

public abstract class UniPrimaryKeyGenerator {

	Map<String, Object> config;

	public Object newPrimaryKeyForObject(Object object, UniContext uniContext) {
		UniEntity entity = uniContext.entityForObject(object);
		if(entity == null)
			return null;
		return newPrimaryKeyForEntity(entity, uniContext);
	}
	
	public abstract Object newPrimaryKeyForEntity(UniEntity entity, UniContext uniContext);
	
	public void setConfig(Map<String, Object> dict) {
		this.config = dict;
	}
	public Map<String, Object> config() {
		return config;
	}
	
	public static class SequenceTableKeyGenerator extends UniPrimaryKeyGenerator {

		public static final String PK_TABLE_NAME = "_SEQUENCE_TABLE";

		@Override
		public Object newPrimaryKeyForEntity(UniEntity entity,
				UniContext uniContext) {
			UniQuery query = uniContext.processorForEntity(entity);
			UniObjectsSession session = query.session();
			UniFile file = null;
			Object pk = null;
			try {
				if(!UniFileUtil.exists(PK_TABLE_NAME, session.uniSession()))
					createPkTable(session);
				file = session.getFile(PK_TABLE_NAME);
				pk = getNextKey(entity, file);
			} catch (Exception e) {
				UniLogger.universe.error("SequenceTableKeyGenerator: Could not generate primary key for entity '" + entity.entityName() + "'");
				return null;
			} finally {
				if(file != null)
					session.closeFile(file);
			}
			return pk;
		}

		private void createPkTable(UniObjectsSession session) throws Exception {
			session.executeUniCommand("CREATE.FILE " + PK_TABLE_NAME + " 2 1 1");
			session.executeUniCommand(("REVISE DICT " + PK_TABLE_NAME), 
					"SEQUENCE", "D", "1", "", "SEQUENCE", "16R", "S","", "", "",
					"");
		}
		
		private Long getNextKey(UniEntity entity, UniFile file) throws UniFileException {
			UniDataSet rowSet = new UniDataSet();
			rowSet.append(entity.entityName());
			long pkValue = 0;
			Long pk = null;
			try {
				file.lockRecord(rowSet, UniObjectsTokens.UVT_WRITE_RELEASE);
				UniDataSet dataSet = file.readField(rowSet, 1);
				if(dataSet == null) {
					UniRecord record = new UniRecord();
					record.setRecordID(entity.entityName);
					record.setRecord(Long.toString(++pkValue));
					dataSet = new UniDataSet(record);
				} else {
					UniRecord record = dataSet.getUniRecord();
					String pkValueStr = record.getRecord().toString();
					if(pkValueStr != null && pkValueStr.length() > 0)
						pkValue = Long.parseLong(pkValueStr) + 1;
					else
						pkValue++;
					record.setRecord(Long.toString(pkValue));
				}
				file.writeField(dataSet, 1);
				pk = new Long(pkValue);
			} finally {
				if(file.isRecordLocked(entity.entityName()))
					file.unlockRecord(rowSet);
			}
			
			return pk;
		}
		
	}

}
