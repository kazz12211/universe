package universe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import universe.object.UniFaultingList;
import universe.util.UniLogger;
import ariba.util.fieldvalue.FieldValue;
import asjava.uniclientlibs.UniDynArray;
import asjava.uniobjects.UniFile;

public abstract class UniDatabaseOperation {

	Object object;
	UniEntity entity;
	long millis;

	protected UniDatabaseOperation(Object object, UniEntity entity) {
		this.object = object;
		this.entity = entity;
		this.millis = System.currentTimeMillis();
	}
	
	public long timestamp() {
		return millis;
	}
	public Object object() {
		return object;
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + " {object=" + object + "; entity=" + entity.entityName() + "; timestamp=" + new Date(millis) + "}";
	}
	
	public abstract void executeInContext(UniContext uniContext) throws Exception;
	
	public UniUpdater getProcessorInContext(UniContext uniContext) {
		UniUpdater updater = uniContext.updaterForEntity(entity);
		return updater;
	}
	
	public static List<UniDatabaseOperation> sortedOperations(List<UniDatabaseOperation> operations) {
		int size = operations.size();
		List<UniDatabaseOperation> copy = new ArrayList<UniDatabaseOperation>(size);
		for(int i = 0; i < size; i++) {
			copy.add(operations.get(i));
		}
		Collections.sort(copy, new Comparator<UniDatabaseOperation>() {
			@Override
			public int compare(UniDatabaseOperation arg0,
					UniDatabaseOperation arg1) {
				return arg0.millis - arg1.millis > 0 ? 1 : arg0.millis - arg1.millis < 0 ? -1 : 0;
			}});
		return copy;
	}
	
	protected void writeObject(Object object, Object pk, UniContext uniContext, boolean update) throws Exception {
		UniDynArray record = new UniDynArray();
		for(UniField field : entity.fields()) {
			if(field.isAssociated() || field.isReadOnly() || field.location() <= 0)	continue;
			if(field.isMultiValue()) {
				//UniDynArray array = new UniDynArray();
				Object list = FieldValue.getFieldValue(object, field.key());
				int location = 1;
				if(list instanceof Collection) {
					for(Object item : (Collection<?>)list) {
						//array.replace(location++, field.convertToString(item));
						_insertValue(record, field.location(), location++, field.convertToString(item));
					}
				} else {
					//array.append(field.convertToString(list));
					_insertValue(record, field.location(), location, field.convertToString(list));
				}
				//_insertValue(record, field.location(), array);
			} else {
				_insertValue(record, field.location(), field.convertToString(FieldValue.getFieldValue(object, field.key())));
			}
		}
		
		for(UniAssociation assoc : entity.associations()) {
			Object objects = FieldValue.getFieldValue(object, assoc.key());
			for(UniField field : assoc.fieldsInEntity(entity)) {
				//UniDynArray array = new UniDynArray();
				if(field.isMultiValue() || objects instanceof Collection) {
					int location = 1;
					for(Object item : (Collection<?>)objects) {
						UniLogger.universe_test.debug("*** Adding value '" + FieldValue.getFieldValue(item, field.key()) + "' to field " + field.columnName());
						//array.replace(location++, field.convertToString(FieldValue.getFieldValue(item, field.key())));
						_insertValue(record, field.location(), location++, field.convertToString(FieldValue.getFieldValue(item, field.key())));
					}
					//_insertValue(record, field.location(), array);
				} else {
					_insertValue(record, field.location(), field.convertToString(FieldValue.getFieldValue(objects, field.key())));
				}
			}
		}
		
		UniUpdater updater = this.getProcessorInContext(uniContext);			
		UniFile file = updater.session().getFile(entity.filename());
		if(file == null)
			throw new IllegalStateException("UniDatabaseOperation.Insert: could not get UniFile named '" + entity.filename() + "'");
		UniLogger.universe_test.debug("*** Writing record(" + pk + "): " + record);
		file.write(pk, record);
		updater.session().commandHistory().record((update ? "Update " : "Insert ") + entity.entityName() + "(recordId=" + pk + ") values: " + record.toString());
	}

	private void _insertValue(UniDynArray record, int location, Object value) {
		UniLogger.universe_test.debug("*** _insertValue: " + value + " at " + location);
		record.replace(location, value);
	}
	private void _insertValue(UniDynArray record, int location, int valueLocation, Object value) {
		UniLogger.universe_test.debug("*** _insertValue: " + value + " at (" + location + ", " + valueLocation + ")");
		record.replace(location, valueLocation, value);
	}
	
	public interface DatabaseOperationCallback {
		public void didInsert(Object object);
		public void didFetch(Object object);
		public void didUpdate(Object object, UniEntity entity);
		public void willUpdate(Object object, UniEntity entity);
		public void didDelete(Object object, UniEntity entity);
		public void willDelete(Object object, UniEntity entity);
		public boolean shouldDelete(Object object, UniEntity entity);
	}
	
	public static class Update extends UniDatabaseOperation {

		public Update(Object object, UniEntity entity) {
			super(object, entity);
		}

		@Override
		public void executeInContext(UniContext uniContext) throws Exception {
			Object pk = entity.primaryKeyForObject(object);
						
			uniContext.willUpdate(object, entity);
			
			this.writeObject(object, pk, uniContext, true);
			
			uniContext.didUpdate(object, entity);
		}
		

	}

	public static class Delete extends UniDatabaseOperation {

		public Delete(Object object, UniEntity entity) {
			super(object, entity);
		}

		@Override
		public void executeInContext(UniContext uniContext) throws Exception {
			if(!uniContext.shouldDelete(object, entity))
				return;
			
			uniContext.willDelete(object, entity);
			
			
			// delete things
			
			uniContext.didDelete(object, entity);
		}

	}

	public static class Insert extends UniDatabaseOperation {

		public Insert(Object object, UniEntity entity) {
			super(object, entity);
		}

		@Override
		public void executeInContext(UniContext uniContext) throws Exception {
			Object pk = entity.primaryKeyForObject(object);
			if(pk == null) {
				pk = entity.pkGenerator().newPrimaryKeyForObject(object, uniContext);
				entity.setPrimaryKeyForObject(pk, object);
			}
			
			if(pk == null) {
				UniLogger.universe.error("No primary key for object of entity '" + entity.entityName() + "'");
				return;
			}
						
			this.writeObject(object, pk, uniContext, false);
			
			uniContext.didInsert(object);
		}
		

	}

}
