package universe;

import java.util.ArrayList;
import java.util.List;

import universe.util.UniLogger;
import asjava.uniclientlibs.UniException;
import asjava.uniobjects.UniTransaction;


public class UniUpdater extends UniDatabaseAccess {

	private boolean isActive = false;
	private List<UniDatabaseOperation> inserts = new ArrayList<UniDatabaseOperation>();
	private List<UniDatabaseOperation> updates = new ArrayList<UniDatabaseOperation>();
	private List<UniDatabaseOperation> deletes = new ArrayList<UniDatabaseOperation>();
	private boolean hasChanges = false;
	private UniTransaction tx;
	
	public UniUpdater(UniObjectsSession session) {
		super(session);
	}
		
	public void begin() throws Exception {
		session().establishConnection();
		if(!isActive) {
			/*
			tx = session().uniSession().transaction();
			if(!tx.isActive())
				tx.begin();
			isActive = tx.isActive();
			*/
			isActive = true;
		}
	}

	public void commit() throws Exception {
		if(isActive) {
			/*
			if(isActive && tx != null) {
				tx.commit();
			}
			tx = null;
			*/
			isActive = false;
		}
		clear();
	}

	public void rollback() {
		if(isActive) {
			/*
			try {
				if(isActive && tx != null)
					tx.rollback();
			} catch (UniException e) {
				UniLogger.universe.warn("UniUpdater failed to rollback", e);
			} finally {
				isActive = false;
				tx = null;
			}
			*/
			isActive = false;
		}
		clear();
	}

	private void clear() {
		inserts.clear();
		updates.clear();
		deletes.clear();
		hasChanges  = false;
	}
	
	public boolean hasChanges() {
		return hasChanges;
	}
	
	public void insert(Object object, UniContext uniContext) {
		UniEntity entity = uniContext.entityForObject(object);
		inserts.add(new UniDatabaseOperation.Insert(object, entity));
		hasChanges = true;
	}

	public void delete(Object object, UniContext uniContext) {
		UniEntity entity = uniContext.entityForObject(object);
		inserts.add(new UniDatabaseOperation.Delete(object, entity));
		hasChanges = true;
	}

	public void update(Object object, UniContext uniContext) {
		UniEntity entity = uniContext.entityForObject(object);
		inserts.add(new UniDatabaseOperation.Update(object, entity));
		hasChanges = true;
	}

	public void executeInserts(UniContext uniContext) throws Exception {
		for(UniDatabaseOperation insert : inserts) {
			try {
				insert.executeInContext(uniContext);
			} catch (Exception e) {
				throw new IllegalStateException(insert.toString(), e);
			}
		}
	}

	public void executeUpdates(UniContext uniContext) throws Exception {
		for(UniDatabaseOperation update : updates) {
			try {
				update.executeInContext(uniContext);
			} catch (Exception e) {
				throw new IllegalStateException(update.toString(), e);
			}
		}
	}

	public void executeDeletes(UniContext uniContext) throws Exception {
		for(UniDatabaseOperation delete : deletes) {
			try {
				delete.executeInContext(uniContext);
			} catch (Exception e) {
				throw new IllegalStateException(delete.toString(), e);
			}
		}
	}


}
