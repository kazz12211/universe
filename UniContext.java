package universe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import universe.UniDatabaseOperation.DatabaseOperationCallback;
import universe.UniDatabaseOperation.Insert;
import universe.util.UniLogger;
import util.Delegate;
import util.MethodInvocation;
import util.Selector;
import ariba.util.core.Assert;
import ariba.util.core.ClassUtil;

public class UniContext implements DatabaseOperationCallback {

	static ThreadLocal<UniContext> threadLocal = new ThreadLocal<UniContext>();
	Map<UniModel, UniObjectsSession> uniObjectsSessions = new HashMap<UniModel, UniObjectsSession>();
	Map<UniModel, UniQuery> processors = new HashMap<UniModel, UniQuery>();
	Map<UniModel, UniUpdater> transactions = new HashMap<UniModel, UniUpdater>();
	UniEntityCache entityCache;
	
	protected UniContext() {
		Map<String, UniModel> models = UniModelGroup.models();
		for(UniModel model : models.values()) {
			uniObjectsSessions.put(model, new UniObjectsSession(model));
		}
		for(UniModel model : models.values()) {
			UniObjectsSession session = uniObjectsSessions.get(model);
			processors.put(model, new UniQuery(session));
			transactions.put(model,  new UniUpdater(session));
		}
		entityCache = new UniEntityCache();
	}
	
	public static void bind(UniContext context) {
		threadLocal.set(context);
	}
	
	public static void unbind() {
		threadLocal.set(null);
	}
	
	public static UniContext get() {
		UniContext context = threadLocal.get();
		Assert.that(context != null, "get(); on thread local with no UniContext bound");
		return context;
	}
	
	public static UniContext peek() {
		UniContext context = threadLocal.get();
		return context;
	}
	
	public static UniContext createContext() {
		return new UniContext();
	}
	
	public static void bindNewContext() {
		UniContext ctx = peek();
		if(ctx == null)
			bindContext(createContext());
	}
	
	public static void bindContext(UniContext context) {
		bind(context);
	}
	
	public <T> T create(String className) {
		return (T)create(ClassUtil.classForName(className));
	}

	public <T> T create(Class<T> aClass) {
		T o = (T)ClassUtil.newInstance(aClass);
		Assert.that(o != null,  "Unable to create instance of class: %s", aClass.getName());
		this.recordForInsert(o);
		return o;
	}
	
	public void recordForInsert(Object object) {
		UniEntity entity = entityForObject(object);
		UniUpdater tx = updaterForEntity(entity);
		tx.insert(object, this);
	}
	
	public void deleteObject(Object object) {
		UniEntity entity = entityForObject(object);
		UniUpdater tx = updaterForEntity(entity);
		tx.delete(object, this);
	}
	
	public void updateObject(Object object) {
		UniEntity entity = entityForObject(object);
		UniUpdater tx = updaterForEntity(entity);
		tx.update(object, this);
	}
	
	public UniQuery processorForEntity(UniEntity entity) {
		return processorForModel(entity.model());
	}

	public UniQuery processorForModel(UniModel model) {
		UniQuery query = processors.get(model);
		if(query == null) {
			UniObjectsSession session = uniObjectsSessions.get(model);
			query = new UniQuery(session);
			processors.put(model, query);
		}
		return query;
	}
	
	public UniUpdater updaterForEntity(UniEntity entity) {
		return updaterForModel(entity.model());
	}
	public UniUpdater updaterForModel(UniModel model) {
		UniUpdater tx = transactions.get(model);
		if(tx == null) {
			UniObjectsSession session = uniObjectsSessions.get(model);
			tx = new UniUpdater(session);
			transactions.put(model,  tx);
		}
		return tx;
	}
	
	public List<?> executeQuery(UniQuerySpecification spec) {
		UniEntity entity = spec.entity();
		if(entity == null)
			entity = entityForClass(spec.entityClass());
		UniQuery query = processorForEntity(entity);
		return query.executeQuery(spec, this);
	}
	
	public List<?> executeQuery(Class<?> entityClass, Map<String, Object> fieldValues) {
		UniPredicate predicate = UniPredicate.Util.createPredicateFromFieldValues(fieldValues);
		UniEntity entity = entityForClass(entityClass);
		UniQuerySpecification spec = new UniQuerySpecification(entity, predicate);
		return executeQuery(spec);
	}
	
	public Object findOne(UniQuerySpecification spec) {
		List<?> results = this.executeQuery(spec);
		if(results.size() > 0)
			return results.get(0);
		return null;
	}
	
	public Object findOne(Class<?> entityClass, Map<String, Object> fieldValues) {
		List<?> results = this.executeQuery(entityClass, fieldValues);
		if(results.size() > 0)
			return results.get(0);
		return null;
	}
	
	public Object find(Class<?> entityClass, Object primaryKey) {
		UniEntity entity = entityForClass(entityClass);
		UniQuery query = processorForEntity(entity);
		return query.find(entityClass, primaryKey, this);
	}
	
	public UniEntity entityForClass(Class<?> entityClass) {
		UniModelGroup.defaultModelGroup();
		for(UniModel model : UniModelGroup.models().values()) {
			UniEntity entity = model.entityForClass(entityClass);
			if(entity != null)
				return entity;
		}
		return null;
	}
	
	public UniEntity entityForObject(Object object) {
		Class<?> entityClass = object.getClass();
		return entityForClass(entityClass);
	}
	
	private Map<UniModel, List<Object>> objectsByModel(List objects) {
		Map<UniModel, List<Object>> map = new HashMap<UniModel, List<Object>>();
		for(Object object : objects) {
			UniEntity entity = this.entityForObject(object);
			UniModel model = entity.model();
			List<Object> list = map.get(model);
			if(list == null) {
				map.put(model, new ArrayList<Object>());
				list = map.get(model);
			}
			list.add(object);
		}
		
		return map;
	}
	
	public void saveChanges() throws Exception {
		for(UniUpdater tx : transactions.values()) {
			if(!tx.hasChanges())	continue;
			tx.begin();
			try {
				tx.executeInserts(this);
				tx.executeUpdates(this);
				tx.executeDeletes(this);
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				throw e;
			}
		}
	}
	
	public UniEntityID entityIDForObject(Object object) {
		UniEntity entity = entityForObject(object);
		if(entity != null) {
			UniEntityID entityID = entityCache.lookupEntityID(entity.entityName(), entity.primaryKeyForObject(object));
			if(entityID == null) {
				entityID = entityCache.add(object);
			}
			return entityID;
		}
		return null;
	}
		
	public Object cachedObject(Class<?> entityClass, Object primaryKey) {
		UniEntity entity = entityForClass(entityClass);
		return entityCache.get(entity.entityName(), primaryKey);
	}
	
	public void cache(Object object) {
		if(object != null)
			entityCache.add(object);
	}
	
	public void cacheObjects(Collection<?> objects) {
		for(Object object : objects) {
			this.cache(object);
		}
	}
	
	public void clearCache() {
		entityCache.clear();
	}
	
	public Object reload(Object object) {
		UniEntityID entityID = entityIDForObject(object);
		if(entityID != null) {
			return reloadObjectInCache(entityID, object);
		}
		return object;
	}
	
	private Object reloadObjectInCache(UniEntityID entityID, Object object) {
		Object reloaded = find(object.getClass(), entityID.primaryKey());
		if(reloaded != null) {
			cache(reloaded);
		}
		return reloaded;
	}
	
	public Object findAndCache(Class<?> entityClass, Object primarykey) {
		Object object = this.find(entityClass, primarykey);
		cache(object);
		return object;
	}

	@Override
	protected void finalize() throws Throwable {
		closeSessions();
		super.finalize();
	}

	private void closeSessions() {
		for(UniObjectsSession session : uniObjectsSessions.values()) {
			session.disconnect();
		}
	}

	/*
	 * UniDatabaseOperationCallback
	 * 
	 */
	
	private static Selector awakeFromInsertSelector = new Selector("awakeFromInsert", new Class[]{UniContext.class});
	private static Selector awakeFromFetchSelector = new Selector("awakeFromFetch", new Class[]{UniContext.class});

	@Override
	public void didInsert(Object object) {
		if(Selector.objectRespondsTo(object, awakeFromInsertSelector)) {
			awakeFromInsertSelector.safeInvoke(object, new Object[]{this});
		}
		if(_delegate.respondsTo("awakeFromInsert")) {
			try {
				_delegate.perform("awakeFromInsert", new Object[]{object, this});
			} catch (Exception e) {
				UniLogger.universe.error("awakeFromInsert delegate method failed", e);
			}
		}
	}

	@Override
	public void didFetch(Object object) {
		if(Selector.objectRespondsTo(object, awakeFromFetchSelector)) {
			awakeFromFetchSelector.safeInvoke(object, new Object[]{this});
		}
		if(_delegate.respondsTo("awakeFromFetch")) {
			try {
				_delegate.perform("awakeFromFetch", new Object[]{object, this});
			} catch (Exception e) {
				UniLogger.universe.error("awakeFromFetch delegate method failed", e);
			}
		}
	}
	
	@Override
	public void didUpdate(Object object, UniEntity entity) {
		if(_delegate.respondsTo("didUpdate")) {
			try {
				_delegate.perform("didUpdate", new Object[]{object, entity, this});
			} catch (Exception e) {
				UniLogger.universe.error("didUpdate delegate method failed", e);
			}
		}
	}

	@Override
	public void willUpdate(Object object, UniEntity entity) {
		if(_delegate.respondsTo("willUpdate")) {
			try {
				_delegate.perform("willUpdate", new Object[]{object, entity, this});
			} catch (Exception e) {
				UniLogger.universe.error("willUpdate delegate method failed", e);
			}
		}
	}

	@Override
	public void didDelete(Object object, UniEntity entity) {
		if(_delegate.respondsTo("didDelete")) {
			try {
				_delegate.perform("didDelete", new Object[]{object, entity, this});
			} catch (Exception e) {
				UniLogger.universe.error("didDelete delegate method failed", e);
			}
		}
	}

	@Override
	public void willDelete(Object object, UniEntity entity) {
		if(_delegate.respondsTo("willDelete")) {
			try {
				_delegate.perform("willDelete", new Object[]{object, entity, this});
			} catch (Exception e) {
				UniLogger.universe.error("willDelete delegate method failed", e);
			}
		}
	}

	@Override
	public boolean shouldDelete(Object object, UniEntity entity) {
		if(_delegate.respondsTo("shouldDelete")) {
			try {
				return _delegate.booleanPerform("shouldDelete", object, entity, this);
			} catch (Exception e) {
				UniLogger.universe.error("willDelete delegate method failed", e);
			}
		}
		return true;
	}
	
	
	/*
	 * Delegates
	 */
	
	public interface UniContextDelegate {
		public void awakeFromInsert(Object object, UniContext uniContext);
		public void awakeFromFetch(Object object, UniContext uniContext);
		public void willUpdate(Object object, UniEntity entity, UniContext uniContext);
		public void didUpdate(Object object, UniEntity entity, UniContext uniContext);
		public void willDelete(Object object, UniEntity entity, UniContext uniContext);
		public void didDelete(Object object, UniEntity entity, UniContext uniContext);
		public boolean shouldDelete(Object object, UniEntity entity, UniContext uniContext);
	}
	
	private Delegate _delegate = new Delegate(UniContextDelegate.class);
	
	public void setDelegate(Object delegateObject) {
		_delegate.setDelegate(delegateObject);
	}
	
}
