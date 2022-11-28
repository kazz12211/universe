package universe;

import java.util.ArrayList;
import java.util.List;

public class UniQuerySpecification {
	UniEntity entity;
	Class<?> entityClass;
	UniPredicate predicate;
	List<UniSortOrdering> sortOrderings;
		
	public UniQuerySpecification(UniEntity entity, UniPredicate predicate) {
		this.entity = entity;
		this.entityClass = entity.entityClass();
		this.predicate = predicate;
	}
	
	public Class<?> entityClass() {
		return entityClass;
	}

	public UniEntity entity() {
		if(entity == null && entityClass != null) {
		}
		return entity;
	}
	
	public UniPredicate predicate() {
		return predicate;
	}
	
	public void setSortOrderings(List<UniSortOrdering> sortOrderings) {
		this.sortOrderings = sortOrderings;
	}
	
	public List<UniSortOrdering> sortOrderings() {
		if(sortOrderings == null) {
			sortOrderings = new ArrayList<UniSortOrdering>();
		}
		return sortOrderings;
	}
}
