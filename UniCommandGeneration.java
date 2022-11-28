package universe;

import java.util.ArrayList;
import java.util.List;

import universe.UniObjectsSession.AggregateFunction;
import universe.UniObjectsSession.AggregateFunctions;
import universe.UniObjectsSession.Count;
import universe.UniObjectsSession.Select;
import ariba.util.core.ListUtil;
import asjava.uniobjects.UniCommand;
import asjava.uniobjects.UniSessionException;

public class UniCommandGeneration {

	private UniObjectsSession session;

	public UniCommandGeneration(UniObjectsSession session) {
		this.session = session;
	}
	
	public UniCommand generate(Select select) throws UniSessionException {
		String commandString;
		UniQuerySpecification spec = select.querySpecification();
		StringBuffer buffer = new StringBuffer();
		buffer.append("SELECT ");
		buffer.append(spec.entity().filename());
		
		if(spec.sortOrderings() != null && spec.sortOrderings().size() > 0) {
			List<String> strings = new ArrayList<String>();
			for(UniSortOrdering so : spec.sortOrderings()) {
				strings.add(so.generateString(spec.entity()));
			}
			String orderby = ListUtil.listToString(strings, " ");
			if(orderby != null) {
				buffer.append(" ");
				buffer.append(orderby);
			}
		}

		if(spec.predicate() != null) {
			String condition = spec.predicate().generateString(spec.entity());
			if(condition != null) {
				buffer.append(" WITH ");
				buffer.append(condition);
			}
		}
		
		buffer.append(" TO " + select.listNumber());
		commandString = buffer.toString();

		return session.uniSession().command(commandString);
	}

	public UniCommand generate(AggregateFunctions aggregateFunction) throws UniSessionException {
		UniQuerySpecification spec = aggregateFunction.querySpecification();
		UniEntity entity = spec.entity();
		UniField field = entity.fieldNamed(aggregateFunction.key());
		StringBuffer buffer = new StringBuffer();
		buffer.append("LIST ");
		buffer.append(entity.filename());
		buffer.append(" TOTAL "); buffer.append(field.columnName());
		buffer.append(" MAX "); buffer.append(field.columnName());
		buffer.append(" MIN "); buffer.append(field.columnName());
		buffer.append(" AVG "); buffer.append(field.columnName());
		
		if(spec.predicate() != null) {
			String condition = spec.predicate().generateString(entity);
			buffer.append(" WITH ");
			buffer.append(condition);
		}
		buffer.append(" DET.SUP");
		
		String commandString = buffer.toString();
		return session.uniSession().command(commandString);
	}

	public UniCommand generate(Count function) throws UniSessionException {
		UniQuerySpecification spec = function.querySpecification();
		UniEntity entity = spec.entity();
		StringBuffer buffer = new StringBuffer();
		buffer.append("COUNT ");
		buffer.append(entity.filename());
		
		if(spec.predicate() != null) {
			String condition = spec.predicate().generateString(entity);
			buffer.append(" WITH ");
			buffer.append(condition);
		}
		
		String commandString = buffer.toString();
		return session.uniSession().command(commandString);
	}

	public UniCommand generate(AggregateFunction aggregateFunction) throws UniSessionException {
		UniQuerySpecification spec = aggregateFunction.querySpecification();
		UniEntity entity = spec.entity();
		UniField field = entity.fieldNamed(aggregateFunction.key());
		StringBuffer buffer = new StringBuffer();
		buffer.append("LIST ");
		buffer.append(entity.filename());
		buffer.append(" "); buffer.append(aggregateFunction.functionName().toUpperCase()); buffer.append(" ");  buffer.append(field.columnName());
		
		if(spec.predicate() != null) {
			String condition = spec.predicate().generateString(entity);
			buffer.append(" WITH ");
			buffer.append(condition);
		}
		buffer.append(" DET.SUP");
		
		String commandString = buffer.toString();
		return session.uniSession().command(commandString);
	}

}
