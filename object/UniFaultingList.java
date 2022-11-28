package universe.object;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import universe.UniAssociation;
import universe.UniContext;
import universe.UniQuery;

public class UniFaultingList<E> extends ArrayList<E>  {

	private static final long serialVersionUID = 1L;
	private boolean _isFault = false;
	private Object _owner;
	private UniAssociation _association;
	

	public boolean isFault() {
		return _isFault;
	}
	public void setFault(boolean isFault) {
		_isFault = isFault;
	}

	private void loadStoredValues() {
		UniContext ctx = UniContext.get();
		UniQuery processor = ctx.processorForEntity(_association.entity());
		List objects = processor.loadObjectsInAssociation(_owner, _association);
		super.clear();
		super.addAll(objects);
		_isFault = false;
	}
	
	public UniFaultingList(Object owner, UniAssociation association) {
		super();
		this._owner = owner;
		this._association = association;
		this._isFault = false;
	}

	public Object owner() {
		return _owner;
	}
	public void setOwner(Object owner) {
		this._owner = owner;
	}
	
	public UniAssociation association() {
		return _association;
	}
	
	public void setAssociation(UniAssociation assoc) {
		this._association = assoc;
	}
	
	@Override
	public boolean contains(Object arg0) {
		if(_isFault)
			loadStoredValues();
		return super.contains(arg0);
	}

	@Override
	public int indexOf(Object arg0) {
		if(_isFault)
			loadStoredValues();
		return super.indexOf(arg0);
	}

	@Override
	public int lastIndexOf(Object arg0) {
		if(_isFault)
			loadStoredValues();
		return super.lastIndexOf(arg0);
	}

	@Override
	public int size() {
		if(_isFault)
			loadStoredValues();
		return super.size();
	}

	@Override
	public Object[] toArray() {
		if(_isFault)
			loadStoredValues();
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		if(_isFault)
			loadStoredValues();
		return super.toArray(arg0);
	}

	@Override
	public Iterator<E> iterator() {
		if(_isFault)
			loadStoredValues();
		return super.iterator();
	}

	@Override
	public ListIterator<E> listIterator() {
		if(_isFault)
			loadStoredValues();
		return super.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		if(_isFault)
			loadStoredValues();
		return super.listIterator(index);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		if(_isFault)
			loadStoredValues();
		return super.subList(fromIndex, toIndex);
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		if(_isFault)
			loadStoredValues();
		return super.containsAll(arg0);
	}



}
