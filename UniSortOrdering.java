package universe;

public class UniSortOrdering {

	public enum Direction {Ascending, Descending, CaseInsensitiveAsending, CaseInsensitiveDescending};
	
	String key;
	Direction direction;
	
	public UniSortOrdering(String key, Direction direction) {
		this.key = key;
		this.direction = direction;
	}
	
	public String key() {
		return key;
	}
	
	public Direction direction() {
		return direction;
	}

	public boolean isAscending() {
		return (direction == Direction.Ascending || direction == Direction.CaseInsensitiveAsending);
	}
	
	public boolean isDescending() {
		return (direction == Direction.Descending || direction == Direction.CaseInsensitiveDescending);
	}
	
	public boolean isCaseInsensitive() {
		return (direction == Direction.CaseInsensitiveAsending || direction == Direction.CaseInsensitiveDescending);
	}
	
	public String generateString(UniEntity entity) {
		UniField field = entity.fieldNamed(key());
		if(isDescending())
			return "BY.DSND " + field.columnName();
		return "BY " + field.columnName();
	}
}
