package universe.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import asjava.uniclientlibs.UniConnection;
import asjava.uniclientlibs.UniString;

public class UniStringUtil {

	public static UniString getString(UniConnection connection, Object object) {
		return new UniString(connection, object);
	}
	
	public static String coerceToString(UniString uniString) {		
		return uniString.toString();
	}
	
	
	public static Number coerceToNumber(UniString uniString) {
		String str = uniString.toString();
		if(str.contains("."))
			return new BigDecimal(str);
		else
			return new BigInteger(str);
	}
		

}
