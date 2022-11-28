package universe.util;

import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;

public class UniFileUtil {

	public static boolean exists(String filename, UniSession uniSession) {
		boolean result = false;
		try {
			UniFile file = uniSession.openFile(filename);		// throws UniSessionException
			if((result = file.isOpen()))
				file.close();									// throws UniFileException
		} catch (UniSessionException e) {
			/*
			if(e.getErrorCode() == 14002)
				result = false;
			*/
		} catch (UniFileException e) {
			UniLogger.universe_dev.error("Could not close file '" + filename + "'",e );
		}
		return result;
	}
}
