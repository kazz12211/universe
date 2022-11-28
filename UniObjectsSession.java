package universe;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import universe.util.UniLogger;
import ariba.util.core.ListUtil;
import asjava.uniobjects.UniCommand;
import asjava.uniobjects.UniDictionary;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniObjectsTokens;
import asjava.uniobjects.UniSelectList;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;

public class UniObjectsSession {

	protected UniJava uniJava;
	protected UniSession uniSession;
	protected UniModel model;
	CommandHistory commandHistory = new CommandHistory();
	private Map<String, UniFile> openedFiles = null;
	private Map<String, UniDictionary> openedDictionaries = null;

	public UniObjectsSession(UniModel model) {
		this.model = model;
	}

	public UniModel model() {
		return model;
	}

	public boolean isConnected() {
		if(uniSession == null)
			return false;
		return (uniSession.isActive() && uniSession.connection.isConnected());
	}
	
	public UniSession establishConnection() throws Exception {
		if(uniSession == null) {
			uniJava = new UniJava();
			UniLogger.universe_test.debug("Opening UniSession");
			uniSession = uniJava.openSession();
		}
		if(!uniSession.isActive()) {
			UniConnectionInfo connInfo = model.connectionInfo();
			UniLogger.universe_test.debug("Connecting to UniVerse");
			uniSession.connect(connInfo.host(), connInfo.port(), connInfo.username(), connInfo.password(), connInfo.accountPath());
			uniSession.nlsMap().setName("NONE");
		}
		if(isConnected()) {
			openedFiles = new HashMap<String, UniFile>();
			openedDictionaries = new HashMap<String, UniDictionary>();
		}
		return uniSession;
	}
	
	public void disconnect() {
		if(this.isConnected()) {
			try {
				UniLogger.universe_test.debug("Closing UniSession");
				uniSession.disconnect();
				this.closeFiles();
				this.closeDictionaries();
			} catch (UniSessionException e) {
				UniLogger.universe.error(e);
			}
			uniSession = null;
			uniJava = null;
		}
	}
	
	public UniSession uniSession() {
		return uniSession;
	}


	private void _checkConnection() {
		if(!isConnected()) {
			UniLogger.universe.debug("UniObjectsSession is automatically connecting to database");
			try {
				establishConnection();
			} catch (Exception e) {
				UniLogger.universe.warn("UniObjectsSession failed to connect to database");
			}
		}
	}

	public void executeUniCommand(String commandString, String...replies) throws Exception {
		this._checkConnection();
		UniCommand uCommand = uniSession.command();
		uCommand.setCommand(commandString);
		uCommand.exec();
		commandHistory.record(commandString);
		UniLogger.universe_dev.debug(">>" + uCommand.response());
		
		for(String reply : replies) {
			if(uCommand.status() != UniObjectsTokens.UVS_REPLY) 
				break;
			uCommand.reply(reply);
			UniLogger.universe_dev.debug("<<" + reply);
			UniLogger.universe_dev.debug(">>" + uCommand.response());
		}
	}
	
	public String executeUniCommand(String commandString) throws Exception {
		this._checkConnection();
		UniCommand command = uniSession.command();
		command.setCommand(commandString);
		command.exec();
		commandHistory.record(command.getCommand());
		return command.response();
	}

	public String executeUniCommand(UniCommand command) throws Exception {
		this._checkConnection();
		command.exec();
		commandHistory.record(command.getCommand());
		return command.response();
	}

	public UniFile openFile(String filename) throws UniSessionException {
		this._checkConnection();
		UniLogger.universe_test.debug("Opening file '" + filename + "'");
		return uniSession.openFile(filename);
	}
	public UniDictionary openDict(Object filename) throws UniSessionException {
		this._checkConnection();
		UniLogger.universe_test.debug("Opening dictionary '" + filename + "'");
		return uniSession.openDict(filename);
	}
	public UniSelectList selectList(int listNumber) throws UniSessionException {
		this._checkConnection();
		return uniSession.selectList(listNumber);
	}

	public UniFile getFile(String filename) {
		UniFile file = null;
		if(openedFiles.containsKey(filename))
			file = openedFiles.get(filename);
		else {
			try {
				file = openFile(filename);
				openedFiles.put(filename, file);
			} catch (UniSessionException e) {
				UniLogger.universe.warn("UniObjectsSession.getFile(" + filename + "): file open error", e);
			}
		}
		return file;
	}

	public UniDictionary getDict(String filename) {
		UniDictionary dict = null;
		if(openedDictionaries.containsKey(filename))
			dict = openedDictionaries.get(filename);
		else {
			try {
				dict = openDict(filename);
				openedDictionaries.put(filename, dict);
			} catch (UniSessionException e) {
				UniLogger.universe.warn("UniObjectsSession.getDict(" + filename + "): file open error", e);
			}
		}
		return dict;
	}


	protected void closeFiles() {
		for(UniFile file : openedFiles.values()) {
			if(file.isOpen())
				try {
					UniLogger.universe_test.debug("Closing file '" + file.getFileName() + "'");
					file.close();
				} catch (UniFileException e) {
					UniLogger.universe.warn("UniObjectsSession.closeFiles(): file close error", e);
				}
		}
		openedFiles.clear();
	}
	protected void closeDictionaries() {
		for(UniDictionary dict : openedDictionaries.values()) {
			if(dict.isOpen())
				try {
					UniLogger.universe_test.debug("Closing dictionary '" + dict.getFileName() + "'");
					dict.close();
				} catch (UniFileException e) {
					UniLogger.universe.warn("UniObjectsSession.closeDictionaries(): file close error", e);
				}
		}
		openedDictionaries.clear();
	}

	public void closeFile(UniFile file) {
		for(String filename : openedFiles.keySet()) {
			UniFile aFile = openedFiles.get(filename);
			if(file == aFile && file.isOpen()) {
				try {
					UniLogger.universe_test.debug("Closing file '" + file.getFileName() + "'");
					file.close();
				} catch (UniFileException e) {
					UniLogger.universe.warn("UniObjectsSession.closeFile(" + filename + "): file close error", e);
				}
				openedFiles.remove(filename);
				break;
			}
		}
	}
	
	public void closeDict(UniDictionary dict) {
		for(String filename : openedDictionaries.keySet()) {
			UniDictionary aDict = openedDictionaries.get(filename);
			if(dict == aDict && dict.isOpen()) {
				try {
					UniLogger.universe_test.debug("Closing dictionary '" + dict.getFileName() + "'");
					dict.close();
				} catch (UniFileException e) {
					UniLogger.universe.warn("UniObjectsSession.closeDictionary(" + filename + "): file close error", e);
				}
				openedFiles.remove(filename);
				break;
			}
		}
	}
		
	public CommandHistory commandHistory() {
		return commandHistory;
	}
	
	public static abstract class Command {		
		public abstract UniCommand uniCommand(UniCommandGeneration generator) throws Exception;
	}
	
	public static class Select extends Command {
		
		int listNumber;
		UniQuerySpecification spec;
		
		public Select(String filename,String condition, int listNumber) {
			this.listNumber = listNumber;
		}
		
		public Select(UniQuerySpecification spec, int listNumber) {
			this.listNumber = listNumber;
			this.spec = spec;
		}

		public int listNumber() {
			return listNumber;
		}
		
		public UniQuerySpecification querySpecification() {
			return spec;
		}
		
		@Override
		public UniCommand uniCommand(UniCommandGeneration generator)  throws Exception {
			return generator.generate(this);
		}

		public String filename() {
			UniEntity entity = spec.entity();
			return entity.filename();
		}
			
	}
	
	public static class Count extends Command {
		UniQuerySpecification spec;

		public Count(UniQuerySpecification spec) {
			this.spec = spec;
		}

		public UniQuerySpecification querySpecification() {
			return spec;
		}
		
		@Override
		public UniCommand uniCommand(UniCommandGeneration generator)  throws Exception {
			return generator.generate(this);
		}
	}
	
	public static class AggregateFunctions extends Command {
		String key;
		UniQuerySpecification spec;

		public AggregateFunctions(String key, UniQuerySpecification spec) {
			this.key = key;
			this.spec = spec;
		}
		
		public UniQuerySpecification querySpecification() {
			return spec;
		}
		public String key() {
			return key;
		}
		
		@Override
		public UniCommand uniCommand(UniCommandGeneration generator)  throws Exception {
			return generator.generate(this);
		}
	}
	
	public static class AggregateFunction extends Command {
		String key;
		String funcName;
		UniQuerySpecification spec;

		public AggregateFunction(String key, String funcName, UniQuerySpecification spec) {
			this.key = key;
			this.funcName = funcName;
			this.spec = spec;
		}
		
		public UniQuerySpecification querySpecification() {
			return spec;
		}
		public String key() {
			return key;
		}
		public String functionName() {
			return funcName;
		}
		
		@Override
		public UniCommand uniCommand(UniCommandGeneration generator)  throws Exception {
			return generator.generate(this);
		}
	}
	
	private static final int MAX_HISTORY = 200;
	private static final DateFormat HISTORY_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss S");
	
	public static class CommandHistory {
		long timestamps[];
		List<String> commands;
		int size;
		
		public CommandHistory() {
			timestamps = new long[MAX_HISTORY];
			commands = new ArrayList<String>(MAX_HISTORY);
			size = 0;
		}
		
		public synchronized void record(String command) {
			UniLogger.universe_command.debug(command);
			if(size == MAX_HISTORY) {
				commands.remove(0);
				System.arraycopy(timestamps, 1, timestamps, 0, timestamps.length-1);
				timestamps[MAX_HISTORY-1] = 0;
			} else {
				size++;
			}
			commands.add(command);
			timestamps[size-1] = System.currentTimeMillis();
		}
		
		public String lastCommandWithTimestamp() {
			Date date = new Date(timestamps[size-1]);
			return HISTORY_TIMESTAMP_FORMAT.format(date) + " " + commands.get(size-1);
		}
		
		public int size() {
			return size;
		}
		
		public List<String> listCommands() {
			List<String> list = ListUtil.list();
			for(int i = size - 1; i >= 0; i--) {
				Date date = new Date(timestamps[i]);
				list.add(HISTORY_TIMESTAMP_FORMAT.format(date) + " " + commands.get(i));
			}
			return list;
		}
		
		public void clear() {
			Arrays.fill(timestamps, 0);
			commands.clear();
			size = 0;
		}
	}


}
