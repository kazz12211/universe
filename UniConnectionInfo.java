package universe;

public class UniConnectionInfo {
	String host;
	int port;
	String username;
	String password;
	String accountPath;
	
	public String username() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String password() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String host() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int port() {
		if(port == 0)
			return 31438;
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String accountPath() {
		return accountPath;
	}
	public void setAccountPath(String accountPath) {
		this.accountPath = accountPath;
	}

	@Override
	public String toString() {
		return "{host=" + host + "; username=" + username + "; password=******" + "; accountPath=" + accountPath + "}"; 
	}
	
}
