package javaclient;

public class Request {
	private String type;
	private String request;
	
	public Request(String type, String request) {
		this.request = request;
		this.type = type;
	}
	
	public String getType() {
		return this.type;
	}
	
	public String getRequest() {
		return this.request;
	}
}