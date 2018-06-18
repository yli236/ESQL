
import java.io.*;
public class Writer {
	private FileWriter mywriter = null;
	
	public Writer(FileWriter mywriter) {
		super();
		this.mywriter = mywriter;
	}
	
	public Writer(String path) throws IOException {
		this.mywriter = new FileWriter(path);
	}
	
	public void writing(int space, String code) throws IOException {
		if(this.mywriter==null) {
			return;
		}
		String nextline = System.getProperty("line.separator");
		for(int i=0; i<space;i++) {
			mywriter.write("\t");
		}
		mywriter.write(code+nextline);
	}
	
	public void close() throws IOException {
		if(this.mywriter!=null) {
			mywriter.close();
		}
	}
}
