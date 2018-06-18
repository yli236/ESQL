import java.io.IOException;
import java.util.*;

public class mf_Writer {
	private ArrayList<String> writings;
	public mf_Writer(Collector collecter, HashMap<String, String> datatype) {
		this.writings = new ArrayList<String>();
		this.writings.add("public class mf_Structure{");
		for(String x: collecter.getGroupby()) {
			this.writings.add("\t" + datatype.get(x)+" "+x+";");
		}
		
		for(String x: collecter.getFevct()) {
			this.writings.add("\t" + datatype.get(x.substring(x.lastIndexOf("_")+1))+" "+x+";");
		}
		this.writings.add("}");
	}
	
	public int writemfclass(Writer writer, int space) throws IOException {
		for(String x: writings) {
			writer.writing(space, x);
		}
		return space;
	}
	
	
	

}
