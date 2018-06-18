import java.io.IOException;
import java.util.*;
import java.sql.*;

public class QPE {
	static String url = "jdbc:postgresql://localhost:5432/sales";
	static String username = "postgres";
	static String password = "liyingdong";
	static HashMap<String, String> dataTypes = null;
	
	public static int wirteHeader(Writer writer, String classname, int space) throws IOException {
		writer.writing(space, "import java.sql.*;");
		writer.writing(space, "import java.util.*;");
		writer.writing(space, "");	
		writer.writing(space, "public class "+classname+"{");
		return space;
	}
	
	public static int writeConnection(Writer writer, int space) throws IOException {
		//Load the driver
		writer.writing(space, "try{ ");
		writer.writing(space+1, "Class.forName(\"org.postgresql.Driver\");");
		writer.writing(space+1, "System.out.println(\"Successfully loaded the driver!\");");
		writer.writing(space, "}");
		writer.writing(space, "catch(Exception e){ ");
		writer.writing(space+1, "System.out.println(\"Failed to load the driver\");");
		writer.writing(space+1, "e.printStackTrace();");
		writer.writing(space, "}");
		//Connecting to the database
		writer.writing(space, "try{ ");
		writer.writing(space+1, "Connection con = DriverManager.getConnection(url, username, password);");
		writer.writing(space+1, "System.out.println(\"Successfully connected to the server!\");");
		writer.writing(space+1, "Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);"); /*https://stackoverflow.com/questions/6367737/resultset-exception-set-type-is-type-forward-only-why*/
		writer.writing(space+1, "rs = stmt.executeQuery(\"SELECT * FROM sales\");");
		//Beginning of the loop (first scan)
		writer.writing(space+1, "while (rs.next()){ ");
		return space;
	}
	
	public static int writescanLoop(Writer writer, int space) throws IOException {
		// Not the first scan
		writer.writing(space, "try{ ");
		writer.writing(space+1, "while (rs.next()) {");
		return space;
	}
	
	public static int writemainfunc(Writer writer, int space) throws IOException {
		writer.writing(space, "public static void main(String[] args) throws SQLException {");
		return space;
	}
	
	public static int writeendbrace(Writer writer, int space) throws IOException {
		//Right part of curly brackets
		writer.writing(space, "}");
		return space;
	}
	
	public static int writeLoopending(Writer writer, int space) throws IOException{
		writer.writing(space, "}");
		writer.writing(space-1, "}");
		writer.writing(space-1, "catch(SQLException a) {");
		writer.writing(space, "System.out.println(\"Connection URL or Username or password errors\");");
		writer.writing(space, "a.printStackTrace();");
		writer.writing(space-1, "}");
		return space;
	}
	
	public static HashMap<String, String> getDataTypes(){
		String url = "jdbc:postgresql://localhost:5432/sales";
		String username = "postgres";
		String password = "liyingdong";
		if(dataTypes == null) {
			dataTypes = new HashMap<String, String>();
			// load database server
			try {
				Class.forName("org.postgresql.Driver");
				System.out.println("Successfully loaded the driver!");
			}
			catch (Exception e) {
				System.out.println("Failed to load the driver");
				e.printStackTrace();
			}
			//get datatypes
			try {
				Connection con = DriverManager.getConnection(url, username, password);
				System.out.println("Successfully connected to the server!");
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT column_name, data_type from information_schema.columns where table_name = 'sales'");
				while (rs.next()) {
					if(rs.getString("data_type").indexOf("character")!=-1) {
						dataTypes.put(rs.getString("column_name"), "String");
					}
					else if(rs.getString("data_type").equals("integer")) {
						dataTypes.put(rs.getString("column_name"), "int");
					}
				}
			}
			catch (SQLException a) {
				System.out.println("Connection URL or Username or password errors");
				a.printStackTrace();
			}
		}
		return dataTypes;
	}
	
	public static int firstloop(Writer writer, Collector collector, int space) throws IOException{
		writer.writing(space, "boolean found = false;");
		writer.writing(space, "for(mf_Structure mfnode: mfList){");
		int i = 0;
		// set if condition in first loop (if mfnode.cust.equals(rs.getString("cust")))
		String ifcon = "";
		for(String gv: collector.getGroupby()) {
			HashMap<String, String> datatype = QPE.getDataTypes();
			String type = "String";
			String operator = ".equals";
			if(datatype.get(gv).equals("int")) {
				type = "Int";
				operator = "==";
			}
			//first condition
			if(i==0) {
				ifcon = "mfnode."+gv+operator+"(rs.get"+type+"(\""+gv+"\"))"; 
				i = 1;
			}else {
			//other condition
				ifcon = ifcon + "&&" + "mfnode." + gv+operator+	"(rs.get"+ type +"(\""+gv+"\"))";
			}
		}
		// adding where clause
		String where = "";
		if(collector.getWhere()!=null && !collector.getWhere().isEmpty()) {
			where = " && " + collector.getWhere(); 
		}
		ifcon = ifcon + where;
		writer.writing(space, "if("+ifcon+"){");
		writer.writing(space+2, "found = true;");
		writer.writing(space+1, "}");
		writer.writing(space, "}");
		// add new mfnode
		writer.writing(space, "if(!found "+where+"){");
		writer.writing(space+1, "mf_Structure newnode = test.new mf_Structure();");
		for(String gv: collector.getGroupby()) {
			String type = "String";
			HashMap<String, String> datatype = QPE.getDataTypes();


			if(datatype.get(gv).equals("int")) {
				type = "Int";
			}
			writer.writing(space+1, "newnode."+gv+"= rs.get"+type+"(\""+gv+"\");");

		}
		for(String fvect: collector.getFevct()) {
			writer.writing(space+1, "newnode."+fvect+"=0;");
		}
		writer.writing(space+1, "mfList.add(newnode);");
		writer.writing(space, "}");
		return space;
		
	}
	
	public static int updatingLoop(Writer writer, Collector collector, String fvect, int space) throws IOException {
		writer.writing(space, "for(mf_Structure mfnode: mfList){");
		boolean gv0 = false;
		//check if there is grouping variables 0 in suchthat
		for(String fv: collector.getfvect()) {
			if(fv.indexOf("_0_")!= -1) 
				gv0=true;
		}
		int x = gv0?0:1;
		
		String[] split = fvect.split("_"); // {sum,1，quant}
		writer.writing(space+1, "if("+collector.getSuchthat().get(Integer.parseInt(split[1])-x)+"){");
		// aggregation functions
		if(split[0].equals("sum")) {
			// mfnode.sum_1_quant += rs.getInt(quant);
			writer.writing(space+2, "mfnode."+fvect+"+="+"rs.getInt(\"quant\");");
		}
		else if(split[0].equals("count")) {
			// mfnode.count_1_quant ++;
			writer.writing(space+2, "mfnode."+fvect+"++;");
		}
		else if(split[0].equals("max")) {
			// mfnode.max_1_quant = Math.max(mfnode.max_1_quant, rs.getInt("quant"));
			writer.writing(space+2, "mfnode."+fvect+"= Math.max(mfnode."+fvect+", rs.getInt(\"quant\"));");
		}
		else if(split[0].equals("min")) {
			// mfnode.min_1_quant = Math.min(mfnode.min_1_quant, rs.getInt("quant"));
			writer.writing(space+2,  "mfnode."+fvect+"= Math.min(mfnode."+fvect+", rs.getInt(\"quant\"));");
		}
		else if(split[0].equals("avg")) {
			// mfnode.sum_1_quant += rs.getInt("quant");
			// mfnode.amount_1_quant ++; 
			String avg_1 = "sum_"+split[1]+"_"+split[2];
			writer.writing(space+2, "mfnode."+avg_1+"+="+"rs.getInt(\"quant\");");
			String avg_2 = "amount_" + split[1]+"_"+split[2];
			writer.writing(space+2, "mfnode."+avg_2+"++;");
		}
		
		writer.writing(space+1, "}");
		writer.writing(space, "}");
		return space;
	}
	
	public static int outputselect(Writer writer, Collector collector, int space) throws IOException{
		for(String gb: collector.getGroupby()) {
			writer.writing(space, "System.out.print(mfnode."+gb+"+\"\\t\");");
		}
		for(String select: collector.getSelect()) {
			if(select.indexOf("_")==-1) {
				continue;
			}
			select = select.replaceAll("sum", "mfnode.sum");
			select = select.replaceAll("count", "mfnode.count");
			select = select.replaceAll("max", "mfnode.max");
			select = select.replaceAll("min", "mfnode.min");
			select = select.replaceAll("avg", "mfnode.avg");
			// avg_x_quant ==> sum_x_quant / amount_x_quant
			int index = -1;
			do{
				index = select.indexOf("mfnode.avg_", index+1);
				if(index != -1){
					String pre = select.substring(0, index);
					String mid = select.substring(index+"mfnode.avg".length(), index+"mfnode.avg_x_quant".length());
					String behind = select.substring(index+"mfnode.avg_x_quant".length());
					select = pre+"(mfnode.amount" + mid+"==0?0:"+"(mfnode.sum" + mid+ "/mfnode.amount" + mid + "))"+behind;
				}
			}while(index != -1);
			
			writer.writing(space, "System.out.print("+select+"+\"\\t\"+\"\\t\");");
		}
		
		writer.writing(space, "System.out.println();");
		return space;
	}
	
	
	
	public static void generateFile(String filepath, String dirPath) throws IOException {
		// set filepath
		String className = filepath.substring(filepath.lastIndexOf("/")+1, filepath.lastIndexOf("."));
		String javaFileName = dirPath+className+".java";
		// read esql file
		Writer writer = new Writer(javaFileName);
		Collector collector = new Collector(filepath);
		int space = 0; 
		// import and public class Query{
		QPE.wirteHeader(writer, "Query", space); 
		HashMap<String, String> datatypes = QPE.getDataTypes();
		// class mf_Sturcture
		mf_Writer mfclass = new mf_Writer(collector, datatypes);
		// output mf_Structure class
		space++;
		mfclass.writemfclass(writer, space);  

		// main function
		QPE.writemainfunc(writer, space);
		writer.writing(space, "Query test = new Query();");
		space ++;
		// initiate mf_structure list
		writer.writing(space, "List<mf_Structure> mfList = new LinkedList<mf_Structure>();");
		writer.writing(space, "ResultSet rs = null;");
		// connection to database
		writer.writing(space, "String username = \""+QPE.username+"\";");
		writer.writing(space, "String password = \""+QPE.password+"\";");
		writer.writing(space, "String url = \""+ QPE.url+"\";");
		QPE.writeConnection(writer, space);
		// first loop of scan
		space++;
		QPE.firstloop(writer, collector, space+1);
		QPE.writeLoopending(writer, space);
		// multiple scan to fill the mf_List based on Fvect
		space--;
		for(String fvect: collector.getfvect()) {
			writer.writing(space, "rs.beforeFirst();");
			QPE.writescanLoop(writer, space);
			QPE.updatingLoop(writer, collector, fvect, space+2);
			QPE.writeLoopending(writer, space+1);
		}
		// output the results
		for(String x: collector.getSelect()) {
			writer.writing(space, "System.out.print(\""+x+"\"+\"\\t\");");
		}
		writer.writing(space, "System.out.println();");
		writer.writing(space, "for(mf_Structure mfnode: mfList){");
		String having = collector.getHaving();
		if(having!=null && !having.isEmpty()) {
			writer.writing(space+1, "if("+having +"){");
		}
		QPE.outputselect(writer, collector, space+2);
		if(having!=null && !having.isEmpty()) {
			writer.writing(space+1, "}");
		}
		writer.writing(space, "}");// end of for loop
		QPE.writeendbrace(writer, space);
		QPE.writeendbrace(writer, space);
		writer.close();
		System.out.println("File Generated!");
		
	}
}
