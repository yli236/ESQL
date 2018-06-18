import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Collector {
	ArrayList<String> select;
	ArrayList<String> groupby;
	ArrayList<String> suchthat;
	ArrayList<String> Fvect;
	ArrayList<String> fvect;
	int number;
	String where;
	String having;
	
	public Collector(String select, int number, String groupby, String Fvect, String suchthat, String having, String where) {
		SQLtoJava(select, number, groupby, Fvect, suchthat, having, where);
		
	}
	
	public Collector(String filepath) {
		Properties property = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(filepath);
			property.load(input);
			SQLtoJava(property.getProperty("select"), Integer.parseInt(property.getProperty("number")),property.getProperty("groupby"),property.getProperty("Fvect"),property.getProperty("suchthat"),property.getProperty("having"),property.getProperty("where"));
		}catch(IOException e) {
			System.out.println("File not found!");
		}
	}
	

	
	/* Query input format: 
	 * SELECT ATTRIBUTE(S): 
	 * cust, avg_1_quant, avg_2_quant, avg_3_quant
	 * WHERE:
	 * year = 1997
	 * GROUPING ATTRIBUTES:
	 * cust
	 * NUMBER OF GROUPING VAIRABLES(n):
	 * 3
	 * F-VECT(V):
	 * avg_1_quant, avg_2_quant, avg_3_quant 
	 * SELECT CONDITION-VECT(suchthat):
	 * rs.getString("cust").equals(mfnode.cust) && rs.getString("state").equals("NY")
	 * rs.getString("cust").equals(mfnode.cust) && rs.getString("state").equals("CT")
	 * rs.getString("cust").equals(mfnode.cust) && rs.getString("state").equals("NJ")
	 * HAVING CONDITION(G)
	 * avg_1_quant > avg_2_quant and avg_1_quant > avg_3_quant
	 */
	
	/* Query in java format: 
	 * select[]={cust,avg_1_quant,avg_2_quant,avg_3_quant} 
	 * number=3 
	 * groupby[]={cust}
	 * where="year==1997" 
	 * f-vect[]={avg_1_quant,avg_2_quant,avg_3_quant}
	 * suchthat[]={year==1997 && rs.getString("cust").equals(mfnode.cust) && rs.getString("state").equals("NY"),year==1997 && rs.getString("cust").equals(mfnode.cust) && rs.getString("state").equals("CT"),year==1997 && rs.getString("cust").equals(mfnode.cust) && rs.getString("state").equals("NJ") }
	 * having="sum_1_quant/amount_1_quant > sum_2_quant/amount_2_quant && sum_1_quant/amount_1_quant > sum_3_quant/amount_3_quant"
	 */
	
	public void SQLtoJava(String select, int number, String groupby, String Fvect, String suchthat, String having, String where) {
		/*select aggregate functions(max, min, avg, count, sum)*/
		String[] selectinput = select.split(",");
		this.select = new ArrayList<String>();
		for(String x: selectinput) {
			x = x.replaceAll("/", "/(double)");
			this.select.add(x);
		}
		
		/*number of grouping variables*/
		this.number = number;
		
		/*groupby */
		this.groupby = new ArrayList<String>();
		String[] groupbyinput = groupby.split(",");
		for(String x: groupbyinput) {
			this.groupby.add(x);
		}
		
		/*Fvect*/
		//todo: avg => sum and amount
		this.Fvect = new ArrayList<String>();
		this.fvect = new ArrayList<String>();
		String[] fvectinput = Fvect.split(",");
		for(String x: fvectinput) {
			this.fvect.add(x);
			String[] temp = x.split("_"); //{avg,x,quant}
			if(temp[0].equals("avg")) {
				this.Fvect.add("sum_"+temp[1]+"_"+temp[2]);// adding sum_x_quant and amount_x_quant
				this.Fvect.add("amount_"+temp[1]+"_"+temp[2]);
			}else {
				this.Fvect.add(x);
			}
		}
		
		
		/*Where*/
		if(!where.isEmpty() && where!=null) {
			// year = 1997 ==> rs.getInt("year")==1997
			where.replaceAll("and", "&&");
			where.replaceAll("or", "||");
			String[] operators = {"=","<=",">=","<",">"};
			for(String operator: operators) {
				int index = where.indexOf(operator);
				if(index != -1) {
					// handle the difference of "=" in sql and "==" in java
					String befop = where.substring(0, index);
					String op = operator ;
					if(operator.equals("=")) {
						op = "==";
					}else {
						op = operator;
					}
					// handle "int" in datatypes and "Int" in "getInt"
					if(QPE.getDataTypes().get(befop).equals("int")) {
						where = "rs.getInt(\"" + befop + "\")" + operator + where.substring(index, where.length());
					}
				}
				
			}
			this.where = where;
		}else {
			this.where = where;
		}
		
		
		/*suchthat*/
		this.suchthat = new ArrayList<String>();
		String[] suchthatinput = suchthat.split(",");
		for(String x: suchthatinput) {
			if(x.contains(".prod")||x.contains(".cust")||x.contains(".state")) {
				String head = x.substring(0);
			}
		}
		for(String x: suchthatinput) {
			if(!this.where.isEmpty() && this.where!=null) {
				x = this.where + "&&" + x;
			}
			this.suchthat.add(x);
		}
		
		
		
		/*having*/
		if(having!=null && !having.isEmpty()) {
			/*avg_1_quant>avg_2_quant and avg_1_quant>avg_3_quant
			 * => mfnode.sum_1_quant/mfnode.amount_1_quant > mfnode.sum_2_quant/mfnode.amount_2_quant && mfnode.sum_1_quant/mfnode.amount_1_quant > mfnode.sum_3_quant/mfnode.amount_3_quant*/
			having = having.replaceAll("and", "&&");
			having = having.replaceAll("or", "||");
			having = having.replaceAll("=", "==");
			having = having.replaceAll("avg_", "mfnode.avg_");
			having = having.replaceAll("count_", "mfnode.count_");
			having = having.replaceAll("max_", "mfnode.max_");
			having = having.replaceAll("min", "mfnode.min_");
			having = having.replaceAll("sum", "mfnode.sum");
			int index = -1;
			while(true) {
				index = having.indexOf("mfnode.avg_", index+1);
				if(index!=-1) {
					String before = having.substring(0, index);
					String mid = having.substring(index+"mfnode.avg".length(), index+"mfnode.avg_x_quant".length());
					String after = having.substring(index+"mfnode.avg_x_quant".length());
					having  = before + "(mfnode.amount"+mid+"==0?0:"+"(mfnode.sum"+mid+"/mfnode.amount"+mid+"))"+after;
				}
				if(index ==-1) {
					break;
				}
			}
			
		}
		this.having = having;
		
	}
	
	public ArrayList<String> getSelect(){
		return this.select;
	}
	
	public ArrayList<String> getGroupby(){
		return this.groupby;
	}
	
	public ArrayList<String> getSuchthat(){
		return this.suchthat;
	}
	
	public ArrayList<String> getFevct(){
		return this.Fvect;
	}
	
	public String getWhere() {
		return this.where;
	}
	
	public String getHaving() {
		return this.having;
	}
	
	public int getNumber() {
		return this.number;
	}
	
	public ArrayList<String> getfvect(){
		return this.fvect;
	}
	
	
	
	
	
	
	
}
