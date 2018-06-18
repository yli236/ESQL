import java.sql.*;
import java.util.*;

public class Query{
	public class mf_Structure{
		String prod;
		int quant;
		int count_1_quant;
		int count_2_quant;
	}
	public static void main(String[] args) throws SQLException {
	Query test = new Query();
		List<mf_Structure> mfList = new LinkedList<mf_Structure>();
		ResultSet rs = null;
		String username = "postgres";
		String password = "liyingdong";
		String url = "jdbc:postgresql://localhost:5432/sales";
		try{ 
			Class.forName("org.postgresql.Driver");
			System.out.println("Successfully loaded the driver!");
		}
		catch(Exception e){ 
			System.out.println("Failed to load the driver");
			e.printStackTrace();
		}
		try{ 
			Connection con = DriverManager.getConnection(url, username, password);
			System.out.println("Successfully connected to the server!");
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery("SELECT * FROM sales");
			while (rs.next()){ 
				boolean found = false;
				for(mf_Structure mfnode: mfList){
				if(mfnode.prod.equals(rs.getString("prod"))&&mfnode.quant==(rs.getInt("quant"))){
						found = true;
					}
				}
				if(!found ){
					mf_Structure newnode = test.new mf_Structure();
					newnode.prod= rs.getString("prod");
					newnode.quant= rs.getInt("quant");
					newnode.count_1_quant=0;
					newnode.count_2_quant=0;
					mfList.add(newnode);
				}
			}
		}
		catch(SQLException a) {
			System.out.println("Connection URL or Username or password errors");
			a.printStackTrace();
		}
		rs.beforeFirst();
		try{ 
			while (rs.next()) {
				for(mf_Structure mfnode: mfList){
					if(rs.getString("prod").equals(mfnode.prod)){
						mfnode.count_1_quant++;
					}
				}
			}
		}
		catch(SQLException a) {
			System.out.println("Connection URL or Username or password errors");
			a.printStackTrace();
		}
		rs.beforeFirst();
		try{ 
			while (rs.next()) {
				for(mf_Structure mfnode: mfList){
					if( rs.getString("prod").equals(mfnode.prod)&&rs.getInt("quant")>mfnode.quant){
						mfnode.count_2_quant++;
					}
				}
			}
		}
		catch(SQLException a) {
			System.out.println("Connection URL or Username or password errors");
			a.printStackTrace();
		}
		System.out.print("prod"+"\t");
		System.out.print("quant"+"\t");
		System.out.println();
		for(mf_Structure mfnode: mfList){
			if(mfnode.count_2_quant==mfnode.count_1_quant/2){
				System.out.print(mfnode.prod+"\t");
				System.out.print(mfnode.quant+"\t");
				System.out.println();
			}
		}
		}
		}
