package com.myDatabase;

import java.util.List;
import java.awt.dnd.DnDConstants;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.nio.channels.NetworkChannel;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.RandomAccess;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.swing.table.TableStringConverter;
import javax.xml.datatype.DatatypeConfigurationException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.CreateTableDeParser;
import net.sf.jsqlparser.util.deparser.ReplaceDeParser;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.util.*;

@SuppressWarnings("unused")
public class MyDatabase {

	/*
	 * can be used further to support multiple databases
	 */
	//public static String dbName = "";
	public static String anchor = "myDatabase> ";
	public static int pageSize = 512;
	public static String dirName = "./data/";
	public static int pageHeadLen = 8;
	public static RandomAccessFile tables; //meta data table file for storing table data
	public static RandomAccessFile columns; //meta data table file for storing column data
	public static ArrayList<ArrayList<String>> dataTypeList = null;
	public static int bPlusTreeOrder = 5;

	public static void main(String[] args) throws IOException, JSQLParserException {
		// TODO Auto-generated method stub
		/*
		 * display a welcome message and prompt
		 */
		dispWelcomeScreen();
		/*
		 * initialize configuration data
		 */
		initConfData();
		/*
		 * create required directories and meta_data
		 * tables if not present
		 */
		createDirStruct();

		MyDatabase db = new MyDatabase();
		/*
		 * each command is terminated by ;
		 */
		Scanner sc = new Scanner(System.in).useDelimiter(";");
		String userinput = null;
		//run code till user input is exit
		do {
			System.out.print(anchor);
			/*
			 * replace new line with space
			 * convert user input to lower case
			 */
			userinput = sc.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();	
			userinput = userinput.replace("'", "");
			try{
				processCommand(userinput);		
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		} while (!userinput.equalsIgnoreCase("exit"));
		sc.close();
	}

	private static void initConfData() {
		// TODO Auto-generated method stub

		/*
		 * create data_type,size,serial_code data list
		 */
		dataTypeList = new ArrayList<>();

		ArrayList<String> dummyList = new ArrayList<>();
		dummyList.add("tinyint");
		dummyList.add("1");
		dummyList.add("4");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("smallint");
		dummyList.add("2");
		dummyList.add("5");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("int");
		dummyList.add("4");
		dummyList.add("6");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("bigint");
		dummyList.add("8");
		dummyList.add("7");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("real");
		dummyList.add("4");
		dummyList.add("8");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("double");
		dummyList.add("8");
		dummyList.add("9");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("datetime");
		dummyList.add("8");
		dummyList.add("10");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("date");
		dummyList.add("8");
		dummyList.add("11");
		dataTypeList.add(dummyList);

		dummyList = new ArrayList<>();
		dummyList.add("text");
		dummyList.add("100");
		dummyList.add("12");
		dataTypeList.add(dummyList);
	}

	private static void processCommand(String userinput) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println( "execute:" + userinput);
		parseQuery(userinput);
	}

	private static void parseQuery(String userInput) throws Exception {
		// TODO Auto-generated method stub
		/*
		 * parse and validate query then send to corresponding module
		 * for execution
		 */
		String []queryWord = userInput.split(" ");

		switch (queryWord[0]) {
		case "create":
			createTable(userInput);   
			break;

		case "drop":
			dropTable(userInput);
			break;	

		case "show":
			showTables();
			break;	

		case "insert":
			Insert(userInput,"");
			break;	

		case "update":
			update(userInput);
			break;	

		case "delete":
			delete(userInput);
			break;

		case "select":
			select(userInput);
			break;	

		case "exit":
			System.out.println("Exiting from myDatabase.");
			break;	

		case "help":
			showHelp();
			break;	

		default:
			createError("Invalid command");
			break;
		}
		return;

	}

	public static void select(String userInput) throws Exception {
		// TODO Auto-generated method stub
		/*
		 * parse, validate and execute select query
		 */
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		try {
			statement = pm.parse(new StringReader(userInput));
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			createError("You have an error in your SQL syntax");
		}

		if (statement instanceof Select)
		{
			//SelectUtils.addExpression
			//parse table name,column name and where conditions
			Select selectStatement = (Select) statement;
			PlainSelect pl = (PlainSelect)selectStatement.getSelectBody();
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			@SuppressWarnings("unchecked")
			java.util.List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
			String tabName = tableList.get(0);
			
			File rf = null;
			if (tabName.equalsIgnoreCase("mydatabase_columns") || tabName.equalsIgnoreCase("mydatabase_tables"))
				 rf = new File(dirName+"/catalog/" + tabName + ".tbl");
			else
				rf = new File(dirName+"/user_data/" + tabName + ".tbl");
			
			if(!rf.exists())
			{
				createError("Table " + tabName + " not present");
				return;
			}

			ArrayList<ArrayList<String>> tabData = readTable(tabName);
			ArrayList<ArrayList<String>> recFormat = getRecFormat(tabName);

			String[] selectParts = userInput.split(" ");
			ArrayList<String> dispColList = getDispColList(userInput,recFormat);
			//ArrayList<ArrayList<String>> whrColValList = getWhrColVal(userInput);
			//select column name
			ArrayList<String> selList = new ArrayList<>();
			if(selectParts[1].equals("*"))
			{
				for (int i = 0; i < recFormat.size(); i++) {
					selList.add(recFormat.get(i).get(0));
				}
			}
			else
			{
				@SuppressWarnings("unchecked")
				java.util.List<SelectItem> selectList = pl.getSelectItems();

				for (int j = 0; j < recFormat.size(); j++) {
					for (int i = 0; i < selectList.size(); i++) {
						if(recFormat.get(j).get(0).equalsIgnoreCase(selectList.get(i).toString()))
							selList.add(selectList.get(i).toString());
					}
				}
			}
			Expression el =  pl.getWhere();
			//ArrayList<String> whereList = getValues(el);
			String[] whrClause = null;
			if(el!= null)
				whrClause = el.toString().split(" ");           
			
			ArrayList<ArrayList<String>> filterData = filterData(recFormat,selList,whrClause,tabData);

			System.out.println("-------------------------------- ");
			for (int i = 0; i < selList.size(); i++) {
				System.out.print(selList.get(i) + " ");
			}
			System.out.println("\n-------------------------------- ");

			for (int i = 0; i < filterData.size(); i++) {
				for (int j = 0; j < filterData.get(i).size(); j++) {
					System.out.print(filterData.get(i).get(j) + " ");
				}
				System.out.println(" ");
			}
			System.out.println("\n-------------------------------- ");
		}
	}

	public static ArrayList<ArrayList<String>> filterData(ArrayList<ArrayList<String>> recFormat,
			List<String> selectList, String[] whrClause, ArrayList<ArrayList<String>> tabData) {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<String>> filterData = new ArrayList<>();

		int whrColNum =0;
		if(whrClause!=null)
			for (int i = 0; i < recFormat.size(); i++) {
				if(recFormat.get(i).get(0).equalsIgnoreCase(whrClause[0]))
				{
					whrColNum = i;
				}
			}
		for (int i = 0; i < tabData.size(); i++) {
			boolean recMatch = false;
			ArrayList<String> tempList = new ArrayList<>();

			if(whrClause!= null)
			{
				switch (whrClause[1]) {
				case "=":
					if(tabData.get(i).get(whrColNum).equalsIgnoreCase(whrClause[2]))
						recMatch = true;
					break;
				case "!=":
					if(!tabData.get(i).get(whrColNum).equalsIgnoreCase(whrClause[2]))
						recMatch = true;
					break;
				case  "<>":
					if(!tabData.get(i).get(whrColNum).equalsIgnoreCase(whrClause[2]))
						recMatch = true;
					break;

				case  ">":
					if(Double.parseDouble(tabData.get(i).get(whrColNum))> Double.parseDouble(whrClause[2]))
						recMatch = true;
					break;

				case  "<":
					if(Double.parseDouble(tabData.get(i).get(whrColNum)) < Double.parseDouble(whrClause[2]))
						recMatch = true;
					break;	

				case  ">=":
					if(Double.parseDouble(tabData.get(i).get(whrColNum))>= Double.parseDouble(whrClause[2]))
						recMatch = true;
					break;

				case  "<=":
					if(Double.parseDouble(tabData.get(i).get(whrColNum)) <= Double.parseDouble(whrClause[2]))
						recMatch = true;
					break;	

				default:
					createError("Error in parsing relational operator: " + whrClause[1]);
					break;
				}
			}
			else
				recMatch = true;

			for (int j = 0; j < recFormat.size() && recMatch; j++) {
				for (int j2 = 0; j2 < selectList.size(); j2++) {

					if(recFormat.get(j).get(0).equalsIgnoreCase(selectList.get(j2).toString()))
					{
						tempList.add(tabData.get(i).get(j));
					}
				}
			}
			if(recMatch)
				filterData.add(tempList);
		}
		return filterData;
	}

	public static ArrayList<ArrayList<String>> getWhrColVal(String userInput) {
		// TODO Auto-generated method stub
		/*
		 * get column and corresponding value from where clause
		 */
		ArrayList<String> formattedQuery = new ArrayList<>();
		ArrayList<ArrayList<String>> whrColList = new ArrayList<>();
		String[] query = userInput.split("\\s");

		for (int i = 0; i < query.length; i++) {
			formattedQuery.add(query[i]);
		}

		if(formattedQuery.contains("where"))
		{

		}

		return whrColList;
	}

	private static ArrayList<String> getDispColList(String userInput, ArrayList<ArrayList<String>> recFormat) {
		// TODO Auto-generated method stub
		ArrayList<String> colList = new ArrayList<>();
		boolean allFlag = false;
		String[] query = userInput.split("\\s");
		if(query[1].equals("*"))
			allFlag = true;

		ArrayList<String> tempList = null;
		if(!allFlag)
		{
			tempList = new ArrayList<>();
			String[] query1 = query[1].split(",");
			for (int i = 1; i < query1.length ; i++) {
				tempList.add(query1[i]);
			}
		}
		for (int i = 0; i < recFormat.size(); i++) {

			if(allFlag)
				colList.add(recFormat.get(i).get(0));
			else
			{
				if(tempList.contains(recFormat.get(i).get(0)))
					colList.add(recFormat.get(i).get(0));	
			}
		}
		return colList;
	}

	public static void dropTable(String userInput) {
		// TODO Auto-generated method stub

		String[] str = userInput.split(" ");
		String tabName = "";

		try{
			tabName  = str[2];
			String path  = dirName + "user_data/" + tabName+ ".tbl";
			File tabFile = new File(path);
			File idxFile = new File(dirName+"user_data/"+tabName+".ndx");

			if(tabFile.exists())
			{
				Boolean res = tabFile.delete();
				idxFile.delete();

				if(res)
				{
					/*remove entries from meta-data tables*/
					deleteMeta(tabName);
					System.out.println("Table dropped");
				}
				else
					createError("Error occured while dropping table");
			}
			else
			{
				createError("Meta_data table deletion not allowed.");
				
			}
		}
		catch(ArrayIndexOutOfBoundsException e)
		{
			System.out.println("Invalid Query");
		}
	}

	public static void deleteMeta(String tabName) {
		// TODO Auto-generated method stub
		/*
		 * delete entries from meta_data tables once tables dropped
		 */
		delete("delete from mydatabase_tables where table_name =" + tabName);
		delete("delete from mydatabase_columns where table_name =" + tabName);
	}

	public static void delete(String userInput) {
		// TODO Auto-generated method stub
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		try {
			statement = pm.parse(new StringReader(userInput));
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			createError("You have an error in your SQL syntax");
		}

		if (statement instanceof Delete)
		{	
			Delete delStatement = (Delete) statement;
			String tabName = delStatement.getTable().getName();

			Expression el =   delStatement.getWhere();
			//ArrayList<String> whereList = getValues(el);
			String[] whrClause = null;
			if(el!= null)
			{
				whrClause = el.toString().split(" ");
			}

			if(whrClause != null)
			{
				/*
				 * deletion based on where clause
				 */
				ArrayList<String> selList = new ArrayList<>();
				ArrayList<ArrayList<String>> tabData;
				try {
					tabData = readTable(tabName);
					int recCnt = tabData.size();
					ArrayList<ArrayList<String>> recFormat = getRecFormat(tabName);

					for (int i = 0; i < recFormat.size(); i++) {
						selList.add(recFormat.get(i).get(0));
					}

					ArrayList<ArrayList<String>> filterData = filterData(recFormat,selList,whrClause,tabData);

					int delRecCnt = filterData.size();

					for (int i = 0; i < filterData.size(); i++) {
						for (int j = 0; j < tabData.size(); j++) {
							if(tabData.get(j).get(0).equalsIgnoreCase(filterData.get(i).get(0)))
								tabData.remove(j);
						}
					}
					reorderTable(tabName,tabData);	

					int newRecCnt = recCnt - delRecCnt;

					update("update mydatabase_tables set record_count = " + newRecCnt + " where table_name =" + tabName);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else
			{   /*
			 * delete all data from table
			 * where condition not provided
			 */

				ArrayList<ArrayList<String>> tabData = new ArrayList<>();
				reorderTable(tabName,tabData);

				//mark number of rows in meta table as 0
				try {
					update("update mydatabase_tables set record_count = 0 where table_name =" + tabName);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static void update(String userInput) throws Exception {
		// TODO Auto-generated method stub
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		try {
			statement = pm.parse(new StringReader(userInput));
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			createError("You have an error in your SQL syntax");
		}

		if (statement instanceof Update)
		{	
			Update updatestatement = (Update) statement;
			String tabName = updatestatement.getTable().getName();
			List<Column> columnlist = updatestatement.getColumns();
			Expression el =   updatestatement.getWhere();

			List<Expression> ex = updatestatement.getExpressions();
			ArrayList<ArrayList<String>> updColValList = new ArrayList<>();

			if(columnlist.size() == ex.size())
				for (int i = 0; i < columnlist.size(); i++) {
					ArrayList<String> tempList = new ArrayList<>();
					tempList.add(columnlist.get(i).getColumnName());
					tempList.add(ex.get(i).toString());

					updColValList.add(tempList);
				}
			else
			{
				createError("Error in delete query.");
				return;
			}
			//ArrayList<String> whereList = getValues(el);
			String[] whrClause = null;
			ArrayList<ArrayList<String>> recFormat = getRecFormat(tabName);
			if(el!= null)
			{
				whrClause = el.toString().split(" ");
			}

			if(whrClause!=null)
			{
				/*
				 * deletion based on where clause
				 */
				ArrayList<String> selList = new ArrayList<>();
				ArrayList<ArrayList<String>> tabData;
				try {
					tabData = readTable(tabName);


					for (int i = 0; i < recFormat.size(); i++) {
						selList.add(recFormat.get(i).get(0));
					}

					ArrayList<ArrayList<String>> filterData = filterData(recFormat,selList,whrClause,tabData);

					int updRecCnt = filterData.size();

					for (int i = 0; i < filterData.size(); i++) {
						for (int j = 0; j < tabData.size(); j++) {
							if(tabData.get(j).get(0).equalsIgnoreCase(filterData.get(i).get(0)))
								tabData.remove(j);
						}
					}

					for (int i = 0; i < filterData.size(); i++) {

						for (int j = 0; j < updColValList.size(); j++) {
							int colNum = getColNum(updColValList.get(j).get(0),recFormat);
							filterData.get(i).set(colNum, updColValList.get(j).get(1));
						}
					}

					for (int i = 0; i < filterData.size(); i++) {
						tabData.add(filterData.get(i));
					}

					reorderTable(tabName,tabData);	

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			else
			{
				/*
				 * update all records
				 */

				ArrayList<ArrayList<String>> filterData = readTable(tabName);

				for (int i = 0; i < filterData.size(); i++) {

					for (int j = 0; j < updColValList.size(); j++) {
						int colNum = getColNum(updColValList.get(j).get(0),recFormat);
						filterData.get(i).set(colNum, updColValList.get(j).get(1));
					}
				}
				reorderTable(tabName,filterData);	
			}
		}
	}

	private static int getColNum(String colName, ArrayList<ArrayList<String>> recFormat) {
		// TODO Auto-generated method stub

		for (int i = 0; i < recFormat.size(); i++) {
			if(recFormat.get(i).get(0).equalsIgnoreCase(colName))
				return i;
		}
		return -1;
	}

	private static void createTable(String userInput) {
		// TODO Auto-generated method stub

		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		try {
			statement = pm.parse(new StringReader(userInput));
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		java.util.List<ColumnDefinition> colList = null;
		Table tabName = null;
		if (statement instanceof CreateTable) {
			CreateTable createTabSt = (CreateTable) statement;
			tabName = createTabSt.getTable();
			colList = createTabSt.getColumnDefinitions();	

			/*
			 * if table name and column details are present
			 */
			if(tabName != null && colList != null)
			{
				/*
				 * Do validations and then create a table file and information
				 * in meta-data tables
				 */

				File tabFile = null;
				File idxFile = null;
				if (tabName.getName().equalsIgnoreCase("mydatabase_tables") || tabName.getName().equalsIgnoreCase("mydatabase_columns"))
				{
					tabFile = new File(dirName + "catalog/" + tabName.getName()+ ".tbl");
					idxFile = new File(dirName + "catalog/" + tabName.getName()+ ".ndx");
				}
				else
				{
					tabFile = new File(dirName + "user_data/" + tabName.getName()+ ".tbl");
					idxFile = new File(dirName + "user_data/" + tabName.getName()+ ".ndx");
				}
				if(!tabFile.exists())
				{
					try{
						tabFile.createNewFile();
						idxFile.createNewFile();
						RandomAccessFile rf = new RandomAccessFile(tabFile, "rw");
						rf.setLength(pageSize);
						rf.seek(0);
						rf.writeByte(13); //type of page 0x0D for leaf table page
						rf.writeByte(0); //number of records in page
						rf.writeShort(511); //start context, initially it points to end of page
						rf.writeInt(-1);
						rf.close();

						rf = new RandomAccessFile(idxFile, "rw");
						rf.setLength(pageSize);
						rf.seek(0);
						rf.writeByte(10); //type of page 0x0a for leaf index page
						rf.writeByte(0); //number of records in page
						rf.writeShort(511); //start context, initially it points to end of page
						rf.writeInt(-1); //right sibling/child pointer, initialize to -1
						rf.close();

						//get latest row-id for next insertion
						int rowid = getRowId("mydatabase_tables");						
						int avg_len = getAvgLen(colList);

						//insert an entry in mydatabase_tables
						String insertQuery = "insert into mydatabase_tables (rowid,table_name,record_count,avg_length) values ( "
								+ rowid + "," + tabName.getName() + ",0," + avg_len + ")";

						Insert(insertQuery,"");

						String colNameList = "(";
						String dataTypeList = "(";
						rowid = getRowId("mydatabase_columns");
						int ordPos = 1;
						for (int i = 0; i < colList.size(); i++) {

							if(i==0 && !colList.get(i).getColDataType().toString().equalsIgnoreCase("int"))
							{
								createError("First column of table must be integer and primary key");
								return;
							}
							String primKey ="no";
							String notNull = "yes";
							try{
								if(colList.get(i).getColumnSpecStrings().get(0).toString().equalsIgnoreCase("primary"))
									primKey = "pri";
								else
									if (colList.get(i).getColumnSpecStrings().get(0).toString().equalsIgnoreCase("not"))
										notNull = "no";
							}
							catch (Exception e)
							{

							}

							insertQuery = "insert into mydatabase_columns (rowid,table_name,column_name,"
									+ "data_type,ordinal_position,primary_key,is_nullable) values(" + rowid +"," + tabName.getName()
									+ "," + colList.get(i).getColumnName() + "," + colList.get(i).getColDataType() +","
									+ ordPos
									+ "," + primKey + "," + notNull + ")";
							rowid++;ordPos++;

							Insert(insertQuery,"");
						}

						//insert entries in mydatabase_columns table
					}
					catch(Exception e)
					{
						e.printStackTrace();
						createError("Error in creation of meta data tables");
					}

					/*meta data entry*/
				}
				else
					createError("Table already exist");

			}
			else
				createError("Error in table creation");
		}
	}

	private static int getRowId(String tabName) {
		// TODO Auto-generated method stub

		int numRows = 0;
		try {
			ArrayList<ArrayList<String>> tabData =  readTable(tabName);

			numRows = tabData.size() + 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return numRows;
	}

	private static int getAvgLen(java.util.List<ColumnDefinition> colList) {
		// TODO Auto-generated method stub
		int avg_len = 0;
		for (int i = 0; i < colList.size(); i++) {
			for (int j = 0; j < dataTypeList.size(); j++) {
				ArrayList<String> tempList = dataTypeList.get(j);
				if (tempList.get(0).equalsIgnoreCase(colList.get(i).getColDataType().getDataType()))
				{
					avg_len = avg_len + Integer.parseInt(tempList.get(1));
				}
			}
		}
		return avg_len;
	}

	private static void showTables() throws IOException {
		// TODO Auto-generated method stub

		try {
			select("select table_name from mydatabase_tables");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void showHelp() {
		// TODO Auto-generated method stub

		System.out.println("----------------------Help------------------------------");
		System.out.println("Supported Commands List");
		System.out.println("1.CREATE TABLE");
		System.out.println("2.DROP TABLE");
		System.out.println("3.SHOW TABLE");
		System.out.println("4.INSERT INTO TABLE (COL1,COL2...) VALUES(VAL1,VAL2,...)");
		System.out.println("5.UPDATE TABLE TABLE_NAME SET COL1 = VAL1 WHERE COL2 = VAL2");
		System.out.println("6.DELETE FROM TABLE WHERE COL1 = VAL1");
		System.out.println("7.SELECT-FROM-WHERE");
		System.out.println("8.EXIT");
		System.out.println("Note: Date Format: (yyyy-MM-dd)");
		System.out.println("----------------------------------------------------------");
	}

	private static void dispWelcomeScreen() {
		// TODO Auto-generated method stub
		/*
		 * display a welcome message at the start of the application
		 */
		System.out.println("**********Welcome to myDatabase*********");
		System.out.println("**********Version V0.1******************");
		System.out.println("***********©2016 Nilesh Pharate*********");
	}

	private static void createError(String err) {
		// TODO Auto-generated method stub

		System.out.println("***Error: " +err + "***");

	}

	private static void createDirStruct() {
		// TODO Auto-generated method stub

		File fl = new File(dirName);

		if(!fl.exists())
		{
			try {
				/*
				 * create all directories inside current path
				 */
				new File(dirName + "\\catalog").mkdirs();
				new File(dirName + "\\user_data").mkdir();

				tables = new RandomAccessFile(dirName + "\\catalog\\myDatabase_tables.tbl","rw");
				columns = new RandomAccessFile(dirName + "\\catalog\\myDatabase_columns.tbl","rw");

				tables.setLength(pageSize);
				tables.seek(0);
				tables.writeByte(13); //type of page 0x0D for leaf table page
				tables.writeByte(0); //number of records in page
				tables.writeShort(511); //start context, initially it points to end of page
				tables.writeInt(-1);
				tables.close();

				columns.setLength(pageSize);
				columns.seek(0);
				columns.writeByte(13); //type of page 0x0D for leaf table page
				columns.writeByte(0); //number of records in page
				columns.writeShort(511); //start context, initially it points to end of page
				columns.writeInt(-1);
				columns.close();

				Insert("insert into mydatabase_tables (rowid,table_name,record_count,avg_length) "
						+ "values (1,mydatabase_tables,2,30)","");
				Insert("insert into mydatabase_tables (rowid,table_name,record_count,avg_length) "
						+ "values (2,mydatabase_columns,10,35)","");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			try {
				tables = new RandomAccessFile(dirName + "\\catalog\\myDatabase_tables.tbl","rw");
				columns = new RandomAccessFile(dirName + "\\catalog\\myDatabase_columns.tbl","rw");

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void initCatalogTables() throws IOException {
		// TODO Auto-generated method stub

		tables.seek(0);
		/*
		 * table page header data
		 */
		tables.writeByte(5);
		tables.writeByte(2);

		tables.seek(pageHeadLen);

		//first record
		tables.writeInt(1);
		tables.writeInt(new String("mydatabase_tables").length());
		tables.writeBytes("mydatabase_tables");
		tables.writeInt(2);
		tables.writeInt(26);

		//second record
		tables.writeInt(2);
		tables.writeInt(new String("mydatabase_columns").length());
		tables.writeBytes("mydatabase_columns");
		tables.writeInt(10);
		tables.writeInt(30);

		/*
		 * column page header data
		 */

		columns.seek(0);
		columns.seek(pageHeadLen);
		columns.writeInt(1);
		columns.writeInt(new String("mydatabase_tables").length());
		columns.writeBytes("mydatabase_tables");
		columns.writeInt(new String("rowid").length());
		columns.writeBytes("rowid");
		columns.writeInt(new String("int").length());
		columns.writeBytes("int");
		columns.writeInt(1);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(2);
		columns.writeInt(new String("mydatabase_tables").length());
		columns.writeBytes("mydatabase_tables");
		columns.writeInt(new String("table_name").length());
		columns.writeBytes("table_name");
		columns.writeInt(new String("text").length());
		columns.writeBytes("text");
		columns.writeInt(2);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(3);
		columns.writeInt(new String("mydatabase_tables").length());
		columns.writeBytes("mydatabase_tables");
		columns.writeInt(new String("record_count").length());
		columns.writeBytes("record_count");
		columns.writeInt(new String("int").length());
		columns.writeBytes("int");
		columns.writeInt(3);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(4);
		columns.writeInt(new String("mydatabase_tables").length());
		columns.writeBytes("mydatabase_tables");
		columns.writeInt(new String("avg_length").length());
		columns.writeBytes("avg_length");
		columns.writeInt(new String("smallint").length());
		columns.writeBytes("smallint");
		columns.writeInt(4);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(5);
		columns.writeInt(new String("mydatabase_columns").length());
		columns.writeBytes("mydatabase_columns");
		columns.writeInt(new String("rowid").length());
		columns.writeBytes("rowid");
		columns.writeInt(new String("int").length());
		columns.writeBytes("int");
		columns.writeInt(5);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(6);
		columns.writeInt(new String("mydatabase_columns").length());
		columns.writeBytes("mydatabase_columns");
		columns.writeInt(new String("table_name").length());
		columns.writeBytes("table_name");
		columns.writeInt(new String("text").length());
		columns.writeBytes("text");
		columns.writeInt(6);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(7);
		columns.writeInt(new String("mydatabase_columns").length());
		columns.writeBytes("mydatabase_columns");
		columns.writeInt(new String("column_name").length());
		columns.writeBytes("column_name");
		columns.writeInt(new String("text").length());
		columns.writeBytes("text");
		columns.writeInt(7);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(8);
		columns.writeInt(new String("mydatabase_columns").length());
		columns.writeBytes("mydatabase_columns");
		columns.writeInt(new String("data_type").length());
		columns.writeBytes("data_type");
		columns.writeInt(new String("text").length());
		columns.writeBytes("text");
		columns.writeInt(8);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(9);
		columns.writeInt(new String("mydatabase_columns").length());
		columns.writeBytes("mydatabase_columns");
		columns.writeInt(new String("ordinal_position").length());
		columns.writeBytes("ordinal_position");
		columns.writeInt(new String("tinyint").length());
		columns.writeBytes("tinyint");
		columns.writeInt(9);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

		columns.writeInt(10);
		columns.writeInt(new String("mydatabase_columns").length());
		columns.writeBytes("mydatabase_columns");
		columns.writeInt(new String("is_nullable").length());
		columns.writeBytes("is_nullable");
		columns.writeInt(new String("text").length());
		columns.writeBytes("text");
		columns.writeInt(10);
		columns.writeInt(new String("no").length());
		columns.writeBytes("no");

	}

	public static RandomAccessFile createFile(String fName) {
		// TODO Auto-generated method stub
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(fName, "rw");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();	
		}	
		return file;
	}

	public static void Insert(String userInput, String reFlag) throws Exception
	{
		//System.out.println("processing insert query:" + userInput);

		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		try {
			statement = pm.parse(new StringReader(userInput));
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			createError("You have an error in your SQL syntax");
		}

		if (statement instanceof Insert)
		{
			Insert is = (Insert) statement;
			Table tabName = is.getTable();

			// ItemsListVisitor values = is.getItemsList();
			@SuppressWarnings("unchecked")
			java.util.List<Column> columns = is.getColumns();
			ExpressionList ex = (ExpressionList) is.getItemsList();
			ArrayList<String> values = getValues(ex);

			Boolean metaFlag = false;
			if(is.isUseValues())
			{   
				if (true)
				{   File fl = null;
				if(tabName.getName().equalsIgnoreCase("mydatabase_tables") || 
						tabName.getName().equalsIgnoreCase("mydatabase_columns"))
				{
					fl = new File(dirName + "catalog/" + tabName.getName() +".tbl");
					metaFlag =  true;
				}
				else
					fl = new File(dirName + "user_data/" + tabName.getName() +".tbl");

				if(!fl.exists())
				{
					createError(tabName.getName() + " does not exist.");
					return;
				}
				RandomAccessFile rfl;
				try {
					rfl = new RandomAccessFile(fl, "rw");

					ArrayList<ArrayList <String>> recordFormat = getRecFormat(tabName.getName());

					ArrayList<ArrayList <String>> columnValList = getColValList(columns,values,recordFormat,tabName);

					if(columnValList==null || recordFormat == null)
					{
						createError("Error in record insertion.");
						rfl.close();
						return;
					}

					//System.out.println(columnValList);

					int columnCnt = 0;
					ArrayList<Integer> recordSerialTypes = new ArrayList<>();
					int payLoadLen = 0;
					for (int i = 0; i < columnValList.size(); i++) {
						columnCnt ++;

						if(columnValList.get(i).get(3).equalsIgnoreCase("12"))
						{
							payLoadLen = payLoadLen + Integer.parseInt(columnValList.get(i).get(2));
							recordSerialTypes.add(Integer.parseInt(columnValList.get(i).get(3)) 
									+ Integer.parseInt(columnValList.get(i).get(2)));
						}
						else
						{
							payLoadLen = payLoadLen + Integer.parseInt(columnValList.get(i).get(2));
							recordSerialTypes.add(Integer.parseInt(columnValList.get(i).get(2)));
						}
					}

					//number of columns indicator 1 byte, one byte each for each column data 
					//type serial code
					payLoadLen = payLoadLen + 1 + recordSerialTypes.size();

					int rowid = getRowId(tabName.getName()); //get this one from record count of table from meta-data
					long pageCnt = getPageCnt(rfl,payLoadLen);

					long pageStartOffset = (pageCnt - 1) * pageSize;

					rfl.seek(pageStartOffset + 1);
					int reccnt = rfl.readUnsignedByte();
					int startContentOffset = rfl.readUnsignedShort();

					rfl.seek(pageStartOffset + 8);

					for (int i = 0; i < reccnt; i++) {
						rfl.seek(rfl.getFilePointer() + 2);
					}

					// -6 = 2 byte for pay-load length, 4 bytes for row-id
					int newStartContentOffset = startContentOffset - payLoadLen - 6;

					long currRecArrLoc = rfl.getFilePointer();

					//getPageaddr(rfl,currRecArrLoc,payLoadLen);

					//space available for this record in current page
					if(newStartContentOffset - currRecArrLoc > 2)
					{
						//update content offset

						rfl.seek(pageStartOffset+ 2);
						rfl.writeShort(newStartContentOffset);

						//place current record location in location array
						rfl.seek(currRecArrLoc);
						rfl.writeShort(newStartContentOffset);

						//go to start of record storage
						rfl.seek(newStartContentOffset);
						//start writing data
						//pay-load length
						rfl.writeShort(payLoadLen);
						rfl.writeInt(rowid); //row id
						rfl.writeByte(columnCnt);
						for (int i = 0; i < recordSerialTypes.size(); i++) {
							rfl.writeByte(recordSerialTypes.get(i));
						}

						int keyVal = 0;
						//write data of record
						for (int i = 0; i < columnValList.size(); i++) {

							//backup the key for record
							if(i==0)
							{
								try{
									keyVal = Integer.parseInt(columnValList.get(i).get(1));
								}
								catch(Exception e)
								{
									e.printStackTrace();
								}
							}
							switch (columnValList.get(i).get(3)) {
							case "4":
								rfl.writeByte(Integer.parseInt(columnValList.get(i).get(1)));
								break;

							case "5":
								rfl.writeShort(Integer.parseInt(columnValList.get(i).get(1)));
								break;

							case "6":
								rfl.writeInt(Integer.parseInt(columnValList.get(i).get(1)));
								break;

							case"7":
								rfl.writeDouble(Integer.parseInt(columnValList.get(i).get(1)));
								break;
							case"8":
								rfl.writeInt(Integer.parseInt(columnValList.get(i).get(1)));
								break;
							case "9":
								rfl.writeDouble(Integer.parseInt(columnValList.get(i).get(1)));
								break;
							case "10":
								long date_long = getDateLong(columnValList.get(i).get(1));
								rfl.writeLong(date_long);
								break;
							case "11":
								date_long = getDateLong(columnValList.get(i).get(1));
								rfl.writeLong(date_long);
								break;
							case "12":
								rfl.writeBytes(columnValList.get(i).get(1));
								break;

							default:
								createError("Error while inserting record in page of file");
								break;
							}
						}

						rfl.seek(pageStartOffset+1);
						rfl.write(reccnt+1);

						if((!reFlag.equalsIgnoreCase("reorder") && 
								!tabName.getName().equalsIgnoreCase("myDatabase_tables") && 
								!tabName.getName().equalsIgnoreCase("myDatabase_columns")))
							insertIdxEntry(tabName,keyVal,newStartContentOffset);
					}
					else
					{
						//current page is full, go to next page
						//need to loop in
						System.out.println("Page full");
					}
					rfl.close();

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				}
				else
					createError("You have an error in your SQL syntax.");
			}
			else
			{
				createError("Provide columns for insert query.");
			}
			if(!reFlag.equalsIgnoreCase("reorder"))
			{
				reorderTable(tabName.getName(),null);
				/*
				 * increment record count in mydatabase_tables
				 */
				ArrayList<ArrayList<String>> data = readTable(tabName.getName());
				update("update mydatabase_tables set record_count = " + data.size() + " where table_name = " + tabName.getName());
			}

		}
	}

	public static void insertIdxEntry(Table tabName, int keyVal, int keyRecAddr) {
		// TODO Auto-generated method stub
		/*
		 * newStartContentOffset = offset of record with keyVal,in data file
		 */

		File fl = null;
		if(tabName.getName().equalsIgnoreCase("mydatabase_tables") || 
				tabName.getName().equalsIgnoreCase("mydatabase_columns"))
		{
			fl = new File(dirName + "catalog/" + tabName.getName() +".ndx");
		}
		else
			fl = new File(dirName + "user_data/" + tabName.getName() +".ndx");

		try {
			RandomAccessFile idxFl = new RandomAccessFile(fl, "rw");

			//go to start of index file

			int pageCount = (int) idxFl.length()/pageSize;

			idxFl.seek((pageCount - 1)*pageSize);

			//idxFl.seek(0);
			int pageType =0;

			pageType = idxFl.readUnsignedByte();
			//long writePgAddr = 0;
			int newPageAddress =(pageCount - 1)*pageSize;

			ArrayList<Integer> parentNodeAddrList = new ArrayList<>();
			//parentNodeAddrList.add(0);
			while(pageType != 10)
			{
				int numRec = idxFl.readUnsignedByte();
				/*
				 * for first pointer
				 * 0 location - dummy value
				 * 1 location - pointer
				 * else
				 * 0 location - value
				 * 1 location - pointer
				 */
				ArrayList<ArrayList<Integer>> internalNodeData = new ArrayList<>();

				readInternalNode(idxFl,newPageAddress,internalNodeData);

				//get pointer from internal node to next node based on key value
				newPageAddress = getTreePageAddr(keyVal,internalNodeData);

				//back up address of parent node for backtracking and upward overflow check
				parentNodeAddrList.add(newPageAddress);
				idxFl.seek(newPageAddress);
				pageType = idxFl.readUnsignedByte();

			}

			if(pageType == 10)
			{   
				insertDataToPage(idxFl,newPageAddress,keyVal,keyRecAddr,parentNodeAddrList);

			}
			else
				System.out.println("error in finding page for index entry");

			idxFl.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void readInternalNode(RandomAccessFile idxFl, int newPageAddress, ArrayList<ArrayList<Integer>> internalNodeData ) throws IOException {

		idxFl.seek(newPageAddress);
		idxFl.seek(newPageAddress+1); //skip page type
		int numRec = idxFl.read();
		idxFl.seek(idxFl.getFilePointer()+6); // go to key val

		ArrayList<Integer>  tempList = new ArrayList<>();

		tempList.add(0); //dummy value
		tempList.add(idxFl.readInt()); //first pointer

		if(numRec > 0)
			internalNodeData.add(tempList);

		for (int i = 0; i < numRec; i++) {
			tempList = new ArrayList<>();

			tempList.add(idxFl.readInt()); //key value
			tempList.add(idxFl.readInt()); // pointer
			internalNodeData.add(tempList);
		}
	}

	private static void insertDataToPage(RandomAccessFile idxFl, int newPageAddress, 
			int keyVal, int keyRecAddr,
			ArrayList<Integer> parentNodeAddrList) throws Exception {
		// TODO Auto-generated method stub

		//insert entry to leaf node, page type =10
		idxFl.seek(newPageAddress);
		int pagetype = idxFl.readUnsignedByte();
		int recCnt = idxFl.readUnsignedByte();
		idxFl.seek(idxFl.getFilePointer()+6);
		ArrayList<ArrayList<Integer>> leafPageData = new ArrayList<>();
		ArrayList<Integer> tempList = new ArrayList<>();

		tempList.add(keyVal);
		tempList.add(keyRecAddr);
		leafPageData.add(tempList);

		if(pagetype == 10)
		{
			for (int i = 0; i < recCnt; i++) {
				tempList = new ArrayList<>();

				tempList.add(idxFl.readInt());
				tempList.add(idxFl.readInt());

				leafPageData.add(tempList);
			}

			//sort leaf page date based on key
			sortKeyData(leafPageData);

			writeIdxPage(idxFl,newPageAddress,leafPageData,pagetype);
			if(leafPageData.size() > bPlusTreeOrder - 1) //leaf order 4
				splitPageLeaf(idxFl,newPageAddress,parentNodeAddrList);
		}
		
	}

	private static void sortInternalNodeData(ArrayList<ArrayList<Integer>> internalNodeData) {
		// TODO Auto-generated method stub

		ArrayList<Integer> tempList = new ArrayList<>();

		for (int c = 1 ; c < internalNodeData.size(); c++)
		{
			for (int d = 1 ; d < internalNodeData.size() - c - 1; d++)
			{
				if (internalNodeData.get(d).get(0) > internalNodeData.get(d+1).get(0)) /* For decreasing order use < */
				{
					tempList = internalNodeData.get(d);
					internalNodeData.set(d, internalNodeData.get(d+1));
					internalNodeData.set(d+1, tempList);
				}
			}
		}

	}

	public static void splitPageLeaf(RandomAccessFile idxFl, int pageAddr, ArrayList<Integer> parentNodeAddrList) throws Exception {
		// TODO Auto-generated method stub

		idxFl.seek(pageAddr);
		int pageType = idxFl.readByte(); // 10- leaf, 02- interior

		ArrayList<ArrayList<Integer>> pageData = new ArrayList<>();

		if(pageType ==10)
		{
			pageData = readPageData(idxFl,pageAddr);

			idxFl.seek(pageAddr);
			idxFl.seek(pageAddr+1);
			idxFl.write(2);
			idxFl.seek(pageAddr+4);
			int sibaddr = idxFl.readInt();

			idxFl.seek(pageAddr+24);

			//delete last 3 records
			for (int i = 0; i <= 3; i++) {
				idxFl.writeInt(0);
				idxFl.writeInt(0);
			}

			idxFl.setLength(idxFl.length()+pageSize);

			int pagenum =(int) idxFl.length()/pageSize;

			int newPageAddress = (pagenum-1)*pageSize;
			idxFl.seek(pageAddr+4);
			idxFl.writeInt(newPageAddress);

			idxFl.seek(newPageAddress);

			initPage(idxFl,newPageAddress,10,sibaddr,3);

			idxFl.seek(newPageAddress + 8);

			//start writing data from record 3 to 5

			for (int i = 2; i < pageData.size(); i++) {
				idxFl.writeInt(pageData.get(i).get(0)); // key
				idxFl.writeInt(pageData.get(i).get(1)); //record address
			}

			if(parentNodeAddrList.size() >= 1)
			{
				int parentNodeArr = parentNodeAddrList.get(parentNodeAddrList.size()-1);

				parentNodeAddrList.remove(parentNodeAddrList.size()-1);
				
				insertDataToInternal(idxFl,parentNodeArr,pageData.get(2).get(0),
						pageAddr,newPageAddress,parentNodeAddrList);
			}
			else
			{
				//no parent node available,create new page
				idxFl.setLength(idxFl.length()+pageSize);

				int parentNodeArr = (int)((idxFl.length()/pageSize) - 1) * pageSize;
				initPage(idxFl, parentNodeArr, 2, 0, 0);

				insertDataToInternal(idxFl,parentNodeArr,pageData.get(2).get(0),
						pageAddr,newPageAddress,parentNodeAddrList);
			}
		}
		else
		{    //internal page

			readInternalNode(idxFl, pageAddr, pageData);

			idxFl.setLength(idxFl.length()+pageSize);

			int newPageAddr =(int) ((idxFl.length()/pageSize) - 1) * pageSize;

			idxFl.seek(newPageAddr);
			idxFl.writeByte(2); //page type
			idxFl.writeByte(2); //number of records

			idxFl.seek(newPageAddr+8);
			idxFl.writeInt(pageData.get(3).get(1)); //first pointer

			idxFl.writeInt(pageData.get(4).get(0)); //key
			idxFl.writeInt(pageData.get(4).get(1));

			idxFl.writeInt(pageData.get(5).get(0)); //key
			idxFl.writeInt(pageData.get(5).get(1));

			idxFl.seek(pageAddr +1);
			idxFl.writeByte(2);

			idxFl.seek(pageAddr +8 + 20);

			idxFl.write(0);
			idxFl.write(0);

			idxFl.write(0);
			idxFl.write(0);

			idxFl.write(0);
		}

	}

	private static void insertDataToInternal(RandomAccessFile idxFl, 
			int pageAddr, 
			Integer key, int childPageAddr1,
			int childPageAddr2,
			ArrayList<Integer> parentNodeAddrList) throws IOException {
		// TODO Auto-generated method stub

		//internal page

		if(parentNodeAddrList.size() == 0)
		{
			idxFl.seek(pageAddr+1);
			idxFl.writeByte(1); //number of records
			idxFl.writeShort(0);
			idxFl.writeInt(-1);
			
			idxFl.writeInt(childPageAddr1);
			idxFl.writeInt(key);
			idxFl.writeInt(childPageAddr2);
			return;
		}

		else
		{
			//ignore childPageAddr1 as it is already there in node
			ArrayList<ArrayList<Integer>> internalNodeData = new ArrayList<>();
			readInternalNode(idxFl, pageAddr, internalNodeData);

			ArrayList<Integer> tempList = new ArrayList<>();
			tempList.add(key);
			tempList.add(childPageAddr2);

			internalNodeData.add(tempList);

			sortInternalNodeData(internalNodeData);
			writeIdxPage(idxFl,pageAddr,internalNodeData,2);

			if(internalNodeData.size() -1  >= bPlusTreeOrder ) //internal node order 5, -1 to handle first pointer with dummy value
				//4 keys allowed with 5 pointers
				splitPageInternal(idxFl, pageAddr, parentNodeAddrList);;
		}



	}

	private static void splitPageInternal(RandomAccessFile idxFl, int pageAddr, ArrayList<Integer> parentNodeAddrList) throws IOException {
		// TODO Auto-generated method stub

		ArrayList<ArrayList<Integer>> pageData = new ArrayList<>();
		readInternalNode(idxFl, pageAddr, pageData);

		idxFl.setLength(idxFl.length()+pageSize);

		int newPageAddr =(int) ((idxFl.length()/pageSize) - 1) * pageSize;

		idxFl.seek(newPageAddr);
		idxFl.writeByte(2); //page type
		idxFl.writeByte(3); //number of records

		idxFl.seek(newPageAddr+8);
		idxFl.writeInt(pageData.get(2).get(1)); //first pointer

		idxFl.writeInt(pageData.get(3).get(0)); //key
		idxFl.writeInt(pageData.get(3).get(1));

		idxFl.writeInt(pageData.get(4).get(0)); //key
		idxFl.writeInt(pageData.get(4).get(1));

		idxFl.writeInt(pageData.get(5).get(0)); //key
		idxFl.writeInt(pageData.get(5).get(1));

		idxFl.seek(pageAddr +1);
		idxFl.writeByte(2);

		idxFl.seek(pageAddr +8 + 20);

		idxFl.write(0);
		idxFl.write(0);

		idxFl.write(0);
		idxFl.write(0);

		idxFl.write(0);

		if(parentNodeAddrList.size()==0)
		{
			//create new node
			idxFl.setLength(idxFl.length()+pageSize);

			int newNodeAddr =(int) ((idxFl.length()/pageSize) - 1) * pageSize;
			initPage(idxFl, newNodeAddr, 2, 0, 0);
			
			insertDataToInternal(idxFl, newNodeAddr, pageData.get(3).get(0),
					pageAddr, newPageAddr, parentNodeAddrList);
			return;
		}
		else
		{
			int parentNodeAddr =  parentNodeAddrList.get(parentNodeAddrList.size()-1);
			parentNodeAddrList.remove(parentNodeAddrList.size()-1);

			insertDataToInternal(idxFl, parentNodeAddr, pageData.get(3).get(0),
			pageAddr, newPageAddr, parentNodeAddrList);

		}

	}

	private static void initPage(RandomAccessFile idxFl, int pagestartAddr, int pageType, int sibaddr,int recnum) throws IOException {
		// TODO Auto-generated method stub

		idxFl.seek(pagestartAddr);
		idxFl.writeByte(pageType);
		idxFl.writeByte(recnum); //number of records in page
		idxFl.writeInt(sibaddr);

	}

	private static ArrayList<ArrayList<Integer>> readPageData(RandomAccessFile idxFl, int pageAddr) throws IOException {
		// TODO Auto-generated method stub

		ArrayList<ArrayList<Integer>> pageData = new ArrayList<>();
		idxFl.seek(pageAddr+1);
		int recCnt = idxFl.readUnsignedByte();

		idxFl.seek(pageAddr+8);
		for (int i = 0; i < recCnt; i++) {
			ArrayList<Integer> tempList = new ArrayList<>();
			tempList.add(idxFl.readInt());
			tempList.add(idxFl.readInt());
			pageData.add(tempList);
		}


		return pageData;
	}

	public static void writeIdxPage(RandomAccessFile idxFl, int newPageAddress,
			ArrayList<ArrayList<Integer>> leafPageData, int pagetype) {
		// TODO Auto-generated method stub

		try {
			idxFl.seek(newPageAddress);
			idxFl.seek(newPageAddress+1);
			idxFl.writeByte(leafPageData.size());
			idxFl.seek(idxFl.getFilePointer()+6);

			for (int i = 0; i < leafPageData.size(); i++) {
				idxFl.writeInt(leafPageData.get(i).get(0));
				idxFl.writeInt(leafPageData.get(i).get(1));
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void sortKeyData(ArrayList<ArrayList<Integer>> leafPageData) {
		// TODO Auto-generated method stub

		ArrayList<Integer> tempList = new ArrayList<>();

		for (int c = 0 ; c < leafPageData.size(); c++)
		{
			for (int d = 0 ; d < leafPageData.size() - c - 1; d++)
			{
				if (leafPageData.get(d).get(0) > leafPageData.get(d+1).get(0)) /* For decreasing order use < */
				{
					tempList = leafPageData.get(d);
					leafPageData.set(d, leafPageData.get(d+1));
					leafPageData.set(d+1, tempList);
				}
			}
		}
	}


	public static int getTreePageAddr(int keyVal, ArrayList<ArrayList<Integer>> internalNodeData) {
		// TODO Auto-generated method stub

		if(keyVal< internalNodeData.get(1).get(0))
			return internalNodeData.get(0).get(1);
		else
			if(keyVal>= internalNodeData.get(internalNodeData.size()-1).get(0))
				return internalNodeData.get(internalNodeData.size()-1).get(1);

		for (int i = 1; i < internalNodeData.size() - 1; i++) {

			if(keyVal >= internalNodeData.get(i).get(0) && keyVal < internalNodeData.get(i).get(0))
				return internalNodeData.get(i).get(1);
		}
		return 0;
	}

	public static void reorderTable(String tabName, ArrayList<ArrayList<String>> tabData2) {
		// TODO Auto-generated method stub

		try {
			ArrayList<ArrayList<String>> tabData = null;
			if( tabData2 ==null )
				tabData = readTable(tabName);
			else
				tabData = tabData2;
			sort(tabData);

			File fl = null;
			if(tabName.equalsIgnoreCase("mydatabase_tables") || 
					tabName.equalsIgnoreCase("mydatabase_columns"))
			{
				fl = new File(dirName + "catalog/" + tabName+".tbl");
			}
			else
				fl = new File(dirName + "user_data/" + tabName +".tbl");
			RandomAccessFile rf = new RandomAccessFile(fl, "rw");

			rf.setLength(0);
			rf.setLength(pageSize);
			rf.seek(0);
			rf.writeByte(13); //type of page 0x0D for leaf table page
			rf.writeByte(0); //number of records in page
			rf.writeShort(511); //start context, initially it points to end of page
			rf.writeInt(-1);

			String iQuery = "insert into " + tabName + "(";
			ArrayList<ArrayList<String>> recFormat = getRecFormat(tabName);
			for (int i = 0; i < tabData.size(); i++) {
				String columns = "";
				String values = "";
				ArrayList<String> tempList= tabData.get(i);

				columns = "";values = "";
				iQuery = "insert into " + tabName + "(";
				for (int j = 0; j < tempList.size(); j++) {

					if(!tempList.get(j).equalsIgnoreCase("null"))
					{
						columns = columns + recFormat.get(j).get(0) + ",";
						values = values + tempList.get(j) + ",";
					}
				}

				columns = columns.substring(0,columns.length()-1);
				values = values.substring(0,values.length()-1);

				iQuery = iQuery + columns + ") values(" + values + ")";
				Insert(iQuery,"reorder");
			}
			rf.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static long getDateLong(String dateString) {
		// TODO Auto-generated method stub

		dateString = dateString.replace(" ", "");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		long epochSeconds = 0;
		Date dt = null;
		try {
			dt = formatter.parse(dateString);
			epochSeconds = dt.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return epochSeconds;
	}

	private static long getPageCnt(RandomAccessFile rfl, int payLoadLen) throws IOException {
		// TODO Auto-generated method stub
		long pageCnt = rfl.length()/pageSize;

		long pageStartOffset = (pageCnt - 1) * pageSize;

		rfl.seek(pageStartOffset + 1);
		int reccnt = rfl.readUnsignedByte();
		int startContentOffset = rfl.readUnsignedShort();

		rfl.seek(pageStartOffset+8);

		for (int i = 0; i < reccnt; i++) {
			rfl.seek(rfl.getFilePointer() + 2);
		}

		// -6 => 2 byte for pay-load length, 4 bytes for row-id
		int newStartContentOffset = startContentOffset - payLoadLen - 6;

		long currRecArrLoc = rfl.getFilePointer();

		//getPageaddr(rfl,currRecArrLoc,payLoadLen);

		/* space available for this record in current page
		 * else create one more page and insert record in that page
		 */
		if(newStartContentOffset - currRecArrLoc > 2)
		{
			return pageCnt;
		}
		else
		{
			rfl.seek(pageStartOffset + 4);

			rfl.writeInt((int)pageStartOffset+ pageSize);

			//add one page
			rfl.setLength((pageCnt + 1) * pageSize);

			pageCnt = rfl.length()/pageSize;
			pageStartOffset = (pageCnt - 1) * pageSize;

			rfl.seek(pageStartOffset);
			rfl.writeByte(13); //type of page 0x0D for leaf table page
			rfl.writeByte(0); //number of records in page
			/*
			 *  data start context for page,
			 *  initially it points to end of page
			 */
			rfl.writeShort((int)pageStartOffset + (int)pageSize -1); 
			rfl.writeInt(-1);

			return pageCnt;
		}
	}

	private static ArrayList<String> getValues(ExpressionList ex) {
		// TODO Auto-generated method stub
		/*
		 * get values as array-list from expression list
		 */
		ArrayList<String> values = new ArrayList<>();
		for (int i = 0; i < ex.getExpressions().size(); i++) {
			values.add(ex.getExpressions().get(i).toString());
		}
		return values;

	}

	private static ArrayList<ArrayList<String>> getColValList(java.util.List<Column> columns2,
			ArrayList<String> values, ArrayList<ArrayList<String>> recordFormat, Table tabName) {
		// TODO Auto-generated method stub
		/*
		 * used to get list of columns and values from insert query
		 */

		ArrayList<ArrayList<String>> finalList = new ArrayList<>();
		ArrayList<String> tempList = null;

		int colLocation = 0;
		for (int i = 0; i < recordFormat.size(); i++) {

			if(columns2!=null)
			{
				colLocation = -1;
				for (int j = 0; j < columns2.size(); j++) {
					if(columns2.get(j).getColumnName().equalsIgnoreCase(recordFormat.get(i).get(0)))
					{
						colLocation = j;
						break;
					}
				}

				/*
				 * Handling not null constraint 
				 */
				if(colLocation == -1)
				{
					if(recordFormat.get(i).get(4).equalsIgnoreCase("pri"))
					{
						System.out.println("Null value not allowed for primary key column: " + recordFormat.get(i).get(0));
						return null;
					}

					if(recordFormat.get(i).get(5).equalsIgnoreCase("no"))
					{
						System.out.println("Null value not allowed for column: " + recordFormat.get(i).get(0));
						return null;
					}

					/*
					 * value for column is not provided
					 * insert null for that column
					 */
					tempList = new ArrayList<>();
					tempList.add (recordFormat.get(i).get(0));
					tempList.add("null");
					tempList.add("" + new String("null").length());
					tempList.add("12");
				}
				else
				{
					/*
					 * handling duplicate values for primary key column 
					 */
					if(recordFormat.get(i).get(4).equalsIgnoreCase("pri")){

						try {
							ArrayList<ArrayList<String>> tabData = readTable(tabName.getName());

							String priKeyVal = values.get(colLocation);

							for (int j = 0; j < tabData.size(); j++) {
								if(tabData.get(j).get(0).equalsIgnoreCase(priKeyVal))
								{
									System.out.println("Duplicate value not allowed for primary key column: " + recordFormat.get(i).get(0));
									return null;
								}
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					tempList = new ArrayList<>();
					tempList.add(columns2.get(colLocation).getColumnName());
					tempList.add(values.get(colLocation));
					if(recordFormat.get(i).get(1).equalsIgnoreCase("text"))
						tempList.add("" + (values.get(colLocation).length()));
					else
						tempList.add(getLen(recordFormat.get(i).get(1)));

					tempList.add(getSerialTypeCode(recordFormat.get(i).get(1)));
				}
			}
			else
			{
				System.out.println("Provide column names in insert command.");
				return null;
				//				tempList = new ArrayList<>();
				//				tempList.add(recordFormat.get(i).get(0));
				//				tempList.add(values.get(i));
				//				if(recordFormat.get(i).get(1).equalsIgnoreCase("text"))
				//					tempList.add("" + (values.get(i).length()));
				//				else
				//					tempList.add(getLen(recordFormat.get(i).get(1)));
				//
				//				tempList.add(getSerialTypeCode(recordFormat.get(i).get(1)));
			}
			finalList.add(tempList);	
		}
		return finalList;
	}

	private static String getLen(String columnName) {
		// TODO Auto-generated method stub
		/*
		 * get a length for given data type
		 */
		String len = "";
		for (int i = 0; i < dataTypeList.size(); i++) {
			if(dataTypeList.get(i).get(0).equalsIgnoreCase(columnName))
				len = dataTypeList.get(i).get(1);
		}
		return len;
	}

	private static ArrayList<ArrayList<String>> getRecFormat(String tableName) throws Exception {
		// TODO Auto-generated method stub
		/*
		 * get record format for table
		 * record format - column_name,data_type,
		 *                 serial_code for data_type,
		 * 				   ordinal_position
		 *                 primary_key_identifier,
		 *                 not_nullable_identifier
		 */

		ArrayList<ArrayList<String>> recFormat = new ArrayList<>();
		ArrayList<String> tempList = null;
		if(tableName.equalsIgnoreCase("mydatabase_tables") || tableName.equalsIgnoreCase("mydatabase_columns"))
		{
			if (tableName.equalsIgnoreCase("mydatabase_columns"))
			{
				tempList = new ArrayList<>();
				tempList.add("rowid");
				tempList.add("int");
				tempList.add(getSerialTypeCode("int"));
				tempList.add("1");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("table_name");
				tempList.add("text");
				tempList.add(getSerialTypeCode("text"));
				tempList.add("2");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("column_name");
				tempList.add("text");
				tempList.add(getSerialTypeCode("text"));
				tempList.add("3");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("data_type");
				tempList.add("text");
				tempList.add(getSerialTypeCode("text"));
				tempList.add("4");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("ordinal_position");
				tempList.add("tinyint");
				tempList.add(getSerialTypeCode("tinyint"));
				tempList.add("5");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("primary_key");
				tempList.add("text");
				tempList.add(getSerialTypeCode("text"));
				tempList.add("6");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("is_nullable");
				tempList.add("text");
				tempList.add(getSerialTypeCode("text"));
				tempList.add("7");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);
			}

			if (tableName.equalsIgnoreCase("mydatabase_tables"))
			{
				tempList = new ArrayList<>();
				tempList.add("rowid");
				tempList.add("int");
				tempList.add(getSerialTypeCode("int"));
				tempList.add("1");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("table_name");
				tempList.add("text");
				tempList.add(getSerialTypeCode("text"));
				tempList.add("2");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("record_count");
				tempList.add("int");
				tempList.add(getSerialTypeCode("int"));
				tempList.add("3");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

				tempList = new ArrayList<>();
				tempList.add("avg_length");
				tempList.add("smallint");
				tempList.add(getSerialTypeCode("smallint"));
				tempList.add("4");
				tempList.add("no");
				tempList.add("no");
				recFormat.add(tempList);

			}
		}
		else
		{	
			ArrayList<ArrayList<String>> mydbcolumns_data = readTable("mydatabase_columns"); 

			for (int i = 0; i < mydbcolumns_data.size(); i++) {
				if(mydbcolumns_data.get(i).get(1).equalsIgnoreCase(tableName))
				{
					tempList = new ArrayList<>();
					tempList.add(mydbcolumns_data.get(i).get(2));
					tempList.add(mydbcolumns_data.get(i).get(3));
					tempList.add(getSerialTypeCode(mydbcolumns_data.get(i).get(3)));
					tempList.add(mydbcolumns_data.get(i).get(4));
					tempList.add(mydbcolumns_data.get(i).get(5));
					tempList.add(mydbcolumns_data.get(i).get(6));
					recFormat.add(tempList);
				}
			}
		}
		return recFormat;
	}

	public static ArrayList<ArrayList<String>> readTable(String tabName) throws Exception {
		// TODO Auto-generated method stub

		/*
		 * read table data
		 */
		ArrayList<ArrayList<String>> table_data = new ArrayList<>();
		ArrayList<String> tempList = null;

		//ArrayList<ArrayList<String>> mydbcolumnsRecFormat = getRecFormat("mydatabase_columns");
		ArrayList<ArrayList<String>> tabRecFormat = getRecFormat(tabName);
		RandomAccessFile rf = null;
		if (tabName.equalsIgnoreCase("mydatabase_columns") || tabName.equalsIgnoreCase("mydatabase_tables"))
			rf = new RandomAccessFile(new File(dirName+"/catalog/" + tabName + ".tbl"), "rw");
		else
			rf = new RandomAccessFile(new File(dirName+"/user_data/" + tabName + ".tbl"), "rw");

		long pageCnt = rf.length()/pageSize;

		long pageOffset = 0;

		while(pageCnt > 0 && pageOffset > -1)
		{
			rf.seek(pageOffset);
			rf.readUnsignedByte();
			int reccnt = rf.readUnsignedByte();
			int contextAdd = rf.readShort();
			long next_page_address = rf.readInt();
			int recAddrFactor = 0;
			while(reccnt > 0)
			{   
				rf.seek(pageOffset+ 8 + (recAddrFactor *2));

				int recAddr = rf.readShort();

				recAddrFactor++;
				rf.seek(recAddr);
				int recleng = rf.readShort();
				rf.readInt();

				int columns = rf.readUnsignedByte();

				ArrayList<Integer> colList = new ArrayList<>();
				while(columns > 0)
				{
					colList.add(rf.readUnsignedByte());
					columns--;
				}

				//System.out.println(colList);
				tempList = new ArrayList<>();
				for (int i = 0; i < colList.size(); i++) {
					getValue(rf,colList.get(i),tempList,getSerialTypeCode(tabRecFormat.get(i).get(1)));
				}
				table_data.add(tempList);
				reccnt--;
			}

			pageOffset = next_page_address;
			pageCnt--;
		}
		rf.close();
		return table_data;
	}

	public static void getValue(RandomAccessFile rf, Integer len, ArrayList<String> tempList, String serialTypeCode) throws IOException {
		// TODO Auto-generated method stub
		/*
		 * read from file based on data_type
		 */
		int a =0;
		double d =0;
		long l = 0;

		if( len > 12)
		{
			byte[] b = new byte[len - 12];
			if (len > 12)
				try {
					rf.read(b, 0, len-12);
					tempList.add(new String(b));
					return;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		switch (serialTypeCode) {
		case "4":
			a=0;
			a = rf.readUnsignedByte();
			tempList.add(Integer.toString(a));
			break;
		case"5":
			a=0;
			a = rf.readUnsignedShort();
			tempList.add(Integer.toString(a));
			break;

		case"6":
			a=0;
			a = rf.readInt();
			tempList.add(Integer.toString(a));
			break;

		case "7":
			double a1=0;
			a1 = rf.readLong();
			tempList.add(Double.toString(a1));
			break;

		case "8":
			a=0;
			a = rf.readInt();
			tempList.add(Integer.toString(a));
			break;

		case "9":
			a1=0;
			a1 = rf.readLong();
			tempList.add(Double.toString(a1));
			break;	
		case "10":
			l=0;
			l = rf.readLong();
			tempList.add(getDate(l,"yyyy-MM-dd HH:mm:ss"));
			break;
		case "11":
			l=0;
			l = rf.readLong();
			tempList.add(getDate(l,"yyyy-MM-dd"));
			break;	

		case "12":
			if( len > 12)
			{
				byte[] b = new byte[len - 12];
				if (len > 12)
					try {
						rf.read(b, 0, len-12);
						tempList.add(new String(b));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			else
				System.out.println("Len:" + len);
			break;
		}
	}

	private static String getDate(long a1, String dateFormat) {
		// TODO Auto-generated method stub
		/*
		 * long to date conversion in date_format
		 */
		Date date=new Date(a1);
		SimpleDateFormat df = new SimpleDateFormat(dateFormat);
		String dateText = df.format(date);
		return dateText;
	}

	private static String getSerialTypeCode(String string) {
		// TODO Auto-generated method stub
		/*
		 * get serial_type_code for given data_type
		 */
		for (int i = 0; i < dataTypeList.size(); i++) {
			if(dataTypeList.get(i).get(0).equalsIgnoreCase(string))
				return dataTypeList.get(i).get(2);
		}
		return null;
	}

	public static void sort(ArrayList<ArrayList<String>> data) {
		/*
		 * sort data based on primary key(1st column of table)
		 */
		ArrayList<String> tempList = new ArrayList<>();

		for (int c = 0 ; c < data.size(); c++)
		{
			for (int d = 0 ; d < data.size() - c - 1; d++)
			{
				if (Integer.parseInt(data.get(d).get(0)) > Integer.parseInt(data.get(d+1).get(0))) /* For decreasing order use < */
				{
					tempList = data.get(d);
					data.set(d, data.get(d+1));
					data.set(d+1, tempList);
				}
			}
		}
	}
}

