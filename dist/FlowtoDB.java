

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.boomi.document.scripting.DataContextImpl;
import com.boomi.execution.ExecutionUtil;  
import java.util.logging.Logger;  
  


public class Lookup 
{

	public String query(DataContextImpl dataContext) 
	{

        Logger logger = ExecutionUtil.getBaseLogger();  
        
        
        
		String classname = ExecutionUtil.getDynamicProcessProperty("DatabaseDriverClass");
        String user = ExecutionUtil.getDynamicProcessProperty("UserName");
        String password = ExecutionUtil.getDynamicProcessProperty("Password");
        String protocol = ExecutionUtil.getDynamicProcessProperty("Protocol");
        String host = ExecutionUtil.getDynamicProcessProperty("Host");
        String port = ExecutionUtil.getDynamicProcessProperty("Port");
        String database = ExecutionUtil.getDynamicProcessProperty("Database");
        String tableName = ExecutionUtil.getDynamicProcessProperty("TableName");
        
        Properties conProps = new Properties();
        conProps.setProperty("user", user);
        conProps.setProperty("password", password);
        conProps.setProperty("ssl", "false");
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        
        
        String req = ""; 
        
        String connectionUrl = protocol + host + ":" + port + "/" + database;  
        
        Connection con = null;  
        Statement stmt = null;  
        ResultSet rs = null;  
        
        Class.forName(classname);  
        con = DriverManager.getConnection(connectionUrl,conProps);  
        stmt = con.createStatement();
        
         //build SQL
        String sql = "SELECT * FROM " + tableName;
        String where = "";
        String orderBy = "";
        
        //get input document
        if(dataContext.getDataCount() > 0)
        {
            InputStream is = dataContext.getStream(0); 
            
            Scanner s = new Scanner(is).useDelimiter("\\A");
            req = s.hasNext() ? s.next() : "";
            
            
        	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        	factory.setNamespaceAware(true);
        	DocumentBuilder builder = factory.newDocumentBuilder();
        	logger.info("REQ=" + req);
        	Document doc = builder.parse(new InputSource(new StringReader(req)));
        	NodeList expressions = doc.getElementsByTagName("expression");
        	NodeList filters = doc.getElementsByTagName("nestedExpression");
        	NodeList sorts = doc.getElementsByTagName("sort");
        	Node item = null;
        	String field = "";
        	String comparator = "";
        	String value="";
        	
        	
        	//get outer grouping
        	String joiner = expressions.item(0).getAttributes().getNamedItem("operator").getTextContent().toUpperCase() == "AND"?"AND":"OR";
        	
        	//get filters
        	for(int fPos = 0 ; fPos < filters.getLength() ; fPos++)
        	{
        		String whereItem="";
        		String whereItems="";
        		item = filters.item(fPos);
        		field=item.getAttributes().getNamedItem("property").getTextContent();
        		comparator=item.getAttributes().getNamedItem("operator").getTextContent().toUpperCase().trim();
        		NodeList values=((Element)item).getElementsByTagName("argument");
        		logger.info("VAL_LEN=" + values.getLength());
        		for(int vPos = 0 ; vPos < values.getLength() ; vPos++)
        		{
        			value = values.item(vPos).getTextContent();
        			logger.info("VAL=" + value);
        			
        			whereItem = makeWhere(field,comparator,value,"TEXT");
        			
        			if(whereItem.length() > 0)
            		{
            			if(whereItems.length()>0)
                		{
            				whereItems += " " + joiner + " ";
                		}
            			whereItems += whereItem;
            		}
        		}
        		
        		if(whereItems.length() > 0)
        		{
        			if(where.length()>0)
            		{
        				where += " " + joiner + " ";
            		}
        			where += "(" + whereItems + ")";
        		}
        	}
        	
        	//get sorts
        	for(int sPos = 0 ; sPos < sorts.getLength() ; sPos++)
        	{
        		item = sorts.item(sPos);
        		field=item.getAttributes().getNamedItem("property").getTextContent();
        		value=item.getAttributes().getNamedItem("sortOrder").getTextContent();
        		
        		if(orderBy.length()>0)
        		{
        			orderBy += ", ";
        		}
        		orderBy += field + " " + value;
        	}
           
        }
        
        if(where.length() > 0)
        {
        	sql += " WHERE " + where;
        }
        	
        if(orderBy.length() > 0)
        {
        	sql += " ORDER BY " + orderBy;
        }

        logger.info("SQL=" + sql);
        
        rs = stmt.executeQuery(sql);
        
        //get columns
        Map<String, String> columns = new HashMap<String,String>();
        ResultSetMetaData rsmd = rs.getMetaData();
        for(int pos = 1 ; pos <= rsmd.getColumnCount() ; pos++)
        {
        	columns.put(rsmd.getColumnName(pos), rsmd.getColumnTypeName(pos));
        }
        
        String json = "";
        while (rs.next()) 
        {
        	json = "{";
        	String inner = "";
        	String text = "";
        	BigDecimal bd;
        	for(Map.Entry<String,String> column : columns.entrySet())
        	{
        		if(inner.length() > 0)
        		{
        			inner += ",";
        		}
        		inner += "\"" + column.getKey() + "\"";
        		inner += ":";
        		switch(column.getValue())
        		{
        			case "text":
        				text = rs.getString(column.getKey());
        				text = rs.wasNull()?"":text;
        				inner += "\"" + text + "\"";
        				break;
        			
        			case "timestamp":
        				Timestamp ts = rs.getTimestamp(column.getKey());
        				text = rs.wasNull()?"":df.format(ts);
        				inner += "\"" + text  + "\"";
        				break;
        			case "bigserial":
        				bd = rs.getBigDecimal(column.getKey());
        				text = rs.wasNull()?"":bd.toString();
        				inner += text;
        				break;
        			
        			default:
        				inner += "\"" + rs.getString(column.getKey()) + "\"";
        				break;
        		}
        		
        	}
        	
        	json += inner + "}";
        	
        	Properties docProps = dataContext.getProperties(0);
            dataContext.storeStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),docProps);
        }
		

        return json;
	}
	
	private String makeWhere(String field, String comparator, String value, String fieldType)
	{
		String result = "";
		String comma = fieldType=="TEXT"?"'":"";
		String wildcard = "%";
		switch(comparator)
		{
    		case "EQUAL":
    			result += field + " = " + comma + value + comma;
    			break;
    			
    		case "NOT_EQUAL":
    			result += field + " != " + comma + value + comma;
    			break;
    			
    		case "LESS_THAN":
    			result += field + " < " + comma + value + comma;
    			break;
    			
    		case "LESS_THAN_OR_EQUAL":
    			result += field + " <= " + comma + value + comma;
    			break;
    			
    		case "GREATER_THAN":
    			result += field + " > " + comma + value + comma;
    			break;
    			
    		case "GREATER_THAN_OR_EQUAL":
    			result += field + " >= " + comma + value + comma;
    			break;
    		
    		case "STARTS_WITH":
    			result += field + " LIKE " + comma + value + wildcard + comma;
    			break;
    			
    		case "ENDS_WITH":
    			result += field + " LIKE " + comma + wildcard + value + comma;
    			break;
    			
    		case "CONTAINS":
    			result += field + " LIKE " + comma + wildcard + value + wildcard + comma;
    			break;
    			
    		case "IS_EMPTY":
    			value = value.toUpperCase().trim();
    			if(value=="TRUE")
    			{
    				result += field + " != ''";
    			}
    			else
    			{
    				result += field + " = ''";
    			}
    			break;
		}
		
		return result;
	}

}

Lookup lu = new Lookup();
String json = lu.query(dataContext);
