/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cs310.json.database;

import java.sql.*;
import org.json.simple.*;


/**
 *
 * @author lemmo
 */
public class CS310JsonDatabase {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args){
        JSONArray results = getJSONData();
        System.out.println(results);
    }
    public static JSONArray getJSONData() {
        
        JSONArray results = new JSONArray();
        
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        String query;
        boolean hasresults;
        int resultCount, columnCount;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS310";
            System.out.println("Connecting to " + server + "...");
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
                
                // Prepare Update Query
                // Execute Update Query
                // Get New Key; Print To Console
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                /* Execute Select Query */
                
                System.out.println("Submitting Query ...");
                
                hasresults = pstSelect.execute();                
                
                /* Get Results */
                
                System.out.println("Getting Results ...");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        JSONArray headers = new JSONArray();
                        /* Get Column Names; Print as Table Header */
                        
                        for (int i = 2; i <= columnCount; i++) {

                            headers.add(metadata.getColumnLabel(i));


                        }
                        
                        /* Get Data; Print as Table Rows */
                        
                        while(resultset.next()) {
                            
                            /* Begin Next ResultSet Row */
                            JSONObject dataSet = new JSONObject();
                            System.out.println();
                            
                            /* Loop Through ResultSet Columns; Print Values */

                            for (int i = 0; i <= columnCount - 2; i++){
                                for (int j = 1; j <= columnCount;j++){
                                   String colName = (String)headers.get(i);
                                   dataSet.put(headers.get(i), resultset.getString(colName));
                                }

                            }

                            results.add(dataSet);
                            
                        }
                        
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                
            }
            
            System.out.println();
            
            /* Close Database Connection */
            
            conn.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }   
        
        return results;
        
    }
    
}
