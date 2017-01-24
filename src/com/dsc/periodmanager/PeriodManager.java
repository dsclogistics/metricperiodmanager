package com.dsc.periodmanager;

import java.sql.Connection;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/*
 * This class has the following functionality:
 * - it first creates a record in rz_mtrc_period_status table with period status = Open
 * 
 * - then it checks if there are any incomplete(not Approved) action plans that are older that 2 months
 *   If it finds them, it sets the status to Expired 
 * 
 * This script should run on the first day of the month.
 * If something goes wrong and it can't perform any of the steps listed above, it sends an email to the 
 * email address passed as a parameter
 * 
 * @param: url - database connection URL
 * @param: sendTo - email address to use in case of error 
 *   
 */
public class PeriodManager {

	
	public static void main(String[] args) {

		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement expPS = null;// preparedStatement to set Action Plan status to Expired 
		Statement stmt = null;
		ResultSet rs = null;
		/*
		String dbServerName = "DSCOBSSQLDEV";
		String dbInstanceName ="OBSSQLDEV";
		String dbName = "DSC_MTRC_DEV";
		String url = "jdbc:sqlserver://"+dbServerName+"\\"+dbInstanceName+";DatabaseName="+dbName;
		String sendTo ="rasul.abduguev@dsc-logistics.com";*/
		//String user = "sqluser";
		//String pass = "sqluser";
		String url = args[0];
		String sendTo = args[1];		
		String user = "MTRC_BATCH_USER";
		String pass = "MTRCbatchuser";
		
		int month = getMonth();
		int year = getYear();
		if(month ==0)
		{
			month = 12;
			year = year -1;
		}
		System.out.println("url = "+url);
		System.out.println("email = "+sendTo);
		
		
		String  rzMetricPeriodSQL = "select MTRC_METRIC_PRODUCTS.mtrc_period_id,MTRC_TM_PERIODS.tm_period_id"
				+ " from MTRC_METRIC_PRODUCTS,MTRC_PRODUCT,MTRC_TM_PERIODS,MTRC_TIME_PERIOD_TYPE"
				+ " where MTRC_PRODUCT.prod_id = MTRC_METRIC_PRODUCTS.prod_id"
				+ " and MTRC_PRODUCT.prod_token = 'RED_ZONE_WHS'"
				+ " and MTRC_METRIC_PRODUCTS.mtrc_prod_eff_start_dt<=CAST(MTRC_TM_PERIODS.tm_per_start_dtm AS DATE)"
				+ " and MTRC_METRIC_PRODUCTS.mtrc_prod_eff_end_dt>=CAST(MTRC_TM_PERIODS.tm_per_end_dtm AS DATE)"
				+ " and MTRC_METRIC_PRODUCTS.mtrc_prod_top_lvl_parent_yn = 'Y'"
				+ " and MTRC_TM_PERIODS.tpt_id = MTRC_TIME_PERIOD_TYPE.tpt_id"
				+ " and MTRC_TIME_PERIOD_TYPE.tpt_name = 'Month' "
				+ " and DATEADD(month, -1, GETDATE()) between MTRC_TM_PERIODS.tm_per_start_dtm and MTRC_TM_PERIODS.tm_per_end_dtm";
  

		
		String insertSQL = " insert into rz_mtrc_period_status(tm_period_id,mtrc_period_id,rz_mps_status,rz_mps_opened_on_dtm)"
				+ " values(?,?,'Open',GETDATE())";
				     
		
		String actionPlanExpSQL = "select RZ_BAP_METRICS.rz_bapm_id"
				+ " from MTRC_TM_PERIODS,"
				+ " MTRC_TIME_PERIOD_TYPE,"
				+ " RZ_BLDG_ACTION_PLAN,"
				+ " RZ_BAP_METRICS"
				+ " where MTRC_TM_PERIODS.tpt_id = MTRC_TIME_PERIOD_TYPE.tpt_id"
				+ " and MTRC_TIME_PERIOD_TYPE.tpt_name = 'Month' "
				+ " and MTRC_TM_PERIODS.tm_period_id = RZ_BLDG_ACTION_PLAN.tm_period_id"
				+ " and RZ_BLDG_ACTION_PLAN.rz_bap_id = RZ_BAP_METRICS.rz_bap_id"
				+ " and RZ_BAP_METRICS.rz_bapm_status not in ('Approved','Expired')"
				+ " and MTRC_TM_PERIODS.tm_per_end_dtm < DATEADD(month, -2, GETDATE())";
		
		
		String expireAPSQL = "update RZ_BAP_METRICS set rz_bapm_status = 'Expired' where rz_bapm_id = ?";
		

		try {

			System.out.println("Connecting...");
			conn = DriverManager.getConnection(url, user, pass);
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			System.out.println("Connected");
			//first need to update all action plans that haven't been completed. 
			System.out.println("Updating Expired Action Plans.....");
			expPS = conn.prepareStatement(expireAPSQL);
			rs = stmt.executeQuery(actionPlanExpSQL);
			while(rs.next())
			{
				System.out.println(rs.getString("rz_bapm_id"));
				expPS.setInt(1,rs.getInt("rz_bapm_id"));
				expPS.addBatch();
			}
			rs.close();
			expPS.executeBatch();
			
			//Now need to open new period
			System.out.println("Opening period.....");
			ps = conn.prepareStatement(insertSQL);
			rs = stmt.executeQuery(rzMetricPeriodSQL);
			while(rs.next())
			{			 
			  ps.setInt(1, rs.getInt("tm_period_id"));
			  ps.setInt(2, rs.getInt("mtrc_period_id"));
			  ps.addBatch();
			}
			rs.close();
			ps.executeBatch();	
			System.out.println("Done");
			conn.commit();			

		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Exception...");
			try {
				conn.rollback();								
			
			} catch (Exception e) {
				e.printStackTrace();
			}
			sendEmail(ex.getMessage(),sendTo);
		}
		finally {
			try {				
				if (stmt != null && !stmt.isClosed()) {
					stmt.close();
				}
				if (ps != null && !ps.isClosed()) {
					ps.close();
				}
				if (expPS != null && !expPS.isClosed()) {
					expPS.close();
				}
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}	
    public static int getMonth()
    {
    	Date date= new Date();
    	Calendar cal = Calendar.getInstance();
    	cal.setTime(date);
    	int month = cal.get(Calendar.MONTH);//always returns month -1 since it starts the count from 0
    	return month;
    }
    public static int getYear()
    {
    	Date date= new Date();
    	Calendar cal = Calendar.getInstance();
    	cal.setTime(date);
    	int year = cal.get(Calendar.YEAR);
    	return year;
    }
    public static void sendEmail(String error, String sendTo)
    {
    	// Recipient's email ID needs to be mentioned.
        //String to = "rasul.abduguev@dsc-logistics.com";

        // Sender's email ID needs to be mentioned
        String from = "RedZone";

        String host = "DSC_NOTES.DSCCORP.NET";

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", host);

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);

        try {
           // Create a default MimeMessage object.
           MimeMessage message = new MimeMessage(session);

           // Set From: header field of the header.
           message.setFrom(new InternetAddress(from));

           // Set To: header field of the header.
           message.addRecipients(Message.RecipientType.TO, sendTo);

           // Set Subject: header field
           message.setSubject("Error Opening Period");

           // Now set the actual message
           message.setText("Application couldn't open a period for the current month. \n\n Error returned: "+error);

           // Send message
           Transport.send(message);
           System.out.println("Sent message successfully....");
        }catch (MessagingException mex) {
           mex.printStackTrace();
        }
    }

}
