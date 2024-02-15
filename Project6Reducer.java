//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Six
//
//  File Name:    Project6Reducer.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     10/27/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This class contains the reducer for the hadoop project
//
//***************************************************************

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Project6Reducer extends Reducer<Text, FraudWritable, Text, IntWritable> {
	// declare instance variable to hold list of customer fraud info
    private List<CustomerFraud> fraudList = new ArrayList<>();
    
	//***************************************************************
    //
    //  Method:       reduce override
    // 
    //  Description:  Runs reducer job for customer fraud
    //
    //  Parameters:   Text key, Iterable<FraudWritable> values, Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    public void reduce(Text key, Iterable<FraudWritable> values, Context context){
    	// Declare variables to hold information from each customer purchase
    	String customerId = key.toString();
    	int numReturns = 0;
    	int numPurchases = 0;
        int totalFraudPoints = 0;
        String customerName = "";
        
        // Loop through list of customer purchases
        for (FraudWritable value : values) {
        	
        	// If its the first purchase set name of customer to use in output
        	if (numPurchases == 0) {
        		customerName = value.getCustomerName();
        	}
        	
        	// If customer returned a product
            if (value.getReturnedFlag()) {
            	numReturns++;
            	int timeDiff = numDays(value);
            	
            	// If customer returned the product after 10 days increment fraud points by 1
                if (timeDiff > 10) {
                	totalFraudPoints += 1;
                	}
                
                }
            
            // Increment number of purchases
            numPurchases++;
            }
        
        // Check to see if they have returned more than 50% of their purchases cast to double to avoid truncation
        if ((((double) numReturns / numPurchases)) >= .50) {
        	totalFraudPoints += 10;
        }
        
        // Add fraud points to list of customer fraud points 
        fraudList.add(new CustomerFraud(customerId, customerName, totalFraudPoints));
        
        }
    
	//***************************************************************
    //
    //  Method:       numDays
    // 
    //  Description:  calculates the difference of days from purchase to return
    //
    //  Parameters:   FraudWritable value
    //
    //  Returns:      int numdays 
    //
    //**************************************************************
    public int numDays(FraudWritable value) {
    	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
        // Declare Date variables to hold parsed date
        Date receivedDate = null;
        Date returnedDate = null;
		try {
			// Parse received date
			receivedDate = formatter.parse(value.getReceivedDate());
            // Parse returned date
			returnedDate = formatter.parse(value.getReturnedDate());
		} catch (ParseException e) {
			e.printStackTrace();
		}
        // Calculate time difference in days
        return (int) ((returnedDate.getTime() - receivedDate.getTime()) / (86400000));
    }
    
	//***************************************************************
    //
    //  Method:       cleanup
    // 
    //  Description:  uses compare to order the list in decending order
    //
    //  Parameters:   Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    public void cleanup(Context context){
        // Sort the fraudList in descending order based on fraud points
        Collections.sort(fraudList, new Comparator<CustomerFraud>() {
            @Override
            public int compare(CustomerFraud cf1, CustomerFraud cf2) {
            	// Compare the customer fraud points objects
                return Integer.compare(cf2.getPoints(), cf1.getPoints());
            }
        });

        // Write out the sorted results
        for (CustomerFraud cf : fraudList) {
        	try {
        		context.write(new Text(cf.getIdName()), new IntWritable(cf.getPoints()));
        	} catch(Exception e) {
    			e.printStackTrace();
    			System.exit(0);
        	}
        }
    }
}