//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Six
//
//  File Name:    Project6Mapper.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     10/27/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This mapper uses the new FraudWritable datatype
//
//***************************************************************

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Project6Mapper extends Mapper<LongWritable, Text, Text, FraudWritable> {

	//***************************************************************
    //
    //  Method:       map
    // 
    //  Description:  map override 
    //
    //  Parameters:   LongWritable key, Text value, Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            // Split the input line into columns using a comma as the delimiter
            String[] columns = value.toString().split(",");

            // Check if the input line has all 9 columns
            if (columns.length == 9) {
                String customerID = columns[0];
                String customerName = columns[1];
                String receivedDateStr = columns[5];
                String returned = columns[6];
                boolean returnedFlag = returned.equalsIgnoreCase("yes");
                String returnedDateStr = columns[7];
            	// Write out Key value pair
                context.write(new Text(customerID), new FraudWritable(customerName, receivedDateStr, returnedFlag, returnedDateStr));
                }
            
        } catch (Exception e) {
            // Handle any exceptions that occur during processing
            e.printStackTrace();
            System.exit(0);
        }
    }
}