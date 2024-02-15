//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Six
//
//  File Name:    FraudWritable.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     10/27/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This class contains the new datatype for the project
//
//***************************************************************

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class FraudWritable implements Writable {
    private String customerName;
    private String receivedDate;
    private boolean returnedFlag;
    private String returnedDate;

	//***************************************************************
    //
    //  Method:       FraudWritable constructor
    // 
    //  Description:  no arguments, calls 4 argument constructor 
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public FraudWritable() {
        // Calls the four-argument constructor with default values
        this("", "", false, "");
    }

	//***************************************************************
    //
    //  Method:       FraudWritable constructor
    // 
    //  Description:  4 argument constructor
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public FraudWritable(String customerName, String receivedDate, boolean returnedFlag, String returnedDate) {
        this.customerName = customerName;
        this.receivedDate = receivedDate;
        this.returnedFlag = returnedFlag;
        this.returnedDate = returnedDate;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getReceivedDate() {
        return receivedDate;
    }

    public void setReceivedDate(String receivedDate) {
        this.receivedDate = receivedDate;
    }

    public void setReturnedFlag(boolean returnedFlag) {
        this.returnedFlag = returnedFlag;
    }
    
    public boolean getReturnedFlag() {
    	return this.returnedFlag;
    }

    public String getReturnedDate() {
        return returnedDate;
    }

    public void setReturnedDate(String returnedDate) {
        this.returnedDate = returnedDate;
    }
    
	//***************************************************************
    //
    //  Method:       write override
    // 
    //  Description:  overrides the write method to write out the datatype
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    public void write(DataOutput out) {
        // Write instance variables to the DataOutput
    	try {
			WritableUtils.writeString(out, this.customerName);
	    	WritableUtils.writeString(out, this.receivedDate);
	        out.writeBoolean(returnedFlag);
	    	WritableUtils.writeString(out, this.returnedDate);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
			}
    	}
    
	//***************************************************************
    //
    //  Method:       readFields override
    // 
    //  Description:  overrides the readFields method to read the datatype
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    public void readFields(DataInput in) {
        // Read instance variables from the DataInput
    	try {
			this.customerName = WritableUtils.readString(in);
	    	this.receivedDate = WritableUtils.readString(in);
	    	returnedFlag = in.readBoolean();
	    	this.returnedDate = WritableUtils.readString(in);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
			}
    	}  
    }