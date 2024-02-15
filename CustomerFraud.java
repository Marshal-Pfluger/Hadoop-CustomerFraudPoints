//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Six
//
//  File Name:    CustomerFraud.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     10/27/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This class contains the customer fraud object
//
//***************************************************************

public class CustomerFraud {
	private String idName;
	private int fraudPoints;
	
	//***************************************************************
    //
    //  Method:       CustomerFraud constructor
    // 
    //  Description:  no arguments, calls 3 argument constructor 
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	CustomerFraud(){
		this("", "", 0);
	}
	
	//***************************************************************
    //
    //  Method:       CustomerFraud constructor
    // 
    //  Description:  3 argument constructor 
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	CustomerFraud(String id, String name, int points){
		this.idName = (String.format("%s\t%-15s", id, name));
		this.fraudPoints = points;
	}
	
	public String getIdName() {
		return idName;
	}
	
	public int getPoints() {
		return fraudPoints;
	}
}
