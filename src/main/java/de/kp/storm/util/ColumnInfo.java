package de.kp.storm.util;

public class ColumnInfo {

	private String type;
	private String name;

	public ColumnInfo() {		
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String toString() {
		
		String columnStr = null;
		
        if (type.equalsIgnoreCase("String"))
            columnStr = name + " VARCHAR(500)";
        
        else
            columnStr = name + " " + type;
  
        return columnStr;
        
	}
	
}
