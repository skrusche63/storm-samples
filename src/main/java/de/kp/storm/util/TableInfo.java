package de.kp.storm.util;

import java.util.List;

public class TableInfo {

	private static TableInfo instance = new TableInfo();
	private List<ColumnInfo> columnInfo;
	
	private TableInfo() {
	}
	
	public static TableInfo getInstance() {
		if (instance == null) instance = new TableInfo();
		return instance;
	}
	
	public List<ColumnInfo> getColumnInfo() {
		return columnInfo;
	}
	
	public void setColumnInfo(List<ColumnInfo> columnInfo) {
		this.columnInfo = columnInfo;
	}
	
	public ColumnInfo getColumn(int index) {
		return this.columnInfo.get(index);
	}
	
	public int size() {
		return this.columnInfo.size();
	}
}
