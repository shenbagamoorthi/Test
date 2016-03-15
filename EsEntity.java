/***
*** Object Model for Reading tables from ES. sets/gets the value from/to ES to this model using JACKSON object mapper.
***/
package com.elasticsearch.model;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sales.intelligence.common.IbdSIAbstractModel;

public class EsEntity extends IbdSIAbstractModel {
	 public EsEntity() {
	        //constructor Code
	    }
	public String getTableName() {
        return "ENTITY".toLowerCase();
    }
	public void setTableName() {
		tableName =  "ENTITY".toLowerCase();
    }
    private long Id;
    public long getPK() {
        return this.Id;
    }

    public void setPK(long Id) {
        this.Id = Id;
    }
    protected String auditXML;
    protected String tableName;
    protected String entityName;
    protected String entityCode;
    protected String displayName;
    protected long entityType;
    protected long pos;

	public String getEsEntityName() {
		return entityName;
	}
	public void setEsEntityName(String entityName) {
		this.entityName = entityName;
	}
	public long getEsEntityType() {
		return entityType;
	}
	public void setEsEntityType(long entityType) {
		this.entityType = entityType;
	}

	public String getEsEntityCode() {
		return entityCode;
	}

	public void setEsEntityCode(String entityCode) {
		this.entityCode = entityCode;
	}

	public String getEsDisplayName() {
		return displayName;
	}

	public void setEsDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public long getEsPos() {
		return pos;
	}

	public void setEsPos(long pos) {
		this.pos = pos;
	}
	 protected String getInsertSql() {return "";}

	    protected String getUpdateSql() {return "";}

	    protected void bindStmnt(PreparedStatement pstmt, String username, boolean isInsert, int oldLockTs) throws Exception {}

	    public void readAll(ResultSet rs) throws Exception {
	       /* setPK(rs.getLong("ID"));
	        setEsEntityName(rs.getString("ENTITY_NAME"));
	        setEsEntityCode(rs.getString("ENTITY_CODE"));
	        setEsDisplayName(rs.getString("DISPLAY_NAME"));
	        readAllStdCols(rs);*/
	    }
}