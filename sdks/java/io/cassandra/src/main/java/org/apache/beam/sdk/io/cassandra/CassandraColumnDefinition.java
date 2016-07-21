package org.apache.beam.sdk.io.cassandra;

import java.io.Serializable;

/**
 *
 */
public class CassandraColumnDefinition implements Serializable {
    private String colName;
    private Type colType;

    public CassandraColumnDefinition(String name, Type type){
        this.colName = name;
        this.colType = type;
    }

    public String getColName(){
        return colName;
    }
    public Type getColType(){
        return colType;
    }

    /**
     *
     */
    public enum Type {
        INT, TEXT
    }
}
