package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 */
public class CassandraRow implements Serializable {

    private List<CassandraColumnDefinition> definitions;
    private List<Object> columnValues;

    private CassandraRow(List<CassandraColumnDefinition> definitions, List<Object> columnValues) {
        this.definitions = definitions;
        this.columnValues = columnValues;
    }

    public CassandraRow() {
        definitions = new ArrayList<>();
        columnValues = new ArrayList<>();
    }

    public static CassandraRow fromJavaDriverRow(com.datastax.driver.core.Row row) {
        List<Object> columnValues = new ArrayList<>();
        List<CassandraColumnDefinition> definitions = new ArrayList<>();

        for (ColumnDefinitions.Definition definition : row.getColumnDefinitions().asList()) {
            columnValues.add(row.getObject(definition.getName()));

            CassandraColumnDefinition def = new CassandraColumnDefinition(definition.getName(),
                    getType(definition.getType()));
            definitions.add(def);
        }

        return new CassandraRow(definitions, columnValues);
    }

    public void add(String name, CassandraColumnDefinition.Type type, Object value){
        definitions.add(new CassandraColumnDefinition(name, type));
        columnValues.add(value);
    }

    public static CassandraColumnDefinition.Type getType(DataType type) {
        switch (type.getName()) {
            case TEXT:
                return CassandraColumnDefinition.Type.TEXT;
            case INT:
                return CassandraColumnDefinition.Type.INT;
            default:
                return CassandraColumnDefinition.Type.TEXT;
        }
    }

    public int getIndex(String name) throws NoSuchElementException {
        int i = 0;
        for (CassandraColumnDefinition definition : definitions) {
            if (definition.getColName().equals(name)) {
                return i;
            }
            i++;
        }
        throw new NoSuchElementException();
    }

    public Object getValue(int index) {
        return columnValues.get(index);
    }

    public Object getValue(String name) {
        return columnValues.get(getIndex(name));
    }

    public List<Object> getValues() {
        return columnValues;
    }

    public List<CassandraColumnDefinition> getDefinitions() {
        return definitions;
    }


}
