package org.apache.beam.sdk.io.cassandra;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an Cassandra database for JUnit tests.
 */
public class EmbeddedCassandraExampleDataResource extends EmbeddedCassandraResource {
    /**
     * All of the columns in the example_src table.
     */
    public static final String[] SEXAMPLESRCCOLUMNS = {"key1", "st_ascii", "st_bigint",
            "st_blob", "st_boolean",
            "st_date", "st_decimal",
            "st_double", "st_float", "st_inet", "st_int", "st_smallint", "st_text", "st_time",
            "st_timestamp",
            "st_timeuuid", "st_uuid",
            "st_tinyint", "st_varchar", "st_varint", "st_list", "st_map", "st_set", "st_tuple",
            "st_udt"};
    /**
     * All of the row keys in the example_src table.
     */
    public static final String[] SEXAMPLESRCROWKEYS = {"null", "zero", "example", "min", "max",
            "nested"};
    /**
     * The snippets to create the example source table from. The values are build directly into a
     * INSERT CQL statement.
     */
    public static final Table<String, String, String> SEXAMPLESRCCELLS = getExampleRows();
    private static final Logger LOG = LoggerFactory.getLogger
            (EmbeddedCassandraExampleDataResource.class);
    private final String mTableSrc;

    private final String mTableCounter;

    private final String mTableDst;

    public EmbeddedCassandraExampleDataResource(String keySpace) {
        this(keySpace, "example_src", "example_counter", "example_dst");
    }

    public EmbeddedCassandraExampleDataResource(String keySpace, String tableSrc, String
            tableCounter, String
                                                        tableDst) {
        super(keySpace);
        mTableSrc = tableSrc.startsWith("\"") ? tableSrc : tableSrc.toLowerCase();
        mTableCounter = tableCounter.startsWith("\"") ? tableCounter : tableCounter.toLowerCase();
        mTableDst = tableDst.startsWith("\"") ? tableDst : tableDst.toLowerCase();
    }

    /**
     * @return a table with the cell values that can be included in a CQL insert table, keyed on
     * example row key and
     * column name.
     */
    private static Table<String, String, String> getExampleRows() {
        // Insert test data into a table. ---------------------------------------
        ImmutableTable.Builder<String, String, String> builder = new ImmutableTable.Builder<>();

        // All of the cell values are null.
        String r = "null";
        builder.put(r, "key1", "'" + r + "'");

        // All of the cells are a zero or empty value.
        r = "zero";
        builder.put(r, "key1", "'" + r + "'") //
                .put(r, "st_ascii", "''") //
                .put(r, "st_bigint", "0") //
                .put(r, "st_blob", "textAsBlob('')") //
                .put(r, "st_boolean", "false") //
                .put(r, "st_date", "'1970-01-01'") //
                .put(r, "st_decimal", "0.0000") //
                .put(r, "st_double", "0.0") //
                .put(r, "st_float", "0.0") //
                .put(r, "st_inet", "'0.0.0.0'") //
                .put(r, "st_int", "0") //
                .put(r, "st_smallint", "0") //
                .put(r, "st_text", "''") //
                .put(r, "st_time", "'00:00:00'") //
                .put(r, "st_timestamp", "'1970-01-01'") //
                .put(r, "st_tinyint", "0") // tinyint is byte
                .put(r, "st_timeuuid", "minTimeuuid('1970-01-01')") //
                .put(r, "st_uuid", "00000000-0000-0000-0000-000000000000") //
                .put(r, "st_varchar", "''") //
                .put(r, "st_varint", "0") //
                .put(r, "st_list", "[]") //
                .put(r, "st_map", "{}") //
                .put(r, "st_set", "{}") //
                .put(r, "st_tuple", "(0, '', 0.0)") //
                .put(r, "st_udt", "{id: 0, name: '', valid: false, value: 0.0}");

        // All of the cells have an example 1234-like value. Nested structures
        // only have one value.
        r = "example";
        builder.put(r, "key1", "'" + r + "'") //
                .put(r, "st_ascii", "'123'") //
                .put(r, "st_bigint", "1234567890123") //
                .put(r, "st_blob", "0x0102030405") //
                .put(r, "st_boolean", "true") //
                .put(r, "st_decimal", "1.23") //
                .put(r, "st_double", "1.234") //
                .put(r, "st_float", "1.2345") //
                .put(r, "st_inet", "'1.2.3.4'") //
                .put(r, "st_int", "1234") //
                .put(r, "st_text", "'1234567'") //
                .put(r, "st_timestamp", "'1998-01-02'") //
                .put(r, "st_timeuuid", "maxTimeuuid('1998-01-02 03:04:05+0006')") //
                .put(r, "st_uuid", "12345678-1234-1234-1234-123456789012") //
                .put(r, "st_varchar", "'12345'") //
                .put(r, "st_varint", "12345678901234567890") //
                .put(r, "st_date", "'1998-01-02'") //
                .put(r, "st_time", "'03:04:05'") //
                .put(r, "st_smallint", "123") //
                .put(r, "st_tinyint", "1") //
                .put(r, "st_list", "[{id: 123, name: '123', valid: false, value: 1.234}]") //
                .put(r, "st_map", "{'123': {id: 123, name: '123', valid: false, value: 1.234}}") //
                .put(r, "st_set", "{{id: 123, name: '123', valid: false, value: 1.234}}") //
                .put(r, "st_tuple", "(12345, '12345', 1.2345)") //
                .put(r, "st_udt", "{id: 123, name: '123', valid: false, value: 1.234}");

        // All of the cells have their maximum numeric value. Non-numeric are
        // null.
        r = "min";
        builder.put(r, "key1", "'" + r + "'") //
                .put(r, "st_bigint", String.valueOf(Long.MIN_VALUE)) //
                .put(r, "st_double", String.valueOf(Double.MIN_VALUE)) //
                .put(r, "st_float", String.valueOf(Float.MIN_VALUE)) //
                .put(r, "st_int", String.valueOf(Integer.MIN_VALUE)) //
                .put(r, "st_smallint", String.valueOf(Short.MIN_VALUE)) //
                .put(r, "st_tinyint", String.valueOf(Byte.MIN_VALUE));
        // BigDecimal and BigInteger don't have useful minimums

        // All of the cells have their maximum numeric value. Non-numeric are
        // null.
        r = "max";
        builder.put(r, "key1", "'" + r + "'") //
                .put(r, "st_bigint", String.valueOf(Long.MAX_VALUE)) //
                .put(r, "st_double", String.valueOf(Double.MAX_VALUE)) //
                .put(r, "st_float", String.valueOf(Float.MAX_VALUE)) //
                .put(r, "st_int", String.valueOf(Integer.MAX_VALUE)) //
                .put(r, "st_smallint", String.valueOf(Short.MAX_VALUE)) //
                .put(r, "st_tinyint", String.valueOf(Byte.MAX_VALUE));
        // BigDecimal and BigInteger don't have useful maximums

        // All of the cells that have "nested" values have one, or more if
        // possible.
        r = "nested";
        builder.put(r, "key1", "'" + r + "'")
                //
                .put(r, "st_list",
                        "[{id: 3, name: 'three', valid: false, value: 3.3}, {id: 4, name: 'four',"
                                + " valid: true, value:"
                                + " 4.4}]") //
                .put(r, "st_map",
                        "{'5': {id: 5, name: 'five', valid: false, value: 5.5}, '6': {id: 6, "
                                + "name: 'six', valid: "
                                + "true, value: 6.6}}") //
                .put(r, "st_set",
                        "{{id: 1, name: 'one', valid: false, value: 1.1}, {id: 2, name: 'two', "
                                + "valid: true, value: 2"
                                + ".2}}") //
                .put(r, "st_tuple", "(7, 'seven', 7.7)") //
                .put(r, "st_udt", "{id: 0, name: 'zero', valid: true, value: 0.0}");

        return builder.build();
    }

    /**
     * @return An input test table name.
     */
    public String getTableSrc() {
        return mTableSrc;
    }

    /**
     * @return An input test table name.
     */
    public String getTableCounter() {
        return mTableCounter;
    }

    /**
     * @return The output test table name.
     */
    public String getTableDst() {
        return mTableDst;
    }

    ;

    @Override
    protected void before() throws Throwable {
        super.before();

        // Create a user-defined type.
        execute("CREATE TYPE udt_item ( " //
                + "id int, " //
                + "name text, " //
                + "valid boolean, " //
                + "value double " //
                + ")");

        // Create table dedicated to a counter.
        execute("CREATE TABLE " + getTableCounter() + " ( " //
                + "key1 text," //
                + "st_counter counter, " //
                + "PRIMARY KEY(key1)" + ") ");

        // Representation of all of the columns possible.
        String allTypes = "key1 text PRIMARY KEY, " //
                + "st_ascii ascii, " //
                + "st_bigint bigint, " // 64-bit long
                + "st_blob blob, " //
                + "st_boolean boolean, " //
                // + "st_counter counter, " // Must be in a dedicated table.
                + "st_date date, " //
                + "st_decimal decimal, " //
                + "st_double double, " //
                + "st_float float, " //
                + "st_inet inet, " //
                + "st_int int, " //
                + "st_smallint smallint, " //
                + "st_text text, " //
                + "st_time time, " //
                + "st_timestamp timestamp, " //
                + "st_tinyint tinyint, " //
                + "st_timeuuid timeuuid, " //
                + "st_uuid uuid, " //
                + "st_varchar varchar, " //
                + "st_varint varint, " //
                + "st_list list<frozen <udt_item>>, " //
                + "st_map map<text, frozen <udt_item>>, " //
                + "st_set set<frozen <udt_item>>, " //
                + "st_tuple tuple<int, text, float>, " //
                + "st_udt frozen <udt_item>";

        // Create tables with all of the Cassandra types.
        execute("CREATE TABLE " + getTableSrc() + " ( " + allTypes + ")");
        execute("CREATE TABLE " + getTableDst() + " ( " + allTypes + ")");

        // Use the table to insert all of the rows in the database.
        for (String rowKey : SEXAMPLESRCROWKEYS) {
            StringBuilder insert = new StringBuilder("INSERT INTO " + getTableSrc() + " ("
                    + Joiner.on(",").join
                    (SEXAMPLESRCCOLUMNS)
                    + ") values (");
            for (String column : SEXAMPLESRCCOLUMNS) {
                insert.append(SEXAMPLESRCCELLS.get(rowKey, column)).append(",");
            }
            insert.setLength(insert.length() - 1);
            insert.append(")");
            execute(insert.toString());
        }

        // Update some counters.
        execute("UPDATE " + getTableCounter() //
                + " SET st_counter = st_counter + 1 " //
                + "WHERE key1='example'");
        execute("UPDATE " + getTableCounter() //
                + " SET st_counter = st_counter + 1 " //
                + "WHERE key1='example'");
        execute("UPDATE " + getTableCounter() //
                + " SET st_counter = st_counter + 1 " //
                + "WHERE key1='nested'");
    }
}
