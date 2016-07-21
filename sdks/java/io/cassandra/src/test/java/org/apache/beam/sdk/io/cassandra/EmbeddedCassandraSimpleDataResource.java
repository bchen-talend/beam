package org.apache.beam.sdk.io.cassandra;

import org.junit.Before;

import java.util.Random;

/**
 *
 */
public class EmbeddedCassandraSimpleDataResource extends EmbeddedCassandraResource {

    private String src;
    private String dest;
    private int nbline;

    public EmbeddedCassandraSimpleDataResource(String keySpace, String src, String dest, int
            nbline) {
        super(keySpace);
        this.src = src;
        this.dest = dest;
        this.nbline = nbline;
    }

    public String getSrc() {
        return src;
    }

    public String getDest() {
        return dest;
    }

    public int getNbline() {
        return nbline;
    }


    @Before
    protected void before() throws Throwable {
        super.before();

        execute("create TABLE " + getSrc() + "( id text, name text, age int, primary KEY (id, "
                + "name))");
        execute("create TABLE " + getDest() + "( id text, name text, age int, primary KEY (id, "
                + "name))");
        Random random = new Random();
        String[] ids = new String[]{"aaa", "bbb", "ccc"};
        for (int i = 0; i < getNbline(); i++) {
            execute("insert into " + getSrc() + "(id, name, age) values ('"
                    + ids[random.nextInt(3)] + "', 'n" + i
                    + "', " + (10 + i) + ")");
        }
    }
}
