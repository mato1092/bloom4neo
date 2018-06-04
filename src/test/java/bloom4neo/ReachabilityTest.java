package bloom4neo;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.harness.junit.Neo4jRule;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ReachabilityTest {
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Indexer.class)
    		.withProcedure(StronglyConnectedComponentsProc.class);

    //TODO Test cases:
    /*
    - Tree
    - Begin in Cycle ;  End in Cylce
    - Begin in Cylce ; End in NonCycle
    - Begin in NonCycle; Go through cylce ; End in NonCycle
     */


    @Test
    public void reachabilitySimpleGraphSameNode() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            String create = "create (a:Node{name:1})\n" +
                    "create (b:Node{name:2})\n" +
                    "create (c:Node{name:3})\n" +
                    "create (d:Node{name:4})\n" +
                    "create (e:Node{name:5})\n" +
                    "create (f:Node{name:6})\n" +
                    "create (g:Node{name:7})\n" +
                    "create (h:Node{name:8})\n" +
                    "create (i:Node{name:9})\n" +
                    "create (j:Node{name:10})\n" +
                    "create (a)-[:HAS]->(b)-[:HAS]->(c)-[:HAS]->(d)\n" +
                    "create (e)-[:HAS]->(f)-[:HAS]->(g)-[:HAS]->(h)\n" +
                    "create (i)-[:HAS]->(c)\n" +
                    "create (j)-[:HAS]->(e)\n" +
                    "create (i)-[:HAS]->(f)";

            session.run(create);
            session.run("CALL createIndex()");

            Boolean out = session.run("MATCH (n:Node{name:1}) " +
                            "MATCH(m:Node{name:1}) CALL checkReachability(n,m) YIELD out RETURN out")
                    .next()
                    .get("out")
                    .asBoolean();
            assertTrue(out);
        }

    }

    @Test
    public void reachabilitySimpleGraphDirectConnection() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            String create = "create (a:Node{name:1})\n" +
                    "create (b:Node{name:2})\n" +
                    "create (c:Node{name:3})\n" +
                    "create (d:Node{name:4})\n" +
                    "create (e:Node{name:5})\n" +
                    "create (f:Node{name:6})\n" +
                    "create (g:Node{name:7})\n" +
                    "create (h:Node{name:8})\n" +
                    "create (i:Node{name:9})\n" +
                    "create (j:Node{name:10})\n" +
                    "create (a)-[:HAS]->(b)-[:HAS]->(c)-[:HAS]->(d)\n" +
                    "create (e)-[:HAS]->(f)-[:HAS]->(g)-[:HAS]->(h)\n" +
                    "create (i)-[:HAS]->(c)\n" +
                    "create (j)-[:HAS]->(e)\n" +
                    "create (i)-[:HAS]->(f)";

            session.run(create);
            session.run("CALL createIndex()");

            Boolean out = session.run("MATCH (n:Node{name:1}) " +
                    "MATCH(m:Node{name:2}) CALL checkReachability(n,m) YIELD out RETURN out")
                    .next()
                    .get("out")
                    .asBoolean();
            assertTrue(out);
        }

    }

    @Test
    public void reachabilitySimpleGraphNoConnection() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            String create = "create (a:Node{name:1})\n" +
                    "create (b:Node{name:2})\n" +
                    "create (c:Node{name:3})\n" +
                    "create (d:Node{name:4})\n" +
                    "create (e:Node{name:5})\n" +
                    "create (f:Node{name:6})\n" +
                    "create (g:Node{name:7})\n" +
                    "create (h:Node{name:8})\n" +
                    "create (i:Node{name:9})\n" +
                    "create (j:Node{name:10})\n" +
                    "create (a)-[:HAS]->(b)-[:HAS]->(c)-[:HAS]->(d)\n" +
                    "create (e)-[:HAS]->(f)-[:HAS]->(g)-[:HAS]->(h)\n" +
                    "create (i)-[:HAS]->(c)\n" +
                    "create (j)-[:HAS]->(e)\n" +
                    "create (i)-[:HAS]->(f)";

            session.run(create);
            session.run("CALL createIndex()");

            Boolean out = session.run("MATCH (n:Node{name:1}) " +
                    "MATCH(m:Node{name:5}) CALL checkReachability(n,m) YIELD out RETURN out")
                    .next()
                    .get("out")
                    .asBoolean();
            assertFalse(out);;
        }

    }

    @Test
    public void reachabilitySimpleGraphList() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            String create = "create (a:Node{name:1})\n" +
                    "create (b:Node{name:2})\n" +
                    "create (c:Node{name:3})\n" +
                    "create (d:Node{name:4})\n" +
                    "create (e:Node{name:5})\n" +
                    "create (f:Node{name:6})\n" +
                    "create (g:Node{name:7})\n" +
                    "create (h:Node{name:8})\n" +
                    "create (i:Node{name:9})\n" +
                    "create (j:Node{name:10})\n" +
                    "create (a)-[:HAS]->(b)-[:HAS]->(c)-[:HAS]->(d)\n" +
                    "create (e)-[:HAS]->(f)-[:HAS]->(g)-[:HAS]->(h)\n" +
                    "create (i)-[:HAS]->(c)\n" +
                    "create (j)-[:HAS]->(e)\n" +
                    "create (i)-[:HAS]->(f)";

            session.run(create);
            session.run("CALL createIndex()");

            Boolean out = session.run("MATCH (n:Node{name:1}) " +
                    "MATCH(m:Node{name:4}) CALL checkReachability(n,m) YIELD out RETURN out")
                    .next()
                    .get("out")
                    .asBoolean();
            assertTrue(out);
        }

    }


    @Test
    public void rechabilityComplexGraphNodeInSameCycle() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            /*
            * Graph:
            *
            *
            *  v----(10)-----v  +----> (7)----v
            *(9)           (6)-+            (1)-->(2)-->(3)-->(4)
            *  ^----(8) <--+  ^------(5)<---+
            *
            * */
            String create ="Create (n:Node{name:1})-[:EDGE]->(:Node{name:2})-[:EDGE]->(:Node{name:3})-[:EDGE]->(:Node{name:4})\n" +
                            "Create (n)-[:EDGE]->(:Node{name:5})-[:EDGE]->(m:Node{name:6})-[:EDGE]->(:Node{name:7})-[:EDGE]->(n)\n" +
                            "Create (m)-[:EDGE]->(:Node{name:8})-[:EDGE]->(o:Node{name:9})\n" +
                            "Create (p:Node{name:10})-[:EDGE]->(m)\n" +
                            "Create (p)-[:EDGE]->(o)";

            session.run(create);
            session.run("CALL createIndex()");

            Boolean out = session.run("match(n:Node{name:7})\n" +
                    "match(m:Node{name:5})\n" +
                    "CALL checkReachability(n,m) YIELD out\n" +
                    "RETURN out").next().get("out").asBoolean();

            assertTrue(out);
        }

    }

    @Test
    public void rechabilityComplexGraphNodeCycle() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            /*
             * Graph:
             *
             *
             *  v----(10)-----v  +----> (7)----v
             *(9)           (6)-+            (1)-->(2)-->(3)-->(4)
             *  ^----(8) <--+  ^------(5)<---+
             *
             * */
            String create ="Create (n:Node{name:1})-[:EDGE]->(:Node{name:2})-[:EDGE]->(:Node{name:3})-[:EDGE]->(:Node{name:4})\n" +
                    "Create (n)-[:EDGE]->(:Node{name:5})-[:EDGE]->(m:Node{name:6})-[:EDGE]->(:Node{name:7})-[:EDGE]->(n)\n" +
                    "Create (m)-[:EDGE]->(:Node{name:8})-[:EDGE]->(o:Node{name:9})\n" +
                    "Create (p:Node{name:10})-[:EDGE]->(m)\n" +
                    "Create (p)-[:EDGE]->(o)";

            session.run(create);
            session.run("CALL createIndex()");

            Boolean out = session.run("match(n:Node{name:10})\n" +
                    "match(m:Node{name:3})\n" +
                    "CALL checkReachability(n,m) YIELD out\n" +
                    "RETURN out").next().get("out").asBoolean();

            assertTrue(out);
        }

    }
}