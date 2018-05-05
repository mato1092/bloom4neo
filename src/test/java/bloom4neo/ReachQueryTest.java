package bloom4neo;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReachQueryTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Indexer.class)
            .withProcedure(StronglyConnectedComponentsProc.class);

    
    @Test
    public void reachQuerySimpleGraph() throws Throwable {
    	
    	// In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().toConfig() );
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
            

            session.run("CALL createIndex");

            // 0~>3
            assertTrue(session.run("CALL checkReachability(0, 3)").hasNext());
            // 8~>3
            assertTrue(session.run("CALL checkReachability(8, 3)").hasNext());
            // 5!~>3
            assertFalse(session.run("CALL checkReachability(5, 3)").hasNext());
            // 8!~>9
            assertFalse(session.run("CALL checkReachability(8, 9)").hasNext());
            
        }
    	
    }
    @Test
    public void reachQuerySimpleGraphWithCycle() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().toConfig() );
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
                    "create (i)-[:HAS]->(f)\n" +
                    "create (g)-[:HAS]->(j)";

            session.run(create);
            
            session.run("CALL createIndex");
            
            // 0~>3
            assertTrue(session.run("CALL checkReachability(0, 3)").hasNext());
            // 8~>3
            assertTrue(session.run("CALL checkReachability(8, 3)").hasNext());
            // 5!~>3
            assertFalse(session.run("CALL checkReachability(5, 3)").hasNext());;
            // 8~>9
            assertTrue(session.run("CALL checkReachability(8, 9)").hasNext());


        }

    }
}