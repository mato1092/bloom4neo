package bloom4neo;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.assertTrue;

public class IndexationTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Indexer.class)
            .withProcedure(StronglyConnectedComponentsProc.class);

    
    @Test
    public void indexationSimpleGraph() throws Throwable {
    	
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
            

            session.run("CALL bloom4neo.createIndex");


            int count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").asInt();
            // count should be 10 because there are 10 nodes outside of SCCs
            assertTrue(count == 10);
            count = session.run("match(n) where exists (n.Lout) return count(n)").next().get("count(n)").asInt();
            // count should be 10 because there are 10 nodes outside of SCCs
            assertTrue(count == 10);
            count = session.run("match(n) where exists (n.Ldis) return count(n)").next().get("count(n)").asInt();
            // count should be 10 because there are 10 nodes outside of SCCs
            assertTrue(count == 10);
            count = session.run("match(n) where exists (n.Lfin) return count(n)").next().get("count(n)").asInt();
            // count should be 10 because there are 10 nodes outside of SCCs
            assertTrue(count == 10);
            count = session.run("match(n) where exists (n.cycleMembers) return count(n)").next().get("count(n)").asInt();
            // count should be 0 because there is no SCC representative
            assertTrue(count == 0);
            count = session.run("match(n) where exists (n.cycleRepID) return count(n)").next().get("count(n)").asInt();
            // count should be 0 because there are no SCC members
            assertTrue(count == 0);

        }
    	
    }
	// will not pass without an implemented CycleNodesGenerator: infinite recursion on cycle => stack overflow
    @Test
    public void indexationSimpleGraphWithCycle() throws Throwable {

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
            
            session.run("CALL bloom4neo.createIndex");

            int count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").asInt();
            // count should be 7 because there are 6 nodes outside of SCCs + 1 SCC representative
            assertTrue(count == 7);
            count = session.run("match(n) where exists (n.Lout) return count(n)").next().get("count(n)").asInt();
            // count should be 7 because there are 6 nodes outside of SCCs + 1 SCC representative
            assertTrue(count == 7);
            count = session.run("match(n) where exists (n.Ldis) return count(n)").next().get("count(n)").asInt();
            // count should be 7 because there are 6 nodes outside of SCCs + 1 SCC representative
            assertTrue(count == 7);
            count = session.run("match(n) where exists (n.Lfin) return count(n)").next().get("count(n)").asInt();
            // count should be 7 because there are 6 nodes outside of SCCs + 1 SCC representative
            assertTrue(count == 7);
            count = session.run("match(n) where exists (n.cycleMembers) return count(n)").next().get("count(n)").asInt();
            // count should be 1 because there is 1 SCC representative
            assertTrue(count == 1);
            count = session.run("match(n) where exists (n.cycleRepID) return count(n)").next().get("count(n)").asInt();
            // count should be 4 because 4 nodes with the nodeIDs (4, 5, 6, 9) are SCC members
            assertTrue(count == 4);

        }

    }
    
    @Test
    public void indexationDeleteIndex() throws Throwable {

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
            
            session.run("CALL bloom4neo.createIndex");

            session.run("CALL bloom4neo.deleteIndex");
            
            int count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").asInt();
            // count should be 0
            assertTrue(count == 0);
            count = session.run("match(n) where exists (n.Lout) return count(n)").next().get("count(n)").asInt();
            // count should be 0
            assertTrue(count == 0);
            count = session.run("match(n) where exists (n.Ldis) return count(n)").next().get("count(n)").asInt();
            // count should be 0
            assertTrue(count == 0);
            count = session.run("match(n) where exists (n.Lfin) return count(n)").next().get("count(n)").asInt();
            // count should be 0
            assertTrue(count == 0);
            count = session.run("match(n) where exists (n.cycleMembers) return count(n)").next().get("count(n)").asInt();
            // count should be 0
            assertTrue(count == 0);
            count = session.run("match(n) where exists (n.cycleRepID) return count(n)").next().get("count(n)").asInt();
            // count should be 0
            assertTrue(count == 0);

        }

    }
}