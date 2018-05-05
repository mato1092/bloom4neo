package bloom4neo;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;


import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexationTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Indexer.class);

    
    @Test
    public void indexationSimpleGraph() throws Throwable {
    	
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
            

            session.run("CALL createIndex");


            String count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").toString();
            assertTrue(Integer.parseInt(count) == 10);

        }
    	
    }
/*	//shouldnt be tested without CycleNodesGenerator - infinite loop on cycle
    @Test
    public void indexationSimpleGraphWithCycle() throws Throwable {

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
                    "create (i)-[:HAS]->(f)\n" +
                    "create (g)-[:HAS]->(j)";

            session.run(create);
            
            session.run("CALL createIndex");

            String count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").toString();
            assertTrue(Integer.parseInt(count) == 10);
            count = session.run("match(n) where exists (n.cyclceId) return count(n)").next().get("count(n)").toString();
            assertTrue(Integer.parseInt(count) == 4);

        }

    }*/
}