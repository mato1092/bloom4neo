package bloom4neo;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.harness.junit.Neo4jRule;

public class CycleNodesGeneratorTest {
	
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Indexer.class)
    		.withProcedure(StronglyConnectedComponentsProc.class);

    @Test
    public void generateCylces() throws Throwable {
    	
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
                    "create (k:Node{name:11})\n" +
                    "create (l:Node{name:12})\n" +
                    "create (m:Node{name:13})\n" +
                    "create (n:Node{name:14})\n" +
                    "create (a)-[:HAS]->(b)-[:HAS]->(c)-[:HAS]->(d)\n" +
                    "create (e)-[:HAS]->(f)-[:HAS]->(g)-[:HAS]->(h)\n" +
                    "create (i)-[:HAS]->(c)\n" +
                    "create (j)-[:HAS]->(e)\n" +
                    "create (i)-[:HAS]->(f)\n" +
                    "create (g)-[:HAS]->(j)" + 
        			"create (k)-[:HAS]->(l)-[:HAS]->(m)-[:HAS]->(n)-[:HAS]->(k)\n";

            session.run(create);
            session.run("CALL generateCylceNodes()");
            

            // currently no assertions
            assertTrue(true);

        }
    	
    	
    	
    }
	

}
