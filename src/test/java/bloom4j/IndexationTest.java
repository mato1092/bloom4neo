package bloom4j;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class IndexationTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    
    @Test
    public void indexation() throws Throwable {
    	
    	// In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
        	// When I use the index procedure to index a node
            System.out.println(session.run( "CALL db.schem()"));

        }
    	
    }
}