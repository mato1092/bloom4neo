package bloom4neo;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.harness.junit.Neo4jRule;


import static org.junit.Assert.assertThat;
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


            String count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").toString();
            assertTrue(Integer.parseInt(count) == 10);
            //assertTrue(true);

        }
    	
    }

    @Test
    public void indexationSimpleGraphWithCylce() throws Throwable {

        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {

            String create = "Create (n01:NodeCycle { name:'n01' })\n" + 
            		"Create (n02:NodeCycle { name:'n02' })\n" + 
            		"Create (n03:NodeCycle { name:'n03' })\n" + 
            		"Create (n04:NodeCycle { name:'n04' })\n" + 
            		"Create (n05:NodeNormal { name:'n05' })\n" + 
            		"Create (n06:NodeNormal { name:'n06' })\n" + 
            		"Create (n07:NodeNormal { name:'n07' })\n" + 
            		"Create (n08:NodeNormal { name:'n08' })\n" + 
            		"Create (n09:NodeNormal { name:'n09' })\n" + 
            		"Create (n10:NodeNormal { name:'n10' })\n" + 
            		"Create (n11:NodeNormal { name:'n11' })\n" + 
            		"Create (n12:NodeCycle { name:'n12' })\n" + 
            		"Create (n13:NodeCycle { name:'n13' })\n" + 
            		"Create (n14:NodeCycle { name:'n14' })\n" + 
            		"Create (n15:NodeCycle { name:'n15' })\n" + 
            		"Create (n16:NodeCycle { name:'n16' })\n" + 
            		"Create (n17:NodeCycle { name:'n17' })\n" + 
            		"Create (n18:NodeCycle { name:'n18' })\n" + 
            		"Create (n19:NodeNormal { name:'n19' })\n" + 
            		"Create (n20:NodeNormal { name:'n20' })\n" + 
            		"Create (n21:NodeNormal { name:'n21' })\n" + 
            		"Create (n22:NodeNormal { name:'n22' })\n" + 
            		"Create (n23:NodeNormal { name:'n23' })\n" + 
            		"Create (n24:NodeCycle { name:'n24' })\n" + 
            		"Create (n25:NodeCycle { name:'n25' })\n" + 
            		"Create (n26:NodeNormal { name:'n26' })\n" + 
            		"Create (n27:NodeCycle { name:'n27' })\n" + 
            		"Create (n28:NodeCycle { name:'n28' })\n" + 
            		"Create (n29:NodeNormal { name:'n29' })\n" + 
            		"Create (n30:NodeNormal { name:'n30' })\n" + 
            		"Create (n31:NodeCycle { name:'n31' })\n" + 
            		"Create (n32:NodeCycle { name:'n32' })\n" + 
            		"Create (n33:NodeCycle { name:'n33' })\n" + 
            		"\n" + 
            		"Create (n01)-[:HAS]->(n03)-[:HAS]->(n04)-[:HAS]->(n02)-[:HAS]->(n01),\n" + 
            		"       (n08)-[:HAS]->(n07)-[:HAS]->(n06),\n" + 
            		"       (n08)-[:HAS]->(n06)-[:HAS]->(n05)-[:HAS]->(n03),\n" + 
            		"       (n04)-[:HAS]->(n10)-[:HAS]->(n11)-[:HAS]->(n09),\n" + 
            		"       (n12)-[:HAS]->(n14)-[:HAS]->(n15)-[:HAS]->(n13)-[:HAS]->(n11),\n" + 
            		"       (n14)-[:HAS]->(n13),\n" + 
            		"       (n13)-[:HAS]->(n12),\n" + 
            		"       (n10)-[:HAS]->(n18),\n" + 
            		"       (n18)-[:HAS]->(n16)-[:HAS]->(n17)-[:HAS]->(n18),\n" + 
            		"       (n17)-[:HAS]->(n19),\n" + 
            		"       (n20)-[:HAS]->(n19),\n" + 
            		"       (n20)-[:HAS]->(n21)-[:HAS]->(n22),\n" + 
            		"       (n20)-[:HAS]->(n22),\n" + 
            		"       (n23)-[:HAS]->(n21),\n" + 
            		"       (n26)-[:HAS]->(n25)-[:HAS]->(n24)-[:HAS]->(n27)-[:HAS]->(n28)-[:HAS]->(n25),\n" + 
            		"       (n28)-[:HAS]->(n29),\n" + 
            		"       (n19)-[:HAS]->(n31)-[:HAS]->(n32)-[:HAS]->(n33)-[:HAS]->(n31)\n";

            session.run(create);
            session.run("CALL createIndex");


            String count = session.run("match(n) where exists (n.Lin) return count(n)").next().get("count(n)").toString();
            assertTrue(Integer.parseInt(count) == 10);
            //count = session.run("match(n) where exists (n.cyclceId) return count(n)").next().get("count(n)").toString();
            //assertTrue(Integer.parseInt(count) == 4);
            //assertTrue(true);
        }

    }
}