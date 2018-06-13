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
            .withFunction(Indexer.class)
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
            
            session.run("CALL bloom4neo.createIndex");
            
            // 0~>3
            assertTrue(session.run("return bloom4neo.checkReachability(0, 3) as result").next().get("result").asBoolean());
            // 8~>3
            assertTrue(session.run("return bloom4neo.checkReachability(8, 3) as result").next().get("result").asBoolean());
            // 5!~>3
            assertFalse(session.run("return bloom4neo.checkReachability(5, 3) as result").next().get("result").asBoolean());
            // 8!~>9
            assertFalse(session.run("return bloom4neo.checkReachability(8, 9) as result").next().get("result").asBoolean());
            
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
            
            session.run("CALL bloom4neo.createIndex");
            
            // 0~>3
            assertTrue(session.run("return bloom4neo.checkReachability(0, 3) as result").next().get("result").asBoolean());
            // 8~>3
            assertTrue(session.run("return bloom4neo.checkReachability(8, 3) as result").next().get("result").asBoolean());
            // 5!~>3
            assertFalse(session.run("return bloom4neo.checkReachability(5, 3) as result").next().get("result").asBoolean());
            // 8~>9
            assertTrue(session.run("return bloom4neo.checkReachability(8, 9) as result").next().get("result").asBoolean());
            // 9~>7
            assertTrue(session.run("return bloom4neo.checkReachability(9, 7) as result").next().get("result").asBoolean());



        }

    }
    @Test
    public void reachQueryBiggerGraph() throws Throwable {
    	
    	// In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().toConfig() );
            Session session = driver.session() )
        {

            String create = "Create (n01:NodeCycle { name:'n01' })\r\n" + 
            		"Create (n02:NodeCycle { name:'n02' })\r\n" + 
            		"Create (n03:NodeCycle { name:'n03' })\r\n" + 
            		"Create (n04:NodeCycle { name:'n04' })\r\n" + 
            		"Create (n05:NodeNormal { name:'n05' })\r\n" + 
            		"Create (n06:NodeNormal { name:'n06' })\r\n" + 
            		"Create (n07:NodeNormal { name:'n07' })\r\n" + 
            		"Create (n08:NodeNormal { name:'n08' })\r\n" + 
            		"Create (n09:NodeNormal { name:'n09' })\r\n" + 
            		"Create (n10:NodeNormal { name:'n10' })\r\n" + 
            		"Create (n11:NodeNormal { name:'n11' })\r\n" + 
            		"Create (n12:NodeCycle { name:'n12' })\r\n" + 
            		"Create (n13:NodeCycle { name:'n13' })\r\n" + 
            		"Create (n14:NodeCycle { name:'n14' })\r\n" + 
            		"Create (n15:NodeCycle { name:'n15' })\r\n" + 
            		"Create (n16:NodeCycle { name:'n16' })\r\n" + 
            		"Create (n17:NodeCycle { name:'n17' })\r\n" + 
            		"Create (n18:NodeCycle { name:'n18' })\r\n" + 
            		"Create (n19:NodeNormal { name:'n19' })\r\n" + 
            		"Create (n20:NodeNormal { name:'n20' })\r\n" + 
            		"Create (n21:NodeNormal { name:'n21' })\r\n" + 
            		"Create (n22:NodeNormal { name:'n22' })\r\n" + 
            		"Create (n23:NodeNormal { name:'n23' })\r\n" + 
            		"Create (n24:NodeCycle { name:'n24' })\r\n" + 
            		"Create (n25:NodeCycle { name:'n25' })\r\n" + 
            		"Create (n26:NodeNormal { name:'n26' })\r\n" + 
            		"Create (n27:NodeCycle { name:'n27' })\r\n" + 
            		"Create (n28:NodeCycle { name:'n28' })\r\n" + 
            		"Create (n29:NodeNormal { name:'n29' })\r\n" + 
            		"Create (n30:NodeNormal { name:'n30' })\r\n" + 
            		"Create (n31:NodeCycle { name:'n31' })\r\n" + 
            		"Create (n32:NodeCycle { name:'n32' })\r\n" + 
            		"Create (n33:NodeCycle { name:'n33' })\r\n" + 
            		"Create (n01)-[:HAS]->(n03)-[:HAS]->(n04)-[:HAS]->(n02)-[:HAS]->(n01),\r\n" + 
            		"       (n08)-[:HAS]->(n07)-[:HAS]->(n06),\r\n" + 
            		"       (n08)-[:HAS]->(n06)-[:HAS]->(n05)-[:HAS]->(n03),\r\n" + 
            		"       (n04)-[:HAS]->(n10)-[:HAS]->(n11)-[:HAS]->(n09),\r\n" + 
            		"       (n12)-[:HAS]->(n14)-[:HAS]->(n15)-[:HAS]->(n13)-[:HAS]->(n11),\r\n" + 
            		"       (n14)-[:HAS]->(n13),\r\n" + 
            		"       (n13)-[:HAS]->(n12),\r\n" + 
            		"       (n10)-[:HAS]->(n18),\r\n" + 
            		"       (n18)-[:HAS]->(n16)-[:HAS]->(n17)-[:HAS]->(n18),\r\n" + 
            		"       (n17)-[:HAS]->(n19),\r\n" + 
            		"       (n20)-[:HAS]->(n19),\r\n" + 
            		"       (n20)-[:HAS]->(n21)-[:HAS]->(n22),\r\n" + 
            		"       (n20)-[:HAS]->(n22),\r\n" + 
            		"       (n23)-[:HAS]->(n21),\r\n" + 
            		"       (n26)-[:HAS]->(n25)-[:HAS]->(n24)-[:HAS]->(n27)-[:HAS]->(n28)-[:HAS]->(n25),\r\n" + 
            		"       (n28)-[:HAS]->(n29),\r\n" + 
            		"       (n19)-[:HAS]->(n31)-[:HAS]->(n32)-[:HAS]->(n33)-[:HAS]->(n31)";

            session.run(create);
            
            session.run("CALL bloom4neo.createIndex");
            
            // 3~>10
            assertTrue(session.run("return bloom4neo.checkReachability(3, 10) as result").next().get("result").asBoolean());
            // 0!~>19
            assertFalse(session.run("return bloom4neo.checkReachability(0, 19) as result").next().get("result").asBoolean());
            
        }
    	
    }
    @Test
    public void massReachQueryBiggerGraph() throws Throwable {
    	
    	// In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().toConfig() );
            Session session = driver.session() )
        {

            String create = "Create (n01:NodeCycle { name:'n01' })\r\n" + 
            		"Create (n02:NodeCycle { name:'n02' })\r\n" + 
            		"Create (n03:NodeCycle { name:'n03' })\r\n" + 
            		"Create (n04:NodeCycle { name:'n04' })\r\n" + 
            		"Create (n05:NodeNormal { name:'n05' })\r\n" + 
            		"Create (n06:NodeNormal { name:'n06' })\r\n" + 
            		"Create (n07:NodeNormal { name:'n07' })\r\n" + 
            		"Create (n08:NodeNormal { name:'n08' })\r\n" + 
            		"Create (n09:NodeNormal { name:'n09' })\r\n" + 
            		"Create (n10:NodeNormal { name:'n10' })\r\n" + 
            		"Create (n11:NodeNormal { name:'n11' })\r\n" + 
            		"Create (n12:NodeCycle { name:'n12' })\r\n" + 
            		"Create (n13:NodeCycle { name:'n13' })\r\n" + 
            		"Create (n14:NodeCycle { name:'n14' })\r\n" + 
            		"Create (n15:NodeCycle { name:'n15' })\r\n" + 
            		"Create (n16:NodeCycle { name:'n16' })\r\n" + 
            		"Create (n17:NodeCycle { name:'n17' })\r\n" + 
            		"Create (n18:NodeCycle { name:'n18' })\r\n" + 
            		"Create (n19:NodeNormal { name:'n19' })\r\n" + 
            		"Create (n20:NodeNormal { name:'n20' })\r\n" + 
            		"Create (n21:NodeNormal { name:'n21' })\r\n" + 
            		"Create (n22:NodeNormal { name:'n22' })\r\n" + 
            		"Create (n23:NodeNormal { name:'n23' })\r\n" + 
            		"Create (n24:NodeCycle { name:'n24' })\r\n" + 
            		"Create (n25:NodeCycle { name:'n25' })\r\n" + 
            		"Create (n26:NodeNormal { name:'n26' })\r\n" + 
            		"Create (n27:NodeCycle { name:'n27' })\r\n" + 
            		"Create (n28:NodeCycle { name:'n28' })\r\n" + 
            		"Create (n29:NodeNormal { name:'n29' })\r\n" + 
            		"Create (n30:NodeNormal { name:'n30' })\r\n" + 
            		"Create (n31:NodeCycle { name:'n31' })\r\n" + 
            		"Create (n32:NodeCycle { name:'n32' })\r\n" + 
            		"Create (n33:NodeCycle { name:'n33' })\r\n" + 
            		"Create (n01)-[:HAS]->(n03)-[:HAS]->(n04)-[:HAS]->(n02)-[:HAS]->(n01),\r\n" + 
            		"       (n08)-[:HAS]->(n07)-[:HAS]->(n06),\r\n" + 
            		"       (n08)-[:HAS]->(n06)-[:HAS]->(n05)-[:HAS]->(n03),\r\n" + 
            		"       (n04)-[:HAS]->(n10)-[:HAS]->(n11)-[:HAS]->(n09),\r\n" + 
            		"       (n12)-[:HAS]->(n14)-[:HAS]->(n15)-[:HAS]->(n13)-[:HAS]->(n11),\r\n" + 
            		"       (n14)-[:HAS]->(n13),\r\n" + 
            		"       (n13)-[:HAS]->(n12),\r\n" + 
            		"       (n10)-[:HAS]->(n18),\r\n" + 
            		"       (n18)-[:HAS]->(n16)-[:HAS]->(n17)-[:HAS]->(n18),\r\n" + 
            		"       (n17)-[:HAS]->(n19),\r\n" + 
            		"       (n20)-[:HAS]->(n19),\r\n" + 
            		"       (n20)-[:HAS]->(n21)-[:HAS]->(n22),\r\n" + 
            		"       (n20)-[:HAS]->(n22),\r\n" + 
            		"       (n23)-[:HAS]->(n21),\r\n" + 
            		"       (n26)-[:HAS]->(n25)-[:HAS]->(n24)-[:HAS]->(n27)-[:HAS]->(n28)-[:HAS]->(n25),\r\n" + 
            		"       (n28)-[:HAS]->(n29),\r\n" + 
            		"       (n19)-[:HAS]->(n31)-[:HAS]->(n32)-[:HAS]->(n33)-[:HAS]->(n31)";

            session.run(create);
            
            session.run("CALL bloom4neo.createIndex");
            
            System.out.println("-*-*-*-*-*-*-*-*-");
            System.out.println(session.run("match (m:NodeCycle) match (n:NodeNormal) with collect (m) as cycl, collect(n) as norm "
            						+ "return bloom4neo.massReachBoolean(cycl, norm) as result").next().get("result").asBoolean());
            System.out.println("-*-*-*-*-*-*-*-*-");
            assertTrue(true);
            
        }
    	
    }
}