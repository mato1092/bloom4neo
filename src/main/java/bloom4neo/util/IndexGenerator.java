package bloom4neo.util;


import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.ArrayList;
import java.util.Iterator;

public abstract class IndexGenerator {


	public static void generateIndex(GraphDatabaseService dbs) {
		BloomFilter filter = new BloomFilter();

		//todo just get all nodes without lin or lout property -> maybe it will make the indexation a little bit faster
		for (Node n: dbs.getAllNodes()) {
			ArrayList<Long> visited = new ArrayList<>();
			ArrayList<Node> adjacentsList = new ArrayList<>();

			//todo logging
			System.out.println("Start indexation of node " + n.toString());

			//mark the node is indexation was successfully
			n.setProperty("index", true);
			adjacentsList.add(n);


			//adding filter node self Lin
			String nodeId = Long.toString(n.getId());
			String filterValue = "";
			if(n.hasProperty("Lin")){
				filterValue = n.getProperty("Lin").toString();
			}
			n.setProperty("Lin", filter.add(nodeId, filterValue));

			//adding filter node self Lout
			filterValue = "";
			if(n.hasProperty("Lout")){
				filterValue = n.getProperty("Lout").toString();
			}
			n.setProperty("Lout", filter.add(nodeId, filterValue));

			while(adjacentsList.size() > 0){
				Node curNode = adjacentsList.get(0);
				adjacentsList.remove(0);


				Iterable<Relationship> children = curNode.getRelationships(Direction.OUTGOING);
				for(Relationship r : children){
					if(!visited.contains(r.getEndNodeId())){
						adjacentsList.add(r.getEndNode());
						visited.add(r.getEndNodeId());

						//hint: curNode == parent node && r.getEndNode() == current children

						//setting bloom filter value of Lin
						if(r.getEndNode().hasProperty("cycleId")){
							//todo if the node in a cycle --> "superNode"
						} else {
							//set values
							nodeId = Long.toString(n.getId());
							filterValue = "";
							if(r.getEndNode().hasProperty("Lin")){
								filterValue = r.getEndNode().getProperty("Lin").toString();
							}

							//adding to filter
							r.getEndNode()
									.setProperty("Lin", filter.add(nodeId, filterValue));
						}

						//setting bloom filter value of Lout
						if(r.getEndNode().hasProperty("cycleId")){
							//todo if the node in a cycle --> "superNode"
						} else {
							//set values
							nodeId = Long.toString(r.getEndNodeId());
							filterValue = "";
							if(n.hasProperty("Lout")){
								filterValue = n.getProperty("Lout").toString();
							}

							//adding to filter
							n.setProperty("Lout", filter.add(nodeId, filterValue));
						}
					}
				}



			}
		}




	}



}