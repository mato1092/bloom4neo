package util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class DepthFirstSearch {
	
	private long current;
	private GraphDatabaseService dbs;
	private List<Long> postOrder;
	
	public List<Long> executeDFS(){
		
		for(Node n : dbs.getAllNodes()) {
			
			// if n has not been discovered by DFS
			if( !n.hasProperty("Ldis") ) {
				
				/* if (n not SCC representative and has no incoming relationships)
				 * 	or (n is SCC representative and SCC has no incoming relationships)
				 */
				if( ( !n.hasProperty("ListOfMembers") && n.getDegree(Direction.INCOMING) == 0 )
						|| ( n.hasProperty("ListOfMembers") && (int) n.getProperty("InDegree") == 0) ) {
					
					DFSVisit(n);
					
				}
			}
			
			
		}
		
		return this.postOrder;
	}  
	
	private void DFSVisit(Node n) {
		
		incrementCurrent();
		// if n not member or representative of an SCC
		if ( !n.hasProperty("InCycleOf") && !n.hasProperty("ListOfMembers") ) {
			n.setProperty("Ldis", getCurrent());
			Node v;
			for(Relationship r : n.getRelationships(Direction.OUTGOING)) {
				v = r.getEndNode();
				if(!r.hasProperty("Ldis")) {
					DFSVisit(v);
				}
			}
			addToPostOrder(n.getId());
			incrementCurrent();
			n.setProperty("Lfin", getCurrent());
		}
		else {
			Node cRep = n;
			if(!n.hasProperty("ListOfMembers")) {
				cRep = dbs.getNodeById((long) n.getProperty("InCycleOf"));
			}
			Set<Node> outList = new HashSet<Node>();
			cRep.setProperty("Ldis", getCurrent());
			Node v;
			List<Long> memberList = (List<Long>) cRep.getProperty("ListOfMembers");
			for(Long id : memberList) {
				v = dbs.getNodeById(id);
				v.setProperty("Ldis", getCurrent());
				for(Relationship r : v.getRelationships(Direction.OUTGOING)) {
					outList.add(r.getEndNode());
				}
			}
			for(Node outNode : outList) {
				if(!outNode.hasProperty("Ldis")) {
					DFSVisit(outNode);
				}
			}
			postOrder.add(cRep.getId());
			long cur = incrementCurrent();
			cRep.setProperty("Lfin", cur);
			for(Long id : memberList) {
				dbs.getNodeById(id).setProperty("Lfin", cur);
			}
		}
	}
	
	public DepthFirstSearch(GraphDatabaseService gdbs) {
		
		this.current = 0;
		this.dbs = gdbs;
		this.postOrder = new ArrayList<Long>();
		
	}
	
	private void addToPostOrder(long nodeID) {
		
		this.postOrder.add(nodeID);
		
	}
	
	private long getCurrent() {
		return this.current;
	}
	
	private long incrementCurrent() {
		
		this.current +=1;
		return this.current;
		
	}

}
