package bloom4neo.util;

import org.neo4j.graphdb.Node;

public class NodePairResult
{
    public final Node a;
    public final Node b;
    public NodePairResult( Node a, Node b )
    {
        this.a = a;
        this.b = b;
    }
}