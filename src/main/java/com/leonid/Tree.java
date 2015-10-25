package com.leonid;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Created by leonid on 23.09.15.
 */
public class Tree {
    public static GraphDatabaseService GraphDb;
    private static final String DB_PATH = "/home/leonid/bigtree2.db";

    private enum RelTypes implements RelationshipType {
        KNOWS
    }

    public enum Point implements Label {
        NODE
    }

    public static void InitDb(String path) {
        GraphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path);
    }

    public static void CreateNode(int id, String name, Label label) {
        Transaction tx = GraphDb.beginTx();
        try {
            Node newNode = GraphDb.createNode();
            newNode.addLabel(label);
            newNode.setProperty("id", id);
            newNode.setProperty("name", name);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public static void createIndex(Label label) {
        Transaction tx = GraphDb.beginTx();
        IndexDefinition indexDefinition;
        try  {
            final IndexCreator indexCreator = GraphDb.schema().indexFor(label).on("id");
            indexDefinition = indexCreator.create();
            GraphDb.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public static void CreateRelationship(int id1, int id2, Label label1, Label label2) {
        Transaction tx = GraphDb.beginTx();
        try {
            Node point1 = null, point2 = null;
            ResourceIterator<Node> points = GraphDb.findNodes(label1, "id", id1);
            while (points.hasNext()) {
                point1 = points.next();
            }
            points = GraphDb.findNodes(label2, "id", id2);
            while (points.hasNext()) {
                point2 = points.next();
            }
            if(point1 != null && point2 != null) {
                Relationship relationship = point1.createRelationshipTo(point2, RelTypes.KNOWS);
                relationship.setProperty("I KNOW", id1);
                //point2.setProperty("parent", id1);
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public static Node findNode(int id, Label label){
        Transaction tx = GraphDb.beginTx();
        try {
            Node point1 = null;
            ResourceIterator<Node> points = GraphDb.findNodes(label, "id", id);
            while (points.hasNext()) {
                point1 = points.next();
            }
            tx.success();
            return point1;
        } finally {
            tx.finish();
        }
    }

    private static Traverser getFriends(final Node person ) {
        Transaction tx = GraphDb.beginTx();
        try {
            TraversalDescription td = GraphDb.traversalDescription()
                    .breadthFirst()
                    .relationships(RelTypes.KNOWS, Direction.OUTGOING)
                    .evaluator(Evaluators.excludeStartPosition())
                    .evaluator(Evaluators.toDepth(1))
                    .evaluator(Evaluators.fromDepth(1));
            tx.success();
            return td.traverse(person);
        } finally {
            tx.finish();
        }
    }

    public static Node findParents(Node person)
    {
        Node parentNode = null;
        Transaction tx = GraphDb.beginTx();
        try {
        //находим интересующую вершину
            TraversalDescription td = GraphDb.traversalDescription()
                .breadthFirst()
                .relationships(RelTypes.KNOWS, Direction.INCOMING)
                .evaluator(Evaluators.excludeStartPosition())
                .evaluator(Evaluators.toDepth(1))
                .evaluator(Evaluators.fromDepth(1));
        Traverser parents =  td.traverse(person);
        for ( Path parentPath : parents )
        {
            parentNode = parentPath.endNode();
        }
            tx.success();
            return parentNode;
        } finally {
            tx.finish();
        }
    }

    public static void batch(){
        BatchInserter inserter = null;
        try
        {
            Label label = DynamicLabel.label("MAIN");
            Label labelRoot = DynamicLabel.label("ROOT");
            inserter = BatchInserters.inserter(
                    new File("/home/leonid/bigtree2.db").getAbsolutePath());


            RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
            Map<String, Object> properties = new HashMap();
            properties.put("id", 0);
            properties.put("name", "Root");
            long root = inserter.createNode( properties, labelRoot );
                for(int i = 1; i < 1000; i++) {
                    properties.put( "id", i );
                    properties.put("name", "Main"+i);
                    long Node = inserter.createNode(properties, label);
                    Label labelChild = DynamicLabel.label("CHILD" + i);
                    inserter.createRelationship(root, Node, knows, null);
                    //System.out.println(i);
                    for (int id = i * 1000; id < (i + 1) * 1000; id++) {
                        properties.put( "id", id );
                        properties.put("name", "Child"+id);
                        long childNode = inserter.createNode(properties, labelChild);
                        inserter.createRelationship( Node, childNode, knows, null );

                    }
                }
        }
        finally
        {
            if ( inserter != null )
            {
                inserter.shutdown();
            }
        }
    }

    public static void main(String[] args) {
        batch();
        InitDb(DB_PATH);
        Transaction tx = GraphDb.beginTx();
        try {
            Label label = DynamicLabel.label("MAIN");
            Label labelRoot = DynamicLabel.label("ROOT");
/*
            CreateNode(0, "Root", labelRoot);

            for(int i = 1; i < 99; i++){
                CreateNode(i, "Main"+i, label);
                Label labelChild = DynamicLabel.label("CHILD"+i);
                CreateRelationship(0, i, labelRoot, label);
                //System.out.println(i);

                for (int id = i * 100; id < (i + 1) * 100; id++) {
                    CreateNode(id, "Child" + id, labelChild);
                    CreateRelationship(i, id, label, labelChild);

                }

            }



            CreateNode(0, "a");
            CreateNode(1, "b");
            CreateNode(2, "c");
            CreateNode(3, "d");
            CreateNode(4, "e");
            CreateRelationship(0, 1);
            CreateRelationship(0, 2);
            CreateRelationship(1, 3);
            CreateRelationship(1, 4);

            //ExecutionEngine execEngine = new ExecutionEngine(GraphDb);
            String query = "match ()-[r:`KNOWS`*]->() return r";
            Result result = GraphDb.execute(query);
            //String results = execResult.dumpToString();
            //System.out.println(results);

            ExecutionEngine execEngine = new ExecutionEngine(GraphDb);
            ExecutionResult execResult = execEngine.execute("MATCH path=(root:NODE{id:0})-[:KNOWS*]-() RETURN nodes(path) AS result");
            for (Object cell : IteratorUtil.asIterable(execResult.columnAs("result"))) {
                Iterable<Node> nodes = (Iterable<Node>) cell;
                List<Object> ids = new ArrayList<Object>();
                int numer = 0;
                for (Node n : nodes) {
                    ids.add(n.getProperty("id"));
                    numer++;
                }
                System.out.println("Child = "+ids.get(numer-1)+" Parent = " + ids.get(numer-2));
            }



            Result result = GraphDb.execute( "match ()-[r:`KNOWS`*]->() return r" );
            {
                while ( result.hasNext() )
                {
                    Map<String,Object> row = result.next();
                    for ( Map.Entry<String,Object> column : row.entrySet() )
                    {
                        System.out.println(column.getKey()+" === "+column.getValue());
                    }
                }
            }
*/



            Traverser friendsTraverser = getFriends(findNode(999, label));
            for (Path friendPath : friendsTraverser) {
                Iterable<Label> labels = friendPath.endNode().getLabels();
                for(Label labe : labels) {
                    System.out.println("At depth " + friendPath.length() + " => " + friendPath.endNode().getProperty("id")+ " name = "+ friendPath.endNode().getProperty("name") + " parent = " + findParents(friendPath.endNode()).getProperty("id") + " label = " + labe.name());
                }
            }

            tx.success();
        } finally{
            tx.finish();
        }

        System.out.println("HelloWorld");



        GraphDb.shutdown();
    }
}

