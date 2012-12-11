package org.neo4j.example.unmanagedextension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

@Path("/service")
public class MyService {

    @GET
    @Path("/helloworld")
    public String helloWorld() {
        return "Hello World!";
    }

    @GET
    @Path("/friends/{name}")
    public Response getFriends(@PathParam("name") String name, @Context GraphDatabaseService db) 
    		throws IOException {
        ExecutionEngine executionEngine = new ExecutionEngine(db);
        ExecutionResult result = executionEngine.execute(
        				"START person=node:people(name={n}) " +
        				"MATCH person-[:KNOWS]-other RETURN other.name",
        				Collections.<String, Object>singletonMap("n", name));
        List<String> friends = new ArrayList<String>();
        for (Map<String, Object> item : result) {
            friends.add((String) item.get("other.name"));
        }
        ObjectMapper objectMapper = new ObjectMapper();
        return Response.ok().entity(objectMapper.writeValueAsString(friends)).build();
    }

    @GET
    @Path("/volkan")
    public Response getVolkan(@Context GraphDatabaseService db) throws IOException {
        ExecutionEngine executionEngine = new ExecutionEngine(db);
        ExecutionResult result = executionEngine.execute("START person=node(1,2) RETURN person.name");
        List<String> friends = new ArrayList<String>();
        for (Map<String, Object> item : result) {
            friends.add((String) item.get("person.name"));
        }
        ObjectMapper objectMapper = new ObjectMapper();
        return Response.ok().entity(objectMapper.writeValueAsString(friends)).build();
    }

	@POST
	@Path("/volkan")
	public Response postVolkan(@Context GraphDatabaseService db, InputStream is)
			throws IOException {

		Map<String, Object> inputMap	= null;
		List<String> resultJson 		= new ArrayList<String>();

		ObjectMapper mapper = new ObjectMapper();
		try {
			inputMap = convertJsonToMap(is, mapper);
			
			TraversalDescription traversalDesc = Traversal.description();
			traversalDesc = addDepth(inputMap, traversalDesc);
			traversalDesc = addRelationships(inputMap, traversalDesc);

			resultJson = traverse(db, inputMap, traversalDesc);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return Response.ok().entity(mapper.writeValueAsString(resultJson)).build();
	}

	private List<String> traverse(GraphDatabaseService db, Map<String, Object> inputMap, 
							TraversalDescription traversal) {
		List<String> resultJson = new ArrayList<String>();
		Node startNode = db.getNodeById((int) inputMap.get("start_node"));
		
		for (Node node : traversal.traverse(startNode).nodes()) {
			String name = (String) node.getProperty("name");
			resultJson.add(name);
		}
		
		return resultJson;
	}

	private Map<String, Object> convertJsonToMap(InputStream is, ObjectMapper mapper) 
			throws IOException, JsonParseException, JsonMappingException {
		Map<String, Object> inputMap;
		String input 	= readFromStream(is);
		inputMap 		= mapper.readValue(input, Map.class);
		return inputMap;
	}

	private TraversalDescription addDepth(
				Map<String, Object> jsonMap, TraversalDescription traversal) {
		int depth = (Integer) jsonMap.get("depth");
		return traversal.evaluator(Evaluators.atDepth(depth));
	}

	private TraversalDescription addRelationships(Map<String, Object> jsonMap,
			TraversalDescription traversal) {
		for (Map<String, String> rel : (List<Map<String, String>>) jsonMap.get("relationships")) {
			String relationName = rel.get("type");
			Direction direction = findOutDirection(rel);
			traversal = traversal.relationships(
					DynamicRelationshipType.withName(relationName),
					direction);
		}
		return traversal;
	}

	private String readFromStream(InputStream stream) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[1000];
		int wasRead = 0;
		do {
			wasRead = stream.read(buffer);
			if (wasRead > 0) {
				baos.write(buffer, 0, wasRead);
			}
		} while (wasRead > -1);

		return new String(baos.toByteArray());
	}

	private Direction findOutDirection(Map<String, String> rel) {
		String directionString = rel.get("direction");
		Direction direction = null;
		if (directionString.equalsIgnoreCase("IN")) {
			direction = Direction.INCOMING;
		} else {
			direction = Direction.OUTGOING;
		}
		return direction;
	}
}
