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
    public Response getFriends(@PathParam("name") String name, @Context GraphDatabaseService db) throws IOException {
        ExecutionEngine executionEngine = new ExecutionEngine(db);
        ExecutionResult result = executionEngine.execute("START person=node:people(name={n}) MATCH person-[:KNOWS]-other RETURN other.name",
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

		Map<String, Object> jsonMap	= null;
		List<String> nodes 			= new ArrayList<String>();

		ObjectMapper mapper = new ObjectMapper();
		try {
			String input = readFromStream(is);
			jsonMap = mapper.readValue(input, Map.class);
			int depth = (Integer) jsonMap.get("depth");
			TraversalDescription traversal = Traversal.description().evaluator(
					Evaluators.atDepth(depth));

			for (Map<String, String> rel : (List<Map<String, String>>) jsonMap.get("relationships")) {
				String relationName = rel.get("type");
				Direction direction = findOutDirection(rel);
				traversal = traversal.relationships(
						DynamicRelationshipType.withName(relationName),
						direction);
			}

			Node startNode = db.getNodeById((int) jsonMap.get("start_node"));

			for (Node node : traversal.traverse(startNode).nodes()) {
				String name = (String) node.getProperty("name");
				nodes.add(name);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return Response.ok().entity(mapper.writeValueAsString(nodes)).build();

		// return userDataRead.get("client").toString();
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

	private static Direction findOutDirection(Map<String, String> rel) {
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
