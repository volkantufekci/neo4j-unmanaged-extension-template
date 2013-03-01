package org.neo4j.example.unmanagedextension;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.util.StringLogger;

import com.volkan.Utility;
import com.volkan.db.H2Helper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;
import com.volkan.interpartitiontraverse.TraverseHelper;
import com.volkan.interpartitiontraverse.TraverseHelperAsync;


@Path("/service")
public class MyService {

	private final StringLogger neo4jLogger = StringLogger.logger(Utility.buildLogFileName());
	
	@POST
	@Path("/volkan")
	public Response postVolkan(@Context GraphDatabaseService db, InputStream is)
			throws IOException {

		List<String> resultJson 	= new ArrayList<String>();

		ObjectMapper mapper = new ObjectMapper();
		try {
			Map<String, Object> jsonMap	= convertJsonToMap(is, mapper);
			
			TraverseHelper traverseHelper = new TraverseHelper();
			resultJson = traverseHelper.traverse(db, jsonMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return Response.ok().entity(mapper.writeValueAsString(resultJson)).build();
	}

	@POST
	@Path("/volkan_async")
	public Response postVolkanAsync(@Context GraphDatabaseService db, InputStream is)
			throws IOException {

		H2Helper h2Helper 	= null;
		String error 		= "";
		ObjectMapper mapper	= new ObjectMapper();
		Map<String, Object> jsonMap = null;
		try {
			jsonMap	 = convertJsonToMap(is, mapper);
			h2Helper = new H2Helper();
			TraverseHelperAsync traverseHelper = new TraverseHelperAsync(h2Helper);
			traverseHelper.traverse(db, jsonMap);
		} catch (Exception e) {
			error = e.toString();
			neo4jLogger.logMessage(e.toString(), true);
			
			String aapn = "NaN";
			try {
				aapn = Utility.getAutomaticallyAssignedPortNumber();
			} catch (IOException e1) {
				neo4jLogger.logMessage("AAPN is not accessible\n"+e1.toString());
			}
			long jobID = (long) jsonMap.get(JsonKeyConstants.JOB_ID);
			try {
				h2Helper.updateJobWithResults(jobID, aapn+" - jobID:" + jobID + " CAKILDI ");
			} catch (SQLException e1) {
				neo4jLogger.logMessage(aapn +" H2Helper error: " + e1.toString(), true);
			}
		} finally {
			if(h2Helper != null)
				try {
					h2Helper.closeConnection();
				} catch (SQLException e) {
					error += e.toString();
					neo4jLogger.logMessage(e.toString(), true);
				}
		}
		
		if (error.isEmpty()) {
			error = "No error occurred";
			return Response.ok().entity(error).build();
		} 
		else {
			return Response.serverError().entity(error).build();
		}
			
	}
	
	@GET
	@Path("/properties")
	public String getProperties(){
		StringBuilder sb = new StringBuilder();
		FileInputStream in = null;
		try {
//			in = new FileInputStream("conf/neo4j-server.properties");
			in = new FileInputStream("conf/interpartitiontraverse.properties");
			Properties properties = new Properties();
			properties.load(in);
			for (Object string : properties.keySet()) {
				sb.append(string + "=" + properties.getProperty((String) string) + "\n");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return sb.toString();
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> convertJsonToMap(InputStream is, ObjectMapper mapper) 
			throws IOException, JsonParseException, JsonMappingException {
		Map<String, Object> inputMap;
		String input 	= readFromStream(is);
		neo4jLogger.logMessage(input);
		inputMap 		= mapper.readValue(input, Map.class);
		return inputMap;
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
}
