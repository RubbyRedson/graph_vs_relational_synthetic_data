package se.kth.gureev.graphvsrelational.queries;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nick on 11/2/2017.
 */
public class QueryNeo4j {
    private static String url = "bolt://localhost:7687";
    private static String user = "neo4j";
    private static String pass = "root";

    private static String allInitialInputsForSpecificOutput = "MATCH (finalOutput:DataNode { id: 2154, name: 'Atlas Z Graphic' })-[:OutputValueOf]->(convertStep:Step)<-[:Next*1..]-(start:Step)<-[:StartsWith]-(e: Execution) WITH e\n" +
            "MATCH (e) -[:StartsWith]-> (initialSteps: Step) <-[:InputValueOf]- (input:DataNode)\n" +
            "RETURN input";

    private static String allInputsForSpecificOutput ="MATCH (finalOutput:DataNode { id: 2154, name: 'Atlas Z Graphic' })-[:OutputValueOf]->(convertStep:Step)<-[:Next*0..]-(s:Step)<-[:InputValueOf]-(input:DataNode) \n" +
            "RETURN input";

    private static String allOutputsForSpecificInput = "MATCH (specificInput:DataNode { id: 2131, value: 'cd88fcd7-e99d-4e27-8e8c-96178cfa4dba' })-[:InputValueOf]->(initialStep:Step)-[:Next*0..]->(s:Step)<-[:OutputValueOf]-(output:DataNode) \n" +
            "RETURN output";

    private static String allResultingOutputsForSpecificInput = "MATCH (specificInput:DataNode { id: 2131, value: 'cd88fcd7-e99d-4e27-8e8c-96178cfa4dba' })-[:InputValueOf]->(initialStep:Step)-[:Next*0..]->(s:Step)<-[:OutputValueOf]-(output:DataNode) \n" +
            "WHERE NOT (s)-[:Next]->()\n" +
            "RETURN output";

    private static String countWorkflowExecutions = "MATCH (w:Workflow {name : 'fMRI'}) <-[:ExecutionOf]- (e:Execution)\n" +
            "RETURN COUNT(e)";

    private static String countWorkflowExecutionsWithTimeLimit = "MATCH (w:Workflow {name : 'fMRI'}) <-[:ExecutionOf]- (e:Execution)\n" +
            "WHERE e.start > 1509456813707 AND e.end < 1509464013001\n" +
            "RETURN COUNT(e)";

    public static void main(String[] args) {
        List<String> queries = new ArrayList<>();
        queries.add(allInitialInputsForSpecificOutput);
        queries.add(allInputsForSpecificOutput);
        queries.add(allOutputsForSpecificInput);
        queries.add(allResultingOutputsForSpecificInput);
        queries.add(countWorkflowExecutions);
        queries.add(countWorkflowExecutionsWithTimeLimit);

        List<List<Long>> results = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            results.add(executeQueries(queries));
        }

        System.out.println(results);
    }

    private static List<Long> executeQueries(List<String> queries) {
        Driver driver = GraphDatabase.driver( url, AuthTokens.basic(user, pass) );
        Session session = driver.session();

        List<Long> result = new ArrayList<>();
        for (String query : queries) {
            long start = System.currentTimeMillis();
            session.run(query);
            result.add(System.currentTimeMillis() - start);
        }

        session.close();
        driver.close();

        return result;
    }
}
