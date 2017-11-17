package se.kth.gureev.graphvsrelational;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import se.kth.gureev.graphvsrelational.model.*;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class FillOutSyntheticDataNeo4j {
    private static String url = "bolt://localhost:7687";
    private static String user = "neo4j";
    private static String pass = "root";
    private static Random random = new Random();
    private static int WORKFLOW_ID = 1;
    private static int OFFSET = 30;
    private static final int LENGTH_OF_DATA = 1024;

    private static Set<Integer> insertedDataNodes = new HashSet<>();

    public static void main(String[] args) throws SQLException {
        for (int i = 50; i < 1000; i++) {
            createExecution("execution" + i, i);
            System.out.println(i);
        }
    }

    private static void createExecution(String name, int id) throws SQLException {
        long start = new java.util.Date().getTime();
        Execution execution = new Execution();
        execution.id = id;
        execution.name = name;
        execution.start = new Timestamp(start);
        execution.workflow = WORKFLOW_ID;

        long finishTime = createSteps(execution);
        execution.end = new Timestamp(finishTime);

        insertExecutionIntoNeo4j(execution);
    }

    private static void insertExecutionIntoNeo4j(Execution execution) {
        List<String> queries = new ArrayList<>();
        StringBuilder query = new StringBuilder();
        query.append("MATCH (w:Workflow {id : ").append(WORKFLOW_ID).append("}) WITH w\n");
        query.append("MERGE (execution:Execution {")
                .append("id : ").append(execution.id).append(',')
                .append("name : '").append(execution.name).append("', ")
                .append("start : ").append(execution.start.getTime()).append(',')
                .append("end : ").append(execution.end.getTime()).append("})")
                .append(" -[:ExecutionOf]-> (w);\n")
        ;
        queries.add(query.toString());

        for (Step s : execution.first) {
            queries = appendStep(s, queries);
            queries.add(linkExecutionToStep(execution, s));
        }

        executeQueries(queries);
    }

    private static void executeQueries(List<String> queries) {
        Driver driver = GraphDatabase.driver( url, AuthTokens.basic(user, pass) );
        Session session = driver.session();

        for (String query : queries)
            session.run(query);

        session.close();
        driver.close();
    }

    private static List<String> appendStep(Step s, List<String> queries) {
        StringBuilder query = new StringBuilder();
        query.append("MERGE (s").append(s.id).append(":Step {")
                .append("id : ").append(s.id).append(',')
                .append("name : '").append(s.name).append("', ")
                .append("start : ").append(s.start.getTime()).append(',')
                .append("end : ").append(s.end.getTime()).append("}); \n");
        queries.add(query.toString());

        if (s.intParameter != null) {
            query = new StringBuilder();
            query.append("MATCH (intType:ParameterType {id : 1}) WITH intType\n");
            query.append("MATCH (s").append(s.id).append(":Step {id : ").append(s.id).append("}) WITH intType").append(", s").append(s.id).append(" \n");
            query.append("MERGE (intType) <-[:ParameterValueType]- (paramI").append(s.id).append(":ParameterValue {")
                    .append("id : ").append(s.intParameter.id).append(',')
                    .append("name : '").append(s.intParameter.name).append("', ")
                    .append("value : ").append(s.intParameter.value).append("}) -[:ParameterValueOf]-> (s").append(s.id).append("); \n");
            queries.add(query.toString());
        }

        if (s.fileParameter != null) {
            query = new StringBuilder();
            query.append("MATCH (fileType:ParameterType {id : 2}) WITH fileType\n");
            query.append("MATCH (s").append(s.id).append(":Step {id : ").append(s.id).append("}) WITH fileType").append(", s").append(s.id).append(" \n");
            query.append("MERGE (fileType) <-[:ParameterValueType]- (paramF").append(s.id).append(":ParameterValue {")
                    .append("id : ").append(s.fileParameter.id).append(',')
                    .append("name : '").append(s.fileParameter.name).append("', ")
                    .append("value : '").append(s.fileParameter.value).append("'}) -[:ParameterValueOf]-> (s").append(s.id).append("); \n");
            queries.add(query.toString());
        }

        for (DataNode in : s.in) {
            query = new StringBuilder();
            query.append("MATCH (s").append(s.id).append(":Step {id : ").append(s.id).append("}) WITH ").append("s").append(s.id).append(" \n");
            if (!insertedDataNodes.contains(in.id)) {
                query = appendDataNode(in, query);
                insertedDataNodes.add(in.id);
                query.append(" -[:InputValueOf]-> (s").append(s.id).append("); \n");
                queries.add(query.toString());
                queries.add(linkDataToInstance(in));
            } else {
                query.append("MATCH (dn").append(in.id).append(":DataNode {id : ").append(in.id).append("}) WITH ")
                        .append("s").append(s.id).append(", ").append("dn").append(in.id).append(" \n")
                        .append("MERGE (dn").append(in.id).append(")");
                query.append(" -[:InputValueOf]-> (s").append(s.id).append("); \n");
                queries.add(query.toString());

            }
        }

        for (DataNode dataNode : s.out) {
            query = new StringBuilder();
            query.append("MATCH (s").append(s.id).append(":Step {id : ").append(s.id).append("}) WITH ").append("s").append(s.id).append(" \n");
            if (!insertedDataNodes.contains(dataNode.id)) {
                query = appendDataNode(dataNode, query);
                insertedDataNodes.add(dataNode.id);
                query.append(" -[:OutputValueOf]-> (s").append(s.id).append("); \n");
                queries.add(query.toString());
                queries.add(linkDataToInstance(dataNode));
            } else {
                query.append("MATCH (dn").append(dataNode.id).append(":DataNode {id : ").append(dataNode.id).append("}) WITH ")
                        .append("s").append(s.id).append(", ").append("dn").append(dataNode.id).append(" \n")
                        .append("MERGE (dn").append(dataNode.id).append(")");
                query.append(" -[:OutputValueOf]-> (s").append(s.id).append("); \n");
                queries.add(query.toString());
            }
        }

        query.append('\n');
        queries.add(linkStepToProcedure(s));
        if (s.next != null) {
            for (Step next : s.next) {
                queries = appendStep(next, queries);
                queries.add(linkSteps(s, next));
            }
        }
        return queries;
    }

    private static String linkStepToProcedure(Step s) {
        StringBuilder query = new StringBuilder();
        query.append("MATCH (s").append(s.id).append(":Step {id : ").append(s.id).append("}) WITH s").append(s.id).append(" \n");
        query.append("MATCH (pr").append(":Procedure {name : '").append(s.name).append("'}) WITH s").append(s.id).append(", pr").append(" \n");
        query.append("MERGE (s").append(s.id).append(") -[:InstanceOf]-> (pr); \n");
        return query.toString();
    }

    private static String linkDataToInstance(DataNode dn) {
        StringBuilder query = new StringBuilder();
        query.append("MATCH (dn").append(dn.id).append(":DataNode {id : ").append(dn.id).append("}) WITH dn").append(dn.id).append(" \n");
        query.append("MATCH (d").append(":Data {name : '").append(dn.name).append("'}) WITH dn").append(dn.id).append(", d").append(" \n");
        query.append("MERGE (dn").append(dn.id).append(") -[:InstanceOf]-> (d); \n");
        return query.toString();
    }

    private static String linkSteps(Step s, Step next) {
        StringBuilder query = new StringBuilder();
        query.append("MATCH (s").append(s.id).append(":Step {id : ").append(s.id).append("}) WITH s").append(s.id).append(" \n");
        query.append("MATCH (s").append(next.id).append(":Step {id : ").append(next.id).append("}) WITH s").append(s.id).append(", s").append(next.id).append(" \n");
        query.append("MERGE (s").append(s.id).append(") -[:Next]-> (s").append(next.id).append("); \n");
        return query.toString();
    }

    private static String linkExecutionToStep(Execution e, Step next) {
        StringBuilder query = new StringBuilder();
        query.append("MATCH (e").append(e.id).append(":Execution {id : ").append(e.id).append("}) WITH e").append(e.id).append(" \n");
        query.append("MATCH (s").append(next.id).append(":Step {id : ").append(next.id).append("}) WITH e").append(e.id).append(", s").append(next.id).append(" \n");
        query.append("MERGE (e").append(e.id).append(") -[:StartsWith]-> (s").append(next.id).append("); \n");
        return query.toString();
    }

    private static StringBuilder appendDataNode(DataNode dn, StringBuilder query) {
        query.append("MERGE (dn").append(dn.id).append(":DataNode {")
                .append("id : ").append(dn.id).append(',')
                .append("name : '").append(dn.name).append("', ")
                .append("value : '").append(dn.bytes).append("'})");

        return query;
    }


    private static long createSteps(Execution e) throws SQLException {
        long stepDuration = getRandomDuration(50);
        long start = e.start.getTime();
        Step s1 = new Step("1. align_warp", e.id * OFFSET + 1, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        e.addFirstStep(s1);
        start += stepDuration;

        DataNode in1 = createDataNode(e.id * OFFSET + 1, "Reference Image", s1.id);
        DataNode in2 = createDataNode(e.id * OFFSET + 2, "Reference Header", s1.id);
        s1.addInput(in1);
        s1.addInput(in2);

        DataNode out1 = createDataNode(e.id * OFFSET + 3, "Warp Params 1", s1.id);
        s1.addOutput(out1);

        s1.intParameter = new IntParameter(e.id * OFFSET + 1, "Anatomy Header 1", 1, s1.id);
        s1.fileParameter = new FileParameter(e.id * OFFSET + 2, "Anatomy Image 1", getRandomBytes(256), s1.id);


        stepDuration = getRandomDuration(50);
        Step s2 = new Step("2. align_warp", e.id * OFFSET + 2, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        e.addFirstStep(s2);
        start += stepDuration;

        s2.addInput(in1);
        s2.addInput(in2);

        DataNode out2 = createDataNode(e.id * OFFSET + 3, "Warp Params 2", s2.id);
        s2.addOutput(out2);

        s2.intParameter = new IntParameter(e.id * OFFSET + 3, "Anatomy Header 2", 2, s2.id);
        s2.fileParameter = new FileParameter(e.id * OFFSET + 4, "Anatomy Image 2", getRandomBytes(256), s2.id);

        stepDuration = getRandomDuration(50);
        Step s3 = new Step("3. align_warp", e.id * OFFSET + 3, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        e.addFirstStep(s3);
        start += stepDuration;

        s3.addInput(in1);
        s3.addInput(in2);

        DataNode out3 = createDataNode(e.id * OFFSET + 4, "Warp Params 3", s3.id);
        s3.addOutput(out3);

        s3.intParameter = new IntParameter(e.id * OFFSET + 5, "Anatomy Header 3", 3, s3.id);
        s3.fileParameter = new FileParameter(e.id * OFFSET + 6, "Anatomy Image 3", getRandomBytes(256), s3.id);

        stepDuration = getRandomDuration(50);
        Step s4 = new Step("4. align_warp", e.id * OFFSET + 4, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        e.addFirstStep(s4);
        start += stepDuration;

        s4.addInput(in1);
        s4.addInput(in2);

        DataNode out4 = createDataNode(e.id * OFFSET + 4, "Warp Params 4", s4.id);
        s4.addOutput(out4);

        s4.intParameter = new IntParameter(e.id * OFFSET + 7, "Anatomy Header 4", 3, s4.id);
        s4.fileParameter = new FileParameter(e.id * OFFSET + 8, "Anatomy Image 4", getRandomBytes(256), s4.id);


        stepDuration = getRandomDuration(50);
        Step s5 = new Step("5. reslice", e.id * OFFSET + 5, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s1.addNextStep(s5);
        start += stepDuration;

        s5.addInput(out1);

        DataNode out5 = createDataNode(e.id * OFFSET + 9, "Resliced Image 1", s5.id);
        DataNode out6 = createDataNode(e.id * OFFSET + 10, "Resliced Header 1", s5.id);
        s5.addOutput(out5);
        s5.addOutput(out6);



        stepDuration = getRandomDuration(50);
        Step s6 = new Step("6. reslice", e.id * OFFSET + 6, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s2.addNextStep(s6);
        start += stepDuration;

        s6.addInput(out2);

        DataNode out7 = createDataNode(e.id * OFFSET + 11, "Resliced Image 2", s6.id);
        DataNode out8 = createDataNode(e.id * OFFSET + 12, "Resliced Header 2", s6.id);
        s6.addOutput(out7);
        s6.addOutput(out8);


        stepDuration = getRandomDuration(50);
        Step s7 = new Step("7. reslice", e.id * OFFSET + 7, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s3.addNextStep(s7);
        start += stepDuration;

        s7.addInput(out3);

        DataNode out9 = createDataNode(e.id * OFFSET + 13, "Resliced Image 3", s7.id);
        DataNode out10 = createDataNode(e.id * OFFSET + 14, "Resliced Header 3", s7.id);
        s7.addOutput(out9);
        s7.addOutput(out10);


        stepDuration = getRandomDuration(50);
        Step s8 = new Step("8. reslice", e.id * OFFSET + 8, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s4.addNextStep(s8);
        start += stepDuration;

        s8.addInput(out4);

        DataNode out11 = createDataNode(e.id * OFFSET + 15, "Resliced Image 4", s8.id);
        DataNode out12 = createDataNode(e.id * OFFSET + 16, "Resliced Header 4", s8.id);
        s8.addOutput(out11);
        s8.addOutput(out12);


        stepDuration = getRandomDuration(50);
        Step s9 = new Step("9. softmean", e.id * OFFSET + 9, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s5.addNextStep(s9);
        s6.addNextStep(s9);
        s7.addNextStep(s9);
        s8.addNextStep(s9);
        start += stepDuration;

        s9.addInput(out5);
        s9.addInput(out6);
        s9.addInput(out7);
        s9.addInput(out8);
        s9.addInput(out9);
        s9.addInput(out10);
        s9.addInput(out11);
        s9.addInput(out12);

        DataNode out13 = createDataNode(e.id * OFFSET + 17, "Atlas Image", s9.id);
        DataNode out14 = createDataNode(e.id * OFFSET + 18, "Atlas Header", s9.id);
        s9.addOutput(out13);
        s9.addOutput(out14);


        stepDuration = getRandomDuration(50);
        Step s10 = new Step("10. slicer", e.id * OFFSET + 10, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s9.addNextStep(s10);
        start += stepDuration;

        s10.addInput(out13);
        s10.addInput(out14);

        DataNode out15 = createDataNode(e.id * OFFSET + 19, "Atlas X Slice", s10.id);
        s10.addOutput(out15);


        stepDuration = getRandomDuration(50);
        Step s11 = new Step("11. slicer", e.id * OFFSET + 11, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s9.addNextStep(s11);
        start += stepDuration;

        s11.addInput(out13);
        s11.addInput(out14);

        DataNode out16 = createDataNode(e.id * OFFSET + 20, "Atlas Y Slice", s11.id);
        s11.addOutput(out16);


        stepDuration = getRandomDuration(50);
        Step s12 = new Step("12. slicer", e.id * OFFSET + 12, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s9.addNextStep(s12);
        start += stepDuration;

        s12.addInput(out13);
        s12.addInput(out14);

        DataNode out17 = createDataNode(e.id * OFFSET + 21, "Atlas Z Slice", s12.id);
        s12.addOutput(out17);



        stepDuration = getRandomDuration(50);
        Step s13 = new Step("13. convert", e.id * OFFSET + 13, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s10.addNextStep(s13);
        start += stepDuration;

        s13.addInput(out15);

        DataNode out18 = createDataNode(e.id * OFFSET + 22, "Atlas X Graphic", s13.id);
        s13.addOutput(out18);


        stepDuration = getRandomDuration(50);
        Step s14 = new Step("14. convert", e.id * OFFSET + 14, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s11.addNextStep(s14);
        start += stepDuration;

        s14.addInput(out16);

        DataNode out19 = createDataNode(e.id * OFFSET + 23, "Atlas Y Graphic", s14.id);

        s14.addOutput(out19);

        stepDuration = getRandomDuration(50);
        Step s15 = new Step("15. convert", e.id * OFFSET + 15, new Timestamp(start), new Timestamp(start + stepDuration), e.id);
        s12.addNextStep(s15);
        start += stepDuration;

        s15.addInput(out17);

        DataNode out20 = createDataNode(e.id * OFFSET + 24, "Atlas Z Graphic", s15.id);
        s15.addOutput(out20);

        return start;

    }

    private static DataNode createDataNode(int id, String inputName, int stepId) throws SQLException {
        DataNode res = new DataNode();
        res.id = id;
        res.name = inputName;
        res.step = stepId;
        res.bytes = generateRandomString(LENGTH_OF_DATA);
        return res;
    }

    private static String getRandomBytes(int i) {
        return  UUID.randomUUID().toString();
    }


    private static long getRandomDuration(int ms) {
        return random.nextInt(ms) * 1000;
    }

    private static String generateRandomString(int length) {
        byte[] array = new byte[length];
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));
        return generatedString;
    }
}
