package se.kth.gureev.graphvsrelational.queries;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nick on 11/4/2017.
 */
public class QueryMySQL {
    private static String url = "jdbc:mysql://localhost:3306/";
    private static String db = "graph_vs_relational1?useSSL=false";
    private static String user = "root";
    private static String pass = "root";

    private static String allInitialInputsForSpecificOutput = "SELECT * FROM graph_vs_relational1.inputvalue outerV, \n" +
            "( \n" +
            "SELECT st.id as s0, st1.previous as s1, st2.previous as s2, st3.previous as s3, st4.previous as s4 FROM graph_vs_relational1.outputvalue ov \n" +
            "JOIN graph_vs_relational1.step st ON ov.step = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st1 ON st1.next = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st2 ON st2.next = st1.previous\n" +
            "JOIN graph_vs_relational1.step_dependency st3 ON st3.next = st2.previous\n" +
            "JOIN graph_vs_relational1.step_dependency st4 ON st4.next = st3.previous\n" +
            "WHERE ov.name = 'Atlas Z Graphic' and ov.id = 979\n" +
            ") AS dependantSteps\n" +
            "WHERE outerV.step = s4\n";

    private static String allInputsForSpecificOutput ="SELECT * FROM graph_vs_relational1.inputvalue outerV, \n" +
            "( \n" +
            "SELECT st.id as s0, st1.previous as s1, st2.previous as s2, st3.previous as s3, st4.previous as s4 FROM graph_vs_relational1.outputvalue ov \n" +
            "JOIN graph_vs_relational1.step st ON ov.step = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st1 ON st1.next = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st2 ON st2.next = st1.previous\n" +
            "JOIN graph_vs_relational1.step_dependency st3 ON st3.next = st2.previous\n" +
            "JOIN graph_vs_relational1.step_dependency st4 ON st4.next = st3.previous\n" +
            "WHERE ov.name = 'Atlas Z Graphic' and ov.id = 979\n" +
            ") AS dependantSteps\n" +
            "WHERE outerV.step = s0 OR outerV.step = s1 OR outerV.step = s2 OR outerV.step = s3 OR outerV.step = s4\n";

    private static String allOutputsForSpecificInput = "SELECT * FROM graph_vs_relational1.outputvalue outerV, \n" +
            "( \n" +
            "SELECT st.id as s0, st1.previous as s1, st2.previous as s2, st3.previous as s3, st4.previous as s4 FROM graph_vs_relational1.inputvalue iv \n" +
            "JOIN graph_vs_relational1.step st ON iv.step = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st1 ON st1.previous = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st2 ON st2.previous = st1.next\n" +
            "JOIN graph_vs_relational1.step_dependency st3 ON st3.previous = st2.next\n" +
            "JOIN graph_vs_relational1.step_dependency st4 ON st4.previous = st3.next\n" +
            "WHERE iv.id = 1400\n" +
            ") AS dependantSteps\n" +
            "WHERE outerV.step = s0 OR outerV.step = s1 OR outerV.step = s2 OR outerV.step = s3 OR outerV.step = s4";

    private static String allResultingOutputsForSpecificInput = "SELECT * FROM graph_vs_relational1.outputvalue outerV, \n" +
            "( \n" +
            "SELECT st.id as s0, st1.previous as s1, st2.previous as s2, st3.previous as s3, st4.previous as s4 FROM graph_vs_relational1.inputvalue iv \n" +
            "JOIN graph_vs_relational1.step st ON iv.step = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st1 ON st1.previous = st.id\n" +
            "JOIN graph_vs_relational1.step_dependency st2 ON st2.previous = st1.next\n" +
            "JOIN graph_vs_relational1.step_dependency st3 ON st3.previous = st2.next\n" +
            "JOIN graph_vs_relational1.step_dependency st4 ON st4.previous = st3.next\n" +
            "WHERE iv.id = 1400\n" +
            ") AS dependantSteps\n" +
            "WHERE outerV.step = s4";

    private static String countWorkflowExecutions = "SELECT COUNT(*) FROM execution WHERE parent = (SELECT id FROM workflow WHERE name = 'fMRI Workflow')\n";

    private static String countWorkflowExecutionsWithTimeLimit = "SELECT COUNT(*) FROM execution WHERE parent = (SELECT id FROM workflow WHERE name = 'fMRI Workflow') AND\n" +
            "start > '2017-10-09 16:26:29' AND end < '2017-11-01 20:13:23'";

    public static void main(String[] args) throws SQLException {
        List<String> queries = new ArrayList<>();
        queries.add(allInitialInputsForSpecificOutput);
        queries.add(allInputsForSpecificOutput);
        queries.add(allOutputsForSpecificInput);
        queries.add(allResultingOutputsForSpecificInput);
        queries.add(countWorkflowExecutions);
        queries.add(countWorkflowExecutionsWithTimeLimit);

        List<List<Long>> results = new ArrayList<>();

        for (int i = 0; i < 50  ; i++) {
            results.add(executeQueries(queries));
        }

        System.out.println(results);
        saveToCSV(results);
    }

    private static void saveToCSV(List<List<Long>> results) {
        //Get the file reference
        Path path = Paths.get("mysql.csv");

        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            for (List<Long> res : results) {
                for (Long each : res) {
                    writer.write(each + ";");
                }
                writer.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Long> executeQueries(List<String> queries) throws SQLException {
        Connection con = null;
        List<Long> results = new ArrayList<>();
        try{
            con = DriverManager.getConnection(url+db, user, pass);
            for (String query : queries) {
                PreparedStatement st = con.prepareStatement(query);
                long start = System.currentTimeMillis();
                st.execute();
                results.add(System.currentTimeMillis() - start);
            }
        }
        catch (SQLException s){
            throw s;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            con.close();
        }
        return results;
    }

}
