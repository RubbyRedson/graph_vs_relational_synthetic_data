package se.kth.gureev.graphvsrelational;

import com.mysql.cj.jdbc.Blob;

import java.sql.*;
import java.util.Random;

/**
 * Created by Nick on 10/8/2017.
 */
public class FillOutSyntheticDataMySQL {
    private static String url = "jdbc:mysql://localhost:3306/";
    private static String db = "graph_vs_relational1?useSSL=false";
    private static String user = "root";
    private static String pass = "root";
    private static Random random = new Random();

    public static void main(String[] args) throws SQLException {
        for (int i = 101; i < 1000; i++) {
            createExecution("execution" + i);
            System.out.println(i);
        }
    }

    private static void createExecution(String name) throws SQLException {
        long start = new java.util.Date().getTime();
        executeQueryWithTimestamp("insert into execution(name, parent, start) values('"+name+"', (SELECT id from workflow where name = 'fMRI Workflow'), ?);", new Timestamp(start));
        long finishTime = createSteps(name, start);
        executeQueryWithTimestamp("update execution SET end = ? WHERE name = '"+name+"';", new Timestamp(finishTime));
    }

    private static long createSteps(String name, long start) throws SQLException {

        long stepDuration = getRandomDuration(50);
        createStep("1. align_warp", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        createInput("Reference Image", "1. align_warp", name);
        createInput("Reference Header", "1. align_warp", name);
        createOutput("Warp Params 1", "1. align_warp", name);
        createIntegerParameter("Anatomy Header 1", 1, "1. align_warp", name);
        createFileParameter("Anatomy Image 1", "1. align_warp", name);

        stepDuration = getRandomDuration(50);
        createStep("2. align_warp", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        createInput("Reference Image", "2. align_warp", name);
        createInput("Reference Header", "2. align_warp", name);
        createOutput("Warp Params 2", "2. align_warp", name);
        createIntegerParameter("Anatomy Header 2", 2, "2. align_warp", name);
        createFileParameter("Anatomy Image 2", "2. align_warp", name);

        stepDuration = getRandomDuration(50);
        createStep("3. align_warp", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        createInput("Reference Image", "3. align_warp", name);
        createInput("Reference Header", "3. align_warp", name);
        createOutput("Warp Params 3", "3. align_warp", name);
        createIntegerParameter("Anatomy Header 3", 3, "3. align_warp", name);
        createFileParameter("Anatomy Image 3", "3. align_warp", name);

        stepDuration = getRandomDuration(50);
        createStep("4. align_warp", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        createInput("Reference Image", "4. align_warp", name);
        createInput("Reference Header", "4. align_warp", name);
        createOutput("Warp Params 4", "4. align_warp", name);
        createIntegerParameter("Anatomy Header 4", 4, "4. align_warp", name);
        createFileParameter("Anatomy Image 4", "4. align_warp", name);

        stepDuration = getRandomDuration(50);
        createStep("5. reslice", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        createInput("Warp Params 1", "5. reslice", name);
        createOutput("Resliced Image 1", "5. reslice", name);
        createOutput("Resliced Header 1", "5. reslice", name);


        stepDuration = getRandomDuration(50);
        createStep("6. reslice", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("2. align_warp", "6. reslice", name);
        createInput("Warp Params 2", "6. reslice", name);
        createOutput("Resliced Image 2", "6. reslice", name);
        createOutput("Resliced Header 2", "6. reslice", name);

        stepDuration = getRandomDuration(50);
        createStep("7. reslice", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("3. align_warp", "7. reslice", name);
        createInput("Warp Params 3", "7. reslice", name);
        createOutput("Resliced Image 3", "7. reslice", name);
        createOutput("Resliced Header 3", "7. reslice", name);

        stepDuration = getRandomDuration(50);
        createStep("8. reslice", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("4. align_warp", "8. reslice", name);
        createInput("Warp Params 4", "8. reslice", name);
        createOutput("Resliced Image 4", "8. reslice", name);
        createOutput("Resliced Header 4", "8. reslice", name);

        stepDuration = getRandomDuration(50);
        createStep("9. softmean", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("5. reslice", "9. softmean", name);
        linkSteps("6. reslice", "9. softmean", name);
        linkSteps("7. reslice", "9. softmean", name);
        linkSteps("8. reslice", "9. softmean", name);
        createInput("Resliced Image 1", "9. softmean", name);
        createInput("Resliced Header 1", "9. softmean", name);
        createInput("Resliced Image 2", "9. softmean", name);
        createInput("Resliced Header 2", "9. softmean", name);
        createInput("Resliced Image 3", "9. softmean", name);
        createInput("Resliced Header 3", "9. softmean", name);
        createInput("Resliced Image 4", "9. softmean", name);
        createInput("Resliced Header 4", "9. softmean", name);

        createOutput("Atlas Image", "9. softmean", name);
        createOutput("Atlas Header", "9. softmean", name);

        stepDuration = getRandomDuration(50);
        createStep("10. slicer", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("9. softmean", "10. slicer", name);
        createInput("Atlas Image", "10. slicer", name);
        createInput("Atlas Header", "10. slicer", name);
        createOutput("Atlas X Slice", "10. slicer", name);

        stepDuration = getRandomDuration(50);
        createStep("11. slicer", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("9. softmean", "11. slicer", name);
        createInput("Atlas Image", "11. slicer", name);
        createInput("Atlas Header", "11. slicer", name);
        createOutput("Atlas Y Slice", "11. slicer", name);

        stepDuration = getRandomDuration(50);
        createStep("12. slicer", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("9. softmean", "12. slicer", name);
        createInput("Atlas Image", "12. slicer", name);
        createInput("Atlas Header", "12. slicer", name);
        createOutput("Atlas Z Slice", "12. slicer", name);

        stepDuration = getRandomDuration(50);
        createStep("13. convert", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("10. slicer", "13. convert", name);
        createInput("Atlas X Slice", "13. convert", name);
        createOutput("Atlas X Graphic", "13. convert", name);

        stepDuration = getRandomDuration(50);
        createStep("14. convert", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("11. slicer", "14. convert", name);
        createInput("Atlas Y Slice", "14. convert", name);
        createOutput("Atlas Y Graphic", "14. convert", name);

        stepDuration = getRandomDuration(50);
        createStep("15. convert", name, new Timestamp(start), new Timestamp(start + stepDuration));
        start += stepDuration;
        linkSteps("12. slicer", "15. convert", name);
        createInput("Atlas Z Slice", "15. convert", name);
        createOutput("Atlas Z Graphic", "15. convert", name);
        return start;
    }

    private static void linkSteps(String name1, String name2, String executionName) throws SQLException {
        String query = "INSERT INTO step_dependency(previous, next) VALUES(" +
                "(SELECT id from step WHERE name = '" + name1 + "' AND execution = "+"(select id from execution where name = '"+executionName+"')"+"), " +
                "(SELECT id from step WHERE name = '" + name2 + "' AND execution = "+"(select id from execution where name = '"+executionName+"')"+")" +
                ");";

        executeQuery(query);
    }

    private static void createIntegerParameter(String paramName, int value, String stepName, String executionName) throws SQLException {
        String query = "INSERT INTO parametervalue(name, value, step, parameter, type) VALUES('" + paramName +"', "+value+", "+
                "(select id from step where name = '"+stepName+"' AND execution = (select id from execution where name = '"+executionName+"')), (select id from parameter where name = '"+paramName+"'), 1"+");";


        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.execute();
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
    }

    private static void createFileParameter(String paramName, String stepName, String executionName) throws SQLException {
        String query = "INSERT INTO parametervalue(name, value, step, parameter, type) VALUES('" + paramName +"', ?, "+
                "(select id from step where name = '"+stepName+"' AND execution = (select id from execution where name = '"+executionName+"')), (select id from parameter where name = '"+paramName+"'), 1"+");";


        byte[] value = new byte[256];
        random.nextBytes(value);

        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.setString(1, value.toString());
            st.execute();
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
    }

    private static void createInput(String inputName, String stepName, String executionName) throws SQLException {
        String query = "INSERT INTO inputvalue(name, value, step, input) VALUES('" + inputName +"', ?, "+
                "(select id from step where name = '"+stepName+"' AND execution = (select id from execution where name = '"+executionName+"')), (select id from input where name = '"+inputName+"')"+");";

        byte[] value = new byte[256];
        random.nextBytes(value);

        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.setBlob(1, new Blob(value, null));
            st.execute();
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
    }

    private static void createOutput(String output, String stepName, String executionName) throws SQLException {
        String query = "INSERT INTO outputvalue(name, value, step, output) VALUES('" + output +"', ?, "+
                "(select id from step where name = '"+stepName+"' AND execution = (select id from execution where name = '"+executionName+"')), (select id from output where name = '"+output+"')"+");";

        byte[] value = new byte[256];
        random.nextBytes(value);

        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.setBlob(1, new Blob(value, null));
            st.execute();
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
    }

    private static void createStep(String name, String executionName, java.sql.Timestamp start, java.sql.Timestamp end) throws SQLException {
        String query = ("insert into step(name, execution, procedureC, start, end) values('"
                +name+"', "+
                "(select id from execution where name = '"+executionName+"'), "+
                "(select id from proceduretable where name = '"+name+"'), ?,?)");
        executeQueryWithTimestamps(query, start, end);
    }

    private static int getExecutionId(String name, long start) throws SQLException {
        String query = "SELECT id FROM execution WHERE name = '"+name+"' AND start = ?";
        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.setTimestamp(1, new Timestamp(start));
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
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
        return 0;
    }

    private static long getRandomDuration(int ms) {
        return random.nextInt(ms) * 1000;
    }

    private static void executeQueryWithTimestamps(String query, Timestamp start, Timestamp end) throws SQLException {
        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.setTimestamp(1, start);
            st.setTimestamp(2, end);
            st.execute();
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

    }

    private static void executeQuery(String query) throws SQLException {
        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.execute(query);
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

    }

    private static void executeQueryWithTimestamp(String query, Timestamp timestamp) throws SQLException {
        Connection con = null;
        try{
            con = getConnection();
            PreparedStatement st = con.prepareStatement(query);
            st.setTimestamp(1, timestamp);
            st.execute();
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

    }

    private static Connection getConnection() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        return DriverManager.getConnection(url+db, user, pass);
    }
}
