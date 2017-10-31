package se.kth.gureev.graphvsrelational.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nick on 10/30/2017.
 */
public class Execution {
    public String name;
    public int id;
    public Timestamp start, end;
    public int workflow;
    public List<Step> first;

    public void addFirstStep(Step step) {
        if (first == null) {
            first = new ArrayList<>();
        }
        first.add(step);
    }
}
