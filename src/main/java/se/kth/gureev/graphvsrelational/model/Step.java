package se.kth.gureev.graphvsrelational.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nick on 10/30/2017.
 */
public class Step {
    public String name;
    public int id;
    public Timestamp start, end;
    public int execution;
    public List<Step> next;
    public List<DataNode> in;
    public List<DataNode> out;
    public IntParameter intParameter;
    public FileParameter fileParameter;

    public Step(String name, int id, Timestamp start, Timestamp end, int execution) {
        this.name = name;
        this.id = id;
        this.start = start;
        this.end = end;
        this.execution = execution;
    }

    public void addNextStep(Step nextStep) {
        if (next == null) {
            next = new ArrayList<>();
        }
        next.add(nextStep);
    }

    public void addInput(DataNode input) {
        if (in == null) {
            in = new ArrayList<>();
        }
        in.add(input);
    }

    public void addOutput(DataNode input) {
        if (out == null) {
            out = new ArrayList<>();
        }
        out.add(input);
    }
}
