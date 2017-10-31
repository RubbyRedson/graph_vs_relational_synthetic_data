package se.kth.gureev.graphvsrelational.model;

/**
 * Created by Nick on 10/30/2017.
 */
public class IntParameter {
    public int id;
    public String name;
    public int value;
    public int step;

    public IntParameter(int id, String name, int value, int step) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.step = step;
    }
}
