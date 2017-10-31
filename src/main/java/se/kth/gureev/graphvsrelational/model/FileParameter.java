package se.kth.gureev.graphvsrelational.model;

/**
 * Created by Nick on 10/30/2017.
 */
public class FileParameter {
    public int id;
    public String name;
    public String value;
    public int step;

    public FileParameter(int id, String name, String value, int step) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.step = step;
    }
}
