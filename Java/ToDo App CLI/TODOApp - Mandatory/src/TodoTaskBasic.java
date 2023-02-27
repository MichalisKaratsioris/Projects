public class TodoTaskBasic {
    public String name;
    public boolean completed;

    public TodoTaskBasic(String name) {
        this.name = name;
    }
    public void completed() {
        this.completed = true;
    }

    @Override
    public String toString() {
        return name;
    }
}
