public class TodoTaskAdvanced {
    public String name;
    public boolean completed;

    public TodoTaskAdvanced(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return (completed ? "[x] " : "[ ] ") + name;
    }

}
