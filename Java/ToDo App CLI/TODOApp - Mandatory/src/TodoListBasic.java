import java.util.ArrayList;
import java.util.List;

public class TodoListBasic {

    public List<TodoTaskBasic> tasks;

    public TodoListBasic() {
        super();
        tasks = new ArrayList<>();
    }

    // PRINTS THE MENU
    public void printUsage() {
        String menu = """

                Command Line Todo Application
                =============================

                Command Line Arguments:
                    -l   Lists all the tasks
                    -a   Adds a new task
                    -r   Removes an task
                    -c   Completes an task
                """;

        System.out.println(menu);
    }

    //    PRINTS LIST OF TASKS
    public void listOfTasks() {
        StringBuilder result = new StringBuilder();
        for(int i = 0; i < tasks.size(); i++) {
            result.append(i + 1).append(" - ").append(tasks.get(i).toString()).append("\n");
        }
        System.out.println(result);
    }
}
