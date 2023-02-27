import java.util.ArrayList;
import java.util.List;

public class TodoListAdvanced {

    public List<TodoTaskAdvanced> tasks;

    public TodoListAdvanced() {
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
                    -r   Removes a task
                    -c   Completes a task
                    -u   Unchecks a task
                """;

        System.out.println(menu);
    }

    //    PRINTS LIST OF CHECKED/UNCHECKED TASKS
    public void listOfTasks() {
        StringBuilder result = new StringBuilder();
        for(int i = 0; i < tasks.size(); i++) {
            result.append(i + 1).append(". ").append(tasks.get(i).name).append("\n");
        }
        System.out.println(result);
    }
}