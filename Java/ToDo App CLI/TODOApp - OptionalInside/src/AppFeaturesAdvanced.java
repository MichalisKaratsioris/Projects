import java.io.*;
import java.util.Scanner;

public class AppFeaturesAdvanced {

    public static boolean argIsCorrect;
    public static TodoListAdvanced todo;
    public static File todoFile;
    public static Scanner input;

    public AppFeaturesAdvanced(String[] args) throws IOException {

        input = new Scanner(System.in);
        todo = new TodoListAdvanced();
        todoFile = new File("todo.txt");

        try {
            Scanner todoFileLines = new Scanner(todoFile);
            while (todoFileLines.hasNextLine()) {
                todo.tasks.add(new TodoTaskAdvanced(todoFileLines.nextLine()));
            }
            todoFileLines.close();
        } catch (FileNotFoundException e) {
            System.out.println("Could not read the file!");
            e.printStackTrace();
        }
        if (args.length < 1) {
            todo.printUsage(); // ------------------------------------------------------> 01. PRINT USAGE (BASICS)
        } else {
            if ("-l".equals(args[0])) { // -----------> 02. LIST TASKS (BASICS)
                this.ListOfTasks(todo); //                                            03. PRINT EMPTY LIST (BASICS)
            } else if ("-a".equals(args[0])) { // ----------------------------------> 04. ADDS NEW TASK (BASICS)
                if (args.length == 1) {
                    this.errorHandlingNoTask(todo);
                } else {
                    this.AddTask(todo, args);
                }
            } else if ("-c".equals(args[0])) { // ----------------------------------> 05. CHECKS TASK (BASICS)
                if (args.length == 1) {
                    this.errorHandlingMissingIndex(todo, "check");
                } else if (!Character.isDigit(args[1].charAt(0))) {
                    this.errorHandlingInvalidArgumentType(todo, "check");
                } else if (Integer.parseInt(args[1]) > todo.tasks.size()) {
                    this.errorHandlingIndexNotFound(todo, "check");
                } else {
                    this.CheckTask(todo, args);
                }
            } else if ("-r".equals(args[0])) { // ----------------------------------> 06. REMOVES TASK (BASICS)
                if (args.length == 1) {
                    this.errorHandlingMissingIndex(todo, "remove");
                } else if (!Character.isDigit(args[1].charAt(0))) {
                    this.errorHandlingInvalidArgumentType(todo, "remove");
                } else if (Integer.parseInt(args[1]) > todo.tasks.size()) {
                    this.errorHandlingIndexNotFound(todo, "remove");
                } else {
                    this.RemoveTask(todo, args);
                }
            } else if ("-u".equals(args[0])) { // ----------------------------------> 05. CHECKS TASK (BASICS)
                if (args.length == 1) {
                    this.errorHandlingMissingIndex(todo, "uncheck");
                } else if (!Character.isDigit(args[1].charAt(0))) {
                    this.errorHandlingInvalidArgumentType(todo, "uncheck");
                } else if (Integer.parseInt(args[1]) > todo.tasks.size()) {
                    this.errorHandlingIndexNotFound(todo, "uncheck");
                } else {
                    this.UnCheckTask(todo);
                }
            } else {
                String[] argsArray = new String[]{"-l", "-a", "-c", "-r"};
                for (String item : argsArray) {
                    if (args[0].length() == 2 && item.equals(args[0])) {
                        argIsCorrect = true;
                        break;
                    }
                }
                if (!argIsCorrect) {
                    this.argumentErrorHandling(todo); // -------------------------------------------> 07. ARGUMENT ERROR HANDLING (BASICS)
                }
            }
        }
    }

    public void RemoveTask(TodoListAdvanced todo, String[] strArg) throws IOException {
        TodoListAdvanced temp = new TodoListAdvanced();
        int numOfTask = Integer.parseInt(strArg[1]) - 1;
        if (todo.tasks.size() >= 1) {
            if (strArg.length == 2 && numOfTask != 0) {
                for (TodoTaskAdvanced task : todo.tasks) {                                       // Duplicates the current list of tasks while skips the task we want to remove
                    if (task != todo.tasks.get((Integer.parseInt(strArg[1])) - 1)) {
                        temp.tasks.add(task);
                    }
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter("todo.txt"));
                for (TodoTaskAdvanced task : temp.tasks) {                                       // Overwrites the duplicated list on the file
                    writer.write(task.name + "\n");
                }
                writer.close();
                System.out.println("""

                        Removed!
                        """);
            }
        } else {
            System.out.println("There is only one task in the list :");
            System.out.println(todo.tasks.get(0) + "\n");
        }
    }

    public void CheckTask(TodoListAdvanced todo, String[] args) throws IOException {
        TodoListAdvanced temp = new TodoListAdvanced();
        int numOfTask = Integer.parseInt(args[1]) - 1;
        if (todo.tasks.size() >= 1) {
            for (TodoTaskAdvanced task : todo.tasks) {                               // Duplicates the current list of tasks while skips the task we want to remove
                if (task != todo.tasks.get(numOfTask)) {
                    if (task.name.charAt(1) != 'x') {
                        task.name = "[ ]" + task.name.substring(3);
                        temp.tasks.add(task);
                    } else {
                        task.name = "[x]" + task.name.substring(3);
                        temp.tasks.add(task);
                    }
                } else {
                    task.name = "[x]" + task.name.substring(3);
                    temp.tasks.add(task);
                }
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter("todo.txt"));
            for (TodoTaskAdvanced task : temp.tasks) {                                       // Overwrites the duplicated list on the file
                writer.write(task.name + "\n");
            }
            writer.close();
            System.out.println("""

                        Checked!
                        """);
        }
    }

    public void UnCheckTask(TodoListAdvanced todo) throws IOException {
        TodoListAdvanced temp = new TodoListAdvanced();
        if (todo.tasks.size() >= 1) {
            for (TodoTaskAdvanced task : todo.tasks) {                               // Duplicates the current list of tasks while skips the task we want to remove
                task.name = "[ ]" + task.name.substring(3);
                temp.tasks.add(task);
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter("todo.txt"));
            for (TodoTaskAdvanced task : temp.tasks) {                                       // Overwrites the duplicated list on the file
                writer.write(task.name + "\n");
            }
            writer.close();
            System.out.println("""

                        Unchecked!
                        """);
        }
    }

    public void AddTask(TodoListAdvanced todo, String[] args) throws IOException {
        TodoListAdvanced temp = new TodoListAdvanced();
        if (todo.tasks.size() >= 1) {
            if (args.length == 2) {
                temp.tasks.addAll(todo.tasks);
                temp.tasks.add(new TodoTaskAdvanced("[ ] " + args[1]));
                BufferedWriter writer = new BufferedWriter(new FileWriter("todo.txt"));
                for (TodoTaskAdvanced task : temp.tasks) {                                       // Overwrites the duplicated list on the file
                    writer.write(task.name + "\n");
                }
                writer.close();
                System.out.println("""

                        Added!
                        """);
            }
        }
    }

    public void ListOfTasks(TodoListAdvanced todo){
        if (todo.tasks.size() == 0) {
            System.out.println("""

                                No todos for today! :)
                                """ + "\n"); // ---------------------------------> 03. PRINTS EMPTY LIST (BASICS)
        } else {
            System.out.println();
            todo.listOfTasks(); // ---------------------------------------> 08. PRINTS LIST OF MARKED TASKS (ADVANCED)
        }
    }

    public void argumentErrorHandling(TodoListAdvanced todo) { // ------------------------> 07. ARGUMENT ERROR HANDLING (BASICS)
        System.out.println("""

                        Unsupported argument
                        """ + "\n");
        todo.printUsage();
    }

    public void errorHandlingMissingIndex(TodoListAdvanced todo, String str) { // ------------------------> 10, 11. CHECK TASK ERROR HANDLING (ADVANCED)
        System.out.println("\n Unable to " + str + ": no index provided" + "\n");
        todo.printUsage();
    }

    public void errorHandlingIndexNotFound(TodoListAdvanced todo, String str) { // ------------------------> 10, 11. CHECK TASK ERROR HANDLING (ADVANCED)
        System.out.println("\n Unable to " + str + ": index is out of bound" + "\n");
        todo.printUsage();
    }

    public void errorHandlingInvalidArgumentType(TodoListAdvanced todo, String str) { // ------------------------> 10, 11. CHECK TASK ERROR HANDLING (ADVANCED)
        System.out.println("\n Unable to " + str + ": index is not a number" + "\n");
        todo.printUsage();
    }

    public void errorHandlingNoTask(TodoListAdvanced todo) { // ------------------------> 09. CHECK TASK ERROR HANDLING (ADVANCED)
        System.out.println("""

                Unable to add: no task provided
                        """ + "\n");
        todo.printUsage();
    }

}
