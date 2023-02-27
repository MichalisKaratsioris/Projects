import java.io.*;
import java.util.Scanner;

public class AppFeaturesBasic {

    public static boolean argIsCorrect;
    public static TodoListBasic todo;
    public static File todoFile;
    public static Scanner input;

    public AppFeaturesBasic(String[] args) throws IOException {

        input = new Scanner(System.in);
        todo = new TodoListBasic();
        todoFile = new File("todo.txt");

        try {
            Scanner todoFileLines = new Scanner(todoFile);
            while (todoFileLines.hasNextLine()) {
                todo.tasks.add(new TodoTaskBasic(todoFileLines.nextLine()));
            }
            todoFileLines.close();
        } catch (FileNotFoundException e) {
            System.out.println("Could not read the file!");
            e.printStackTrace();
        }

        if (args.length < 1 || "-m".equals(args[0])) {
            todo.printUsage(); // ------------------------------------------------------> 01. PRINT USAGE (BASICS)
        } else {
            if ("-l".equals(args[0])) { // -----------> 02. LIST TASKS (BASICS)
                this.ListOfTasks(todo); //                                            03. PRINT EMPTY LIST (BASICS)
            } else if ("-a".equals(args[0])) { // ----------------------------------> 04. ADDS NEW TASK (BASICS)
                this.AddTask(todo);
            } else if ("-c".equals(args[0]) && "".equals(args[1])) { // ----------------------------------> 05. CHECKS TASK (BASICS)
                System.out.println("The number of task is needed as well.");
                int numOfTask = Integer.parseInt(input.nextLine());
                this.CheckTask(todo, args, numOfTask);
            } else if ("-c".equals(args[0])) {
                int numOfTask = Integer.parseInt(args[1]);
                this.CheckTask(todo, args, numOfTask);
            } else if ("-r".equals(args[0])) { // ----------------------------------> 06. REMOVES TASK (BASICS)
                this.RemoveTask(todo, args);
            } else {
                String[] argsArray = new String[]{"-l", "-a", "-c", "-r"};
                for (String item : argsArray) {
                    if (args[0].length() == 2 && item.equals(args[0])) {
                        argIsCorrect = true;
                    }
                }
                if (!argIsCorrect) {
                    this.WrongArgument(todo); // -------------------------------------------> 07. ARGUMENT ERROR HANDLING (BASICS)
                }
            }
        }
    }

    public void RemoveTask(TodoListBasic todo, String[] strArg) throws IOException {
        TodoListBasic temp = new TodoListBasic();
        if (todo.tasks.size() >= 2) {
            if (strArg.length == 2) {
                if (Character.isDigit(strArg[1].charAt(0)) && Integer.parseInt(strArg[1]) <= todo.tasks.size() && !(strArg[1].equals("0"))) {
                    for (TodoTaskBasic task : todo.tasks) {                                       // Duplicates the current list of tasks while skips the task we want to remove
                        if (task != todo.tasks.get((Integer.parseInt(strArg[1]))-1)) {
                            temp.tasks.add(task);
                        }
                    }
                    BufferedWriter writer = new BufferedWriter(new FileWriter("todo.txt"));
                    for (TodoTaskBasic task : temp.tasks) {                                       // Overwrites the duplicated list on the file
                        writer.write(task.name + "\n");
                    }
                    writer.close();
                } else if (Character.isLetter(strArg[1].charAt(0))) {
                    System.out.println("Second argument is invalid" + "\n");
                    System.out.println(todo.tasks.get(0) + "\n");
                } else if (Integer.parseInt(strArg[1]) > todo.tasks.size()) {
                    System.out.println("""

                             Invalid number of task.
                            """);
                } else {
                    System.out.println("Invalid second argument." + "\n");
                }
            } else {
                System.out.println("Please specify which task from the list you want to remove" + "\n");
            }
        } else {
            System.out.println("There is only one task in the list :");
            System.out.println(todo.tasks.get(0) + "\n");
        }
    }

    public void CheckTask(TodoListBasic todo, String[] strArg, int numOfTask) {
        if (todo.tasks.size() >= 2) {
            if (strArg.length == 2 || numOfTask !=0) {
                if (numOfTask <= todo.tasks.size()) {
                    todo.tasks.get(numOfTask).completed();
                    System.out.println("""

                            Done!
                            """);
                } else if (Character.isDigit(strArg[1].charAt(0)) && Integer.parseInt(strArg[1]) <= todo.tasks.size() && !(strArg[1].equals("0"))) {
                    todo.tasks.get((Integer.parseInt(strArg[1]))-1).completed();
                    System.out.println("""

                            Done!
                            """);
                } else if (Character.isLetter(strArg[1].charAt(0))) {
                    System.out.println("Second argument is invalid" + "\n");
                    System.out.println(todo.tasks.get(0) + "\n");
                } else if (Integer.parseInt(strArg[1]) > todo.tasks.size() || numOfTask > todo.tasks.size()) {
                    System.out.println("""

                             Invalid number of task.
                            """);
                } else {
                    System.out.println("Invalid second argument." + "\n");
                }
            } else {
                System.out.println("Please specify which task from the list you want to check" + "\n");
            }
        } else {
            System.out.println("There is only one task in the list :");
            System.out.println(todo.tasks.get(0) + "\n");
        }
    }

    public void AddTask(TodoListBasic todo){
        Scanner input = new Scanner(System.in);
        System.out.println("\n" + "What task would you like to add?");
        String newTask = input.nextLine();
        todo.tasks.add(new TodoTaskBasic(newTask));
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("todo.txt"));
            for (TodoTaskBasic task : todo.tasks) {
                writer.write(task.name + "\n");
            }
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("""

                Done!
                """);
    }

    public void ListOfTasks(TodoListBasic todo){
        if (todo.tasks.size() == 0) {
            System.out.println("""

                                No todos for today! :)
                                """ + "\n"); // ---------------------------------> 03. PRINTS EMPTY LIST (BASICS)
        } else {
            System.out.println();
            todo.listOfTasks(); // ---------------------------------------> 02. PRINTS LIST OF TASKS (BASICS)
        }
    }

    public void WrongArgument(TodoListBasic todo) { // ------------------------> 07. ARGUMENT ERROR HANDLING (BASICS)
        System.out.println("""

                        Unsupported argument
                        """ + "\n");
        todo.printUsage();
    }
}

