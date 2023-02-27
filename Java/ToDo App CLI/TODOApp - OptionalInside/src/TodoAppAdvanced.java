// ✅  U+2705
// ❌  U+274C

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class TodoAppAdvanced {
    public static AppFeaturesAdvanced todo;
    public static String[] exitCondition = new String[] {""};
    public static Scanner input;

    public static void main(String[] args) throws IOException {

//        todo = new AppFeaturesAdvanced(args);

        input = new Scanner(System.in);
        String[] arg = Arrays.copyOf(args, args.length);
        while(!exitCondition[0].equals("-x")) {
            todo = new AppFeaturesAdvanced(arg);
            System.out.println("What's next?");
            System.out.println("[Type -x to exit]");
            String condition = input.nextLine();
            exitCondition = condition.split(" ");
            arg = Arrays.copyOf(exitCondition, exitCondition.length);
        }
    }
}