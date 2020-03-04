import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/**
 * Tool class to split original file into several smaller files that match
 * relational model and table format.
 *
 * @author Jiaqing Shen
 * @date Created in 2020/2/13
 */
public class SplitFile {
    public static void main(String[] args) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(
            "C:\\Users\\Jiaqing Shen\\Desktop\\algorithm\\src\\2017.csv"));
        PrintStream psTicket = new PrintStream(
            new File("C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\ticket.csv"));
        PrintStream psVehicle = new PrintStream(new File(
            "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\vehicle.csv"));
        PrintStream psIssuer = new PrintStream(
            new File("C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\issuer.csv"));
        PrintStream psIssueBy = new PrintStream(new File(
            "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\issueBy.csv"));
        PrintStream psIssueTo = new PrintStream(new File(
            "C:\\Users\\Jiaqing Shen\\Desktop\\bigdata\\issueTo.csv"));

        Set<String> setVehicle = new HashSet<String>();
        Set<String> setIssuer = new HashSet<String>();

        sc.nextLine();
        while (sc.hasNext()) {
            String[] s = sc.nextLine().split(",");
            // ticket
            psTicket.print(s[0]);
            psTicket.print(",");
            psTicket.print(s[4]);
            psTicket.print(",");
            psTicket.print(s[5]);
            psTicket.print(",");
            psTicket.print(s[8]);
            psTicket.print(",");
            psTicket.print(s[21]);
            psTicket.print(",");
            psTicket.print(s[19]);
            psTicket.print(",");
            psTicket.print(s[22]);
            psTicket.print(",");
            psTicket.print(s[27]);
            psTicket.print(",");
            psTicket.print(s[28]);
            psTicket.println();

            // vehicle
            if (!setVehicle.contains(s[1])) {
                setVehicle.add(s[1]);
                psVehicle.print(s[1]);
                psVehicle.print(",");
                psVehicle.print(s[35]);
                psVehicle.print(",");
                psVehicle.print(s[7]);
                psVehicle.print(",");
                psVehicle.print(s[6]);
                psVehicle.print(",");
                psVehicle.print(s[33]);
                psVehicle.print(",");
                psVehicle.print(s[2]);
                psVehicle.print(",");
                psVehicle.print(s[3]);
                psVehicle.print(",");
                psVehicle.print(s[12]);
                psVehicle.print(",");
                psVehicle.print(s[34]);
                psVehicle.println();
            }

            // issuer
            if (!setIssuer.contains(s[16])) {
                setIssuer.add(s[16]);
                psIssuer.print(s[16]);
                psIssuer.print(",");
                psIssuer.print(s[15]);
                psIssuer.print(",");
                psIssuer.print(s[18]);
                psIssuer.println();
            }

            // issueBy
            psIssueBy.print(s[0]);
            psIssueBy.print(",");
            psIssueBy.print(s[16]);
            psIssueBy.print(",");
            psIssueBy.print(s[17]);
            psIssueBy.println();

            // issueTo
            psIssueTo.print(s[0]);
            psIssueTo.print(",");
            psIssueTo.print(s[1]);
            psIssueTo.println();
        }
    }
}
