import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtilTest {
    public static void main(String[] args) {
        String str = "123|232dsds.232.123";
        String REGEX = "\\D";
        Pattern compile = Pattern.compile(REGEX);
        Matcher matcher = compile.matcher(str);

        String trim = matcher.replaceAll("").trim();

        System.out.println(trim);

    }
}
