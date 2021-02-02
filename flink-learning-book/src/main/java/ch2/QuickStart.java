package ch2;

/**
 * @author happy
 * @since 2021-02-01
 */
public class QuickStart {
    public static void main(String[] args) {
        String createMud = "创建模板项目的方式有两种方式：\n" +
                "1. Maven archetype命令进行创建\n" +
                "mvn archetype:generate" +
                "-D archetypeGroupId=org.apache.flink" +
                "-D archetypeArtifactId=flink-quickstart-java" +
                "-D archetypeCatalog=https://repository.apache.org" +
                "content/repositories/snapshots" +
                "-D archetypeVersion=1.7.0\n" +
                "2. 通过Flink提供的Quickstart Shell脚本进行创建\n" +
                "curl https://flink.apache.org/q/quickstart-SNAPSHOT.sh | bash -s 1.7.0";
        System.out.println(createMud);
    }
}
