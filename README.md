## learning flink by myself
## good good study,day day up.

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <flink.version>1.14.4</flink.version>
        <java.version>11</java.version>
        <hutool.version>5.1.5</hutool.version>
        <flink.redis.version>1.1.5</flink.redis.version>
        <scala.binary.version>2.11</scala.binary.version>
        <!--  二选一  <scala.binary.version>2.12</scala.binary.version>-->
        <maven.compiler.source>${java.version}</maven.compiler.source>
        <maven.compiler.target>${java.version}</maven.compiler.target>
        <log4j.version>2.17.1</log4j.version>
    </properties>

    <modules>
        <module>flink-learning-base</module>
        <module>flink-learning-connectors</module>
        <module>flink-learning-common</module>
        <module>flink-learning-book</module>
        <module>flink-learning-project</module>
    </modules>
    
    <module-desc>
        flink-learning-project 主要是一些小项目或者项目案例
    </module-desc> 

# flink整合springboot
`https://www.jianshu.com/p/1ac7671008ae`