java: pgtest.java
	javac pgtest.java
	java -Djava.security.auth.login.config=login.config -classpath ./postgresql-8.4-703.jdbc4.jar:. pgtest

jar: pgtest.class Prompter.class QueryBuilder.class ResultPrinter.class SQLExecutor.class
	jar cvfe executable.jar pgtest *.class

run-jar: executable.jar
	java -jar executable.jar

clean:
	rm -rf *.class
	rm -rf *.jar

sql:
	psql < query1.sql
	psql < query2.sql
	psql < query3.sql
	psql < query4.sql
	psql < query5.sql
	psql < query6.sql
	psql < query7.sql
	psql < query8.sql
	psql < query9.sql
	psql < query10.sql

