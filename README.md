# JDBC driver for raw-db

To build use the following command:

`mvn package -DskipTests`

you will find the jar file in:

./target/jdbc-driver-raw-jar-with-dependencies.jar

Configuration example for squirrel-sql:
Name: "Raw"
Example URL: "jdbc:raw:http://localhost:54321"
Class Name: "raw.jdbc.RawDriver"


The jdbc url format:
jdbc:raw:<execute url>?auth_url=<oauth2 server>

So for instance if your executer url is : 
https://just-ask.raw-labs.com/executer
and your authentication url is :
https://just-ask.raw-labs.com/oauth2/access_token

the jdbc url would be:
jdbc:raw:https://just-ask.raw-labs.com/executer?auth_url=https://just-ask.raw-labs.com/oauth2/access_token

