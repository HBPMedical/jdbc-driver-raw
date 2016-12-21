# JDBC driver for raw-db

## Generating a jar
To generate a jar file use the following command:

`mvn package -DskipTests`

You will find the jar file in:

./target/jdbc-driver-raw-jar-with-dependencies.jar

## JDBC URL
The jdbc url format:
jdbc:raw:\<executor url\>[?auth_url=\<oauth2 url\>]

If the oauth2 url is not specified defaults to: http://localhost:9000/oauth2/access_token

So if your executer url is : 

https://just-ask.raw-labs.com/executer

and your authentication url is :

https://just-ask.raw-labs.com/oauth2/access_token

the jdbc url would be:

jdbc:raw:https://just-ask.raw-labs.com/executer?auth_url=https://just-ask.raw-labs.com/oauth2/access_token

Configuration example for squirrel-sql:
* Name: "Raw"
* Example URL: "jdbc:raw:http://localhost:54321"
* Class Name: "raw.jdbc.RawDriver"

You will need to add the generated jar file as an extra class path in the dialog window for registering the driver.
