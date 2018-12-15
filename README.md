# Ad-Hoc-Java
This is a Extended-SQL evaluation simulation.

## GOAL
Standard-SQL has a drawback in group by clause which ties the group and aggregation function. Extended-SQL can solve this problem by adding several grouping variables to define the range of each group.

## DEPENDENCY Libraries
JavaSE 1.8
postgresql-9.4.1211.jar

## DEPLOY
Download the source code from Repo: https://github.com/yli236/ESQL.git
Download the postgresql-9.4.1211.jar driver from git
Build path->Configure Build path->add Extend Jars and choose the .jar package above
Start your postgreSQL service
Go to QPE.java and modify your SQL administration information at line 6-8

## USAGE
Create a .esql file and put in the query information in the order of select, number, where, groupby, Fvect, suchthat,
having.
Then simply run the main.java and you will get the query in written in java.
