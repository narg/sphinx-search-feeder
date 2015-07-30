## Sphinx Search Server RT index Feeder from PostgreSQL 


### PostgreSQL Trigger Procedure

Instal PostgreSQL plpython package on CentOS, Oracle Linux
> yum install postgresql94-plpython

Activate plpythonu extension on PostgreSQL
> CREATE EXTENSION plpythonu;

### Python worker to feed Sphinx from PostgreSQL

Install PostgreSQL database adapter for Python on CentOS, Oracle Linux
> yum install python-psycopg2
