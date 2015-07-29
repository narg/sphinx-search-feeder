
/*
 * Trigger procedure to feed Sphinx Search Server RT index
 * target_table: PostgreSQL table
 * source_table: Sphinx Search Server RT index
 * @author: Necip Arg <neciparg@gmail.com>
 */

Create or replace function public.sphinx_feeder()
    returns trigger
    as $$
    import MySQLdb

    # Open database connection
    db = MySQLdb.connect(host = "127.0.0.1", port = 9306 )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    if TD["event"] == "INSERT" or TD["event"] == "UPDATE":
        id = TD["new"]["id"]

        postgresRecord = plpy.execute("SELECT * FROM <source_table> WHERE id = " + str(id))

        if not postgresRecord:
            plpy.notice(str(id) +  " not found on PostgreSQL!")
            return None

        sphinxQuery = "SELECT * FROM <target_table> WHERE id = " + str(id)

        # execute SQL query using execute() method.
        cursor.execute(sphinxQuery)

        # Fetch a single row using fetchone() method.
        index = cursor.fetchone()

        if not index:
            cols = ""
            vals = ""
            parameters = ()

            for key, value in postgresRecord[0].iteritems():
                if value:
                    cols += key + ","
                    vals += "%s,"
                    parameters += (value,)

            sphinxInsertQuery = """
                INSERT INTO <target_table> (
                    """ + cols.strip(",") + """
                )
                VALUES (
                    """ + vals.strip(",") + """
                )
            """

            cursor.execute(sphinxInsertQuery, parameters)

            # plpy.info(str(id) +  " inserted!")
        else:
            cols = ""
            vals = ""
            parameters = ()

            for key, value in postgresRecord[0].iteritems():
                if value:
                    cols += key + ","
                    vals += "%s,"
                    parameters += (value,)

            sphinxReplaceQuery = """
                REPLACE INTO <target_table> (
                    """ + cols.strip(",") + """
                )
                VALUES (
                    """ + vals.strip(",") + """
                )
            """

            cursor.execute(sphinxReplaceQuery, parameters)

            # plpy.info(str(id) +  " updated!")
    elif TD["event"] == "DELETE":
        id = TD["old"]["id"]

        sphinxDeleteQuery = "DELETE FROM <target_table> WHERE id = %s"
        parameters = (id, )
        cursor.execute(sphinxDeleteQuery, parameters)

        # plpy.info(str(id) +  " deleted!")
    else:
        plpy.error("Unknown event: " +  TD["event"])

    db.close()

    return None
$$ language plpythonu;
