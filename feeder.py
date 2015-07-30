
# Python worker to feed Sphinx Search Server RT index
# target_table: PostgreSQL table
# source_table: Sphinx Search Server RT index
# @author: Necip Arg <neciparg@gmail.com>

import sys
import time
import psycopg2
import MySQLdb
import ConfigParser

# Get current time
started_at = time.time()

# Get connection parameters
config = ConfigParser.ConfigParser()
config.read("./config/local.ini")

# Open PostgreSQL conection
postgresHost = str(config.get("postgres", "host"))
postgresDatabase = str(config.get("postgres", "database"))
postgresUser = str(config.get("postgres", "user"))
postgresPassword = str(config.get("postgres", "password"))

# print "Connecting to Postgres\n	->%s" % postgresHost
try:
    postgresConn = psycopg2.connect(host=postgresHost, database=postgresDatabase,
                                    user=postgresUser, password=postgresPassword)
    postgresConn.autocommit = True
except:
    print "Cannot connect to PostgreSQL\n	->%s" % postgresHost
    print "Unexpected error:", sys.exc_info()[0]
    raise

postgresCursor = postgresConn.cursor()
postgresCoCursor = postgresConn.cursor()

# Open SphinxSearchServer connection
sphinxHost = str(config.get("sphinx", "host"))
sphinxPort = int(config.get("sphinx", "port"))

# print "Connecting to Sphinx\n	->%s" % sphinxHost
try:
    sphinxConn = MySQLdb.connect(host=sphinxHost, port=sphinxPort)
except:
    print "Cannot connect to Sphinx\n ->%s" % sphinxHost
    print "Unexpected error:", sys.exc_info()[0]
    raise

sphinxCursor = sphinxConn.cursor()

# Read unindexed records from source table at PostgreSQL
psqlStatement = """
    SELECT *
    FROM <source_table>
    WHERE status = 'todo'
    ORDER BY id DESC"""

postgresCursor.execute(psqlStatement)

row = postgresCursor.fetchone()
while row is not None:
    # Check what needs to be
    sql = "SELECT * FROM <source_table> WHERE id = %s"

    postgresCoCursor.execute(sql, (row[0], ))
    data = postgresCoCursor.fetchone()

    if data is None:
        sphinxDeleteQuery = "DELETE FROM <target_table> WHERE id = %s"

        try:
            result = sphinxCursor.execute(sphinxDeleteQuery, (row[0], ))
            # print str(row[0]) + " deleted from Sphinx successfully!"
        except:
            print 'Could not delete data from Sphinx RT index (id): ' + str(row[0])
            print "Unexpected error:", sys.exc_info()[0]
    else:
        cols = ""
        vals = ""
        parameters = ()

        i = 0
        for value in data:
            if value:
                if isinstance(value, list):
                    value = str(value).strip('[]')

                cols += postgresCoCursor.description[i][0] + ","
                vals += "%s,"
                parameters += (value,)
            i += 1

        sphinxReplaceQuery = """
            REPLACE INTO <target_table> (
                """ + cols.strip(",") + """
            )
            VALUES (
                """ + vals.strip(",") + """
            )
        """

        try:
            result = sphinxCursor.execute(sphinxReplaceQuery, parameters)
            # print str(row[0]) + " indexed successfully!"
        except:
            print 'Could not index data to Sphinx RT index (id): ' + str(row[0])
            print "Unexpected error:", sys.exc_info()[0]

    # Mark the source target as indexed
    updateQuery = """
        UPDATE <source_table>
        SET
            status = 'done',
            updated_at = now()
        WHERE id = (%s)"""

    try:
        result = postgresCoCursor.execute(updateQuery, (row[0], ))
        # print str(row[0]) + " updated successfully!"
    except:
        print 'Could not update PostgreSQL (id): ' + str(row[0])
        print "Unexpected error:", sys.exc_info()[0]

    row = postgresCursor.fetchone()

sphinxCursor.execute("select count(*) from <target_table>")
data = sphinxCursor.fetchone()
print "\nSphinx row count: " + str(data[0])

# Close database connections
sphinxConn.close()
postgresConn.close()

# Get current time
end_at = time.time()

# Calculate elapsed time
elapsed_time = end_at - started_at

# Print execution time in seconds
print 'Elapsed time: %.2f seconds' % elapsed_time
