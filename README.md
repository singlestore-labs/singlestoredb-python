# <img src="https://github.com/singlestore-labs/singlestoredb-python/blob/main/resources/singlestore-logo.png" height="60" valign="middle"/> SingleStoreDB Python Interface

This project contains a [DB-API 2.0](https://www.python.org/dev/peps/pep-0249/)
compatible Python interface to the SingleStore database and workspace management API.

## Install

This package can be install from PyPI using `pip`:
```
pip install singlestoredb
```

## Usage

Connections to the SingleStore database are made using the DB-API parameters
`host`, `port`, `user`, `password`, etc, but they may also be done using
URLs that specify these parameters as well (much like the
[SQLAlchemy](https://www.sqlalchemy.org) package).
```
import singlestoredb as s2

# Connect using the default connector
conn = s2.connect('user:password@host:3306/db_name')

# Create a cursor
cur = conn.cursor()

# Execute SQL
cur.execute('select * from foo')

# Fetch the results
print(cur.description)
for item in cur:
    print(item)

# Close the connection
conn.close()
```

Connecting to the HTTP API is done as follows:
```
# Use the HTTP API connector
conn = s2.connect('https://user:password@host:8080/db_name')
```

## Performance

While this package is based on [PyMySQL](https://github.com/PyMySQL/PyMySQL)
which is a pure Python-based MySQL connector, it adds various performance
enhancements that make it faster than most other connectors. The performance
improvents come from changes to the data conversion functions, cursor implementations,
and a C extension that is highly optimized to improve row data reading.

The package can be used both in a pure Python mode and as well as a C accelerated
mode. Generally speaking, the C accelerated version of the client can read
data 10X faster than PyMySQL, 2X faster than MySQLdb, and 1.5X faster than
mysql.connector. All of this is done without having to install any 3rd party
MySQL libraries!

Benchmarking was done with a table of 3,533,286 rows each containing a datetime,
a float, and eight character columns. The data is the same data set used in
[this article](https://www.singlestore.com/blog/how-to-get-started-with-singlestore/).
The client and server were running on the same machine and queries were made
using `fetchone`, `fetchall`, `fetchmany(1000)`, and an iterator over the cursor
object (e.g., `iter(cur)`). The results are shown below.

### Buffered

|                         | PyMySQL | MySQLdb | mysql.connector | SingleStore (Python) | SingleStore |
|-------------------------|---------|---------|-----------------|----------------------|-------------|
| fetchall                |   37.5s |    8.7s |            5.6s |                30.6s |        3.8s |
| fetchmany(1000)         |   37.9s |    9.3s |            6.1s |                33.1s |        4.0s |
| fetchone                |   37.8s |    9.2s |            6.1s |                31.0s |        4.1s |
| iter(cur)               |   37.6s |    9.2s |            6.1s |                31.1s |        4.2s |

### Unbuffered

|                         | PyMySQL | MySQLdb | mysql.connector | SingleStore (Python) | SingleStore |
|-------------------------|---------|---------|-----------------|----------------------|-------------|
| fetchall                |   38.3s |    6.6s |            5.6s |                32.1s |        5.6s |
| fetchmany(1000)         |   38.9s |    7.2s |            6.1s |                33.2s |        6.0s |
| fetchone                |   38.8s |    7.1s |            6.0s |                32.9s |        6.1s |
| iter(cur)               |   38.7s |    7.0s |            6.0s |                32.4s |        6.2s |


## License

This library is licensed under the [Apache 2.0 License](https://raw.githubusercontent.com/singlestore-labs/singlestoredb-python/main/LICENSE?token=GHSAT0AAAAAABMGV6QPNR6N23BVICDYK5LAYTVK5EA).

## Resources

* [Documentation](https://singlestore-labs.github.io/singlestoredb-python)
* [SingleStore](https://singlestore.com)
* [Python](https://python.org)

## User agreement

SINGLESTORE, INC. ("SINGLESTORE") AGREES TO GRANT YOU AND YOUR COMPANY ACCESS TO THIS OPEN SOURCE SOFTWARE CONNECTOR ONLY IF (A) YOU AND YOUR COMPANY REPRESENT AND WARRANT THAT YOU, ON BEHALF OF YOUR COMPANY, HAVE THE AUTHORITY TO LEGALLY BIND YOUR COMPANY AND (B) YOU, ON BEHALF OF YOUR COMPANY ACCEPT AND AGREE TO BE BOUND BY ALL OF THE OPEN SOURCE TERMS AND CONDITIONS APPLICABLE TO THIS OPEN SOURCE CONNECTOR AS SET FORTH BELOW (THIS “AGREEMENT”), WHICH SHALL BE DEFINITIVELY EVIDENCED BY ANY ONE OF THE FOLLOWING MEANS: YOU, ON BEHALF OF YOUR COMPANY, CLICKING THE “DOWNLOAD, “ACCEPTANCE” OR “CONTINUE” BUTTON, AS APPLICABLE OR COMPANY’S INSTALLATION, ACCESS OR USE OF THE OPEN SOURCE CONNECTOR AND SHALL BE EFFECTIVE ON THE EARLIER OF THE DATE ON WHICH THE DOWNLOAD, ACCESS, COPY OR INSTALL OF THE CONNECTOR OR USE ANY SERVICES (INCLUDING ANY UPDATES OR UPGRADES) PROVIDED BY SINGLESTORE.
BETA SOFTWARE CONNECTOR

Customer Understands and agrees that it is  being granted access to pre-release or “beta” versions of SingleStore’s open source software connector (“Beta Software Connector”) for the limited purposes of non-production testing and evaluation of such Beta Software Connector. Customer acknowledges that SingleStore shall have no obligation to release a generally available version of such Beta Software Connector or to provide support or warranty for such versions of the Beta Software Connector  for any production or non-evaluation use.

NOTWITHSTANDING ANYTHING TO THE CONTRARY IN ANY DOCUMENTATION,  AGREEMENT OR IN ANY ORDER DOCUMENT, SINGLESTORE WILL HAVE NO WARRANTY, INDEMNITY, SUPPORT, OR SERVICE LEVEL, OBLIGATIONS WITH
RESPECT TO THIS BETA SOFTWARE CONNECTOR (INCLUDING TOOLS AND UTILITIES).

APPLICABLE OPEN SOURCE LICENSE: Apache 2.0

IF YOU OR YOUR COMPANY DO NOT AGREE TO THESE TERMS AND CONDITIONS, DO NOT CHECK THE ACCEPTANCE BOX, AND DO NOT DOWNLOAD, ACCESS, COPY, INSTALL OR USE THE SOFTWARE OR THE SERVICES.
