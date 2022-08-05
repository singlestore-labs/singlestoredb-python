# <img src="https://github.com/singlestore-labs/singlestore-python/blob/main/resources/singlestore-logo.png" height="60" valign="middle"/> SingleStoreDB Python Interface

This project contains a [DB-API 2.0](https://www.python.org/dev/peps/pep-0249/)
compatible Python interface to the SingleStore database and workspace management API.

## Install

This package can be install from PyPI using `pip`:
```
pip install singlestoredb
```

If you are using Anaconda, you can install with `conda`:
```
conda install -c singlestore singlestoredb
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

## License

This library is licensed under the [Apache 2.0 License](https://raw.githubusercontent.com/singlestore-labs/singlestoredb-python/main/LICENSE?token=GHSAT0AAAAAABMGV6QPNR6N23BVICDYK5LAYTVK5EA).

## Resources

* [Documentation](https://singlestore-labs.github.io/singlestore-python)
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
