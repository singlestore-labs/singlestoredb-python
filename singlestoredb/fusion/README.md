# Fusion SQL

Fusion SQL is a way to extend SQL commands handled by the SingleStoreDB
Python client. These commands are not sent to the database server. They
are handled by handler classes which can be supplied by anyone at any
time.

## Enabling Fusion SQL

By default, Fusion SQL commands are not processed by the client. They
must be enabled by setting an environment variable.
```
SINGLESTOREDB_ENABLE_FUSION=1
```

## Writing Fusion SQL handler classes

Writing handler classes is quite straight-forward. Each handler class
corresponds to a single SQL command. The grammar for the command
is put into the docstring of the class. A `run` method is then written
to perform the actions for that command.

Here is a basic example:
```python
import os
from typing import Dict
from typing import Any
from typing import Optional

from singlestoredb.fusion import result
from singlestoredb.fusion.handler import SQLHandler


class ListDirHandler(SQLHandler):
    """
    SHOW FILES IN directory [ extended ];

    # Location of directory to list
    directory = DIRECTORY '<path>'

    # Show extended file attributes?
    extended = EXTENDED

    """

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        # Create a result object
        res = self.create_result()

        # Add a field to the results
        res.add_field('Name', result.STRING)

        # See if options were specified
        if params.get('extended'):
            res.add_field('Type', result.STRING)
            res.add_field('Owner', result.INTEGER)
            res.add_field('Size', result.INTEGER)

            out = []
            for x in os.listdir(params['directory']):
                # Add additional fields
                path = os.path.join(params['directory'], x)
                stat = os.stat(path)
                ftype = 'directory' if os.path.isdir(path) else 'file'
                out.append((x, ftype, stat.st_uid, stat.st_size))

        else:
            out = [(x,) for x in os.listdir(params['directory'])]

        # Send data to result
        res.set_rows(out)

        return res


# Register the handler with Fusion
ListDirHandler.register()
```

### Writing grammars

Looking at the example above, you may have noticed that the grammar syntax is very
similar to the grammar syntax used in our documentation. The overall structure is
as follows. All syntax up to the first semi-colon is the complete SQL statement
structure. Words in all-caps are keywords. The statement will consist of keywords
and rules (in lowercase).

Rules are defined after the divider semi-colon. Each rule must be on a single
line by itself and consists of the rule name (single word in lower-case) followed
by `=` followed by the grammar for that rule. Comments (lines beginning with `#`)
can be placed anywhere in the block. Rules may contain any of the following:

* **Keywords** - always in all-caps
* **String literals** - specified in single-quoted strings (e.g. `'<foo>'`). Typically,
  a kebab-case identifier surrounded by `<` and `>` is used inside the quotes.
  The identifier is currently for documentation purposes. It is not parsed.
* **Numeric literals** - floats and integers can be specified by `<number>` and
  `<integer>`, respectively.
* **Optional blocks** - tokens surrounded by brackets (`[]`) are considered optional.
* **Selection blocks** - tokens surrounded by braces (`{}`) are used to specify a
  choice of tokens. Each choice is separatade by `|`. For example, to allow
  either quiet or verbose output, you could use: `{ QUIET | VERBOSE }`.
* **Repeated values** - tokens that can be specified multiple times can be followed
  by `,...`. For example, to allow multiple quoted strings to be specified for
  a rule, you would do `myrule,...`. The `myrule` rule would contain the syntax
  of the repeated pattern. All repeated values are comma-delimited and may
  contain optional parentheses; do not include the parantheses in your grammar.

The SingleStoredB Python client uses the keywords at the start of the first block
when determining if a submitted query is a Fusion SQL query. In the example
above, if a user submits a query that begins with `SHOW FILES IN`, that query
will be forwarded to the handler class.


### Writing the `run` method

Once your grammar is written, you can write the handler method (`run`) to perform
the operation corresponding to that syntax. The method takes one parameter
which is a dictionary of the values parsed by the rules in your grammar.
Each key in the dictionary corresponds to a rule that was parsed from a query.
The parsed value is the value stored at that key.

Parsed values that correspond to a single value are stored in the dictionary
as a single value. If the rule corresponds to a repeated value, the value in the
dictionary is a list. If there are no values corresponding to the syntax (i.e.
it contains only keywords), the value stored in the dictionary is `True`. If
a value corresponds to an optional rule, the key will only exist if the
rule was found in the given query. The same is true for selection blocks where
they are mandatory, but only one variant can be specified.

The return value of the `run` method is a `FusionSQLResult`. This object mimics
the `SQLResult` object in the SingleStoreDB Python client so that it can be
passed to the subsequent code in the client as if it was a result from the
database itself.

The primary methods of `FusionSQLResult` are `add_field` which is used to specify
the name and data type of a column in the result. and `set_rows` which is used
to pass a list of tuples as the data of the result. Note that the values returned
will be sent through the data converters used by the database client so that
resulting values for a given data type are consistent between Fusion and the
database client. This means that values returned here must be formatted is the
database would format them. For the most part, this means values should be
strings (including date/times) or numerics.

If the command does not contain results (it may just be an operation that does
not return data), `None` may be returned.


### Validation

It is possible to register validators for rules which get executed during
parsing. This can be used to ensure that the parsed data values are valid
and in the proper format in the parameter dictionary passed to the `run`
method.

Validators are simply specified in a validators dictionary on the class.
```python
def validate_month(mon: Any) -> int:
    """Convert value to int."""
    mon = int(mon)
    if mon < 0 or mon > 11:
        raise ValueError(f'month must be between 0 and 11: {mon}')
    return mon


class ShowMonthHandler(SQLHandler):
    """
    SHOW MONTH USING index;

    # Index of month (0-11)
    index = <integer>

    """

    validators = dict(index=validate_month)

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        ...


ShowMonthHandler.register()
```


## Example

Here is a more complete example demonstrating optional values, selection groups,
and repeated values.

```python
class CreateWorkspaceGroupHandler(SQLHandler):
    """
    CREATE WORKSPACE GROUP [ if_not_exists ] group_name
        IN REGION { region_id | region_name }
        [ with_password ]
        [ expires_at ]
        [ with_firewall_ranges ]
    ;

    # Only create workspace group if it doesn't exist already
    if_not_exists = IF NOT EXISTS

    # Name of the workspace group
    group_name = '<group-name>'

    # ID of region to create workspace group in
    region_id = ID '<region-id>'

    # Name of region to create workspace group in
    region_name = '<region-name>'

    # Admin password
    with_password = WITH PASSWORD '<password>'

    # Datetime or interval for expiration date/time of workspace group
    expires_at = EXPIRES AT '<iso-datetime-or-interval>'

    # Incoming IP ranges
    with_firewall_ranges = WITH FIREWALL RANGES '<ip-range>',...

    """

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        # Only create if one doesn't exist
        if params.get('if_not_exists'):
            try:
                get_workspace_group(params)
                return None
            except (ValueError, KeyError):
                pass

        # Get region ID
        if 'region_name' in params:
            regs = [x for x in manager.regions if x.name == params['region_name']]
            if not regs:
                raise ValueError(f'no region found with name "{params["region_name"]}"')
            if len(regs) > 1:
                raise ValueError(
                    f'multiple regions found with the name "{params["region_name"]}"',
                )
            region_id = regs[0].id
        else:
            region_id = params['region_id']

        # Create the workspace group
        manager.create_workspace_group(
            params['group_name'],
            region=region_id,
            admin_password=params.get('with_password'),
            expires_at=params.get('expires_at'),
            firewall_ranges=params.get('with_firewall_ranges', []),
        )

        return self.create_result()


CreateWorkspaceGroupHandler.register()
```
