#!/usr/bin/env python

''' Database exeception classes '''


class Error(Exception):
    '''
    Generic database exception

    Parameters
    ----------
    code : int
        The database error code
    message : str
        The database error message

    Returns
    -------
    Error

    '''

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return '[{}] {}'.format(self.code, self.message)

    def __repr__(self):
        return str(self)


class Warning(Exception):
    ''' Exception for important warnings like data truncations, etc. '''
    pass


class InterfaceError(Error):
    ''' Exception for errors in the database interface rather than the database '''
    pass


class DatabaseError(Error):
    ''' General exception for errors in the database '''
    pass


class InternalError(DatabaseError):
    ''' Exception for internal database errors such as out of sync transactions '''
    pass


class OperationalError(DatabaseError):
    ''' Exception for operational errors such as unexpected disconnections '''
    pass


class ProgrammingError(DatabaseError):
    ''' Exception for programming errors '''
    pass


class IntegrityError(DatabaseError):
    ''' Exception for relational integrity errors '''
    pass


class DataError(DatabaseError):
    ''' Exception for problems with processed data like division by zero '''
    pass


class NotSupportedError(DatabaseError):
    ''' Exception for using unsupported features of the database '''
    pass


class ClusterManagerError(Error):
    ''' Exception for errors in the cluster management API '''
    pass
