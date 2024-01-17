
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <Python.h>

#ifndef Py_LIMITED_API
#include <datetime.h>
#endif

#ifndef PyBUF_WRITE
#define PyBUF_WRITE 0x200
#endif

#define ACCEL_OUT_TUPLES 0
#define ACCEL_OUT_STRUCTSEQUENCES 1
#define ACCEL_OUT_DICTS 2
#define ACCEL_OUT_NAMEDTUPLES 3

#define NUMPY_BOOL 1
#define NUMPY_INT8 2
#define NUMPY_INT16 3
#define NUMPY_INT32 4
#define NUMPY_INT64 5
#define NUMPY_UINT8 6
#define NUMPY_UINT16 7
#define NUMPY_UINT32 8
#define NUMPY_UINT64 9
#define NUMPY_FLOAT32 10
#define NUMPY_FLOAT64 11
#define NUMPY_TIMEDELTA 12
#define NUMPY_DATETIME 13
#define NUMPY_OBJECT 14

#define MYSQL_FLAG_NOT_NULL 1
#define MYSQL_FLAG_PRI_KEY 2
#define MYSQL_FLAG_UNIQUE_KEY 4
#define MYSQL_FLAG_MULTIPLE_KEY 8
#define MYSQL_FLAG_BLOB 16
#define MYSQL_FLAG_UNSIGNED 32
#define MYSQL_FLAG_ZEROFILL 64
#define MYSQL_FLAG_BINARY 128
#define MYSQL_FLAG_ENUM 256
#define MYSQL_FLAG_AUTO_INCREMENT 512
#define MYSQL_FLAG_TIMESTAMP 1024
#define MYSQL_FLAG_SET 2048
#define MYSQL_FLAG_PART_KEY 16384
#define MYSQL_FLAG_GROUP 32767
#define MYSQL_FLAG_UNIQUE 65536

#define MYSQL_TYPE_DECIMAL 0
#define MYSQL_TYPE_TINY 1
#define MYSQL_TYPE_SHORT 2
#define MYSQL_TYPE_LONG 3
#define MYSQL_TYPE_FLOAT 4
#define MYSQL_TYPE_DOUBLE 5
#define MYSQL_TYPE_NULL 6
#define MYSQL_TYPE_TIMESTAMP 7
#define MYSQL_TYPE_LONGLONG 8
#define MYSQL_TYPE_INT24 9
#define MYSQL_TYPE_DATE 10
#define MYSQL_TYPE_TIME 11
#define MYSQL_TYPE_DATETIME 12
#define MYSQL_TYPE_YEAR 13
#define MYSQL_TYPE_NEWDATE 14
#define MYSQL_TYPE_VARCHAR 15
#define MYSQL_TYPE_BIT 16
#define MYSQL_TYPE_JSON 245
#define MYSQL_TYPE_NEWDECIMAL 246
#define MYSQL_TYPE_ENUM 247
#define MYSQL_TYPE_SET 248
#define MYSQL_TYPE_TINY_BLOB 249
#define MYSQL_TYPE_MEDIUM_BLOB 250
#define MYSQL_TYPE_LONG_BLOB 251
#define MYSQL_TYPE_BLOB 252
#define MYSQL_TYPE_VAR_STRING 253
#define MYSQL_TYPE_STRING 254
#define MYSQL_TYPE_GEOMETRY 255

#define MYSQL_TYPE_CHAR MYSQL_TYPE_TINY
#define MYSQL_TYPE_INTERVAL MYSQL_TYPE_ENUM

#define MYSQL_COLUMN_NULL 251
#define MYSQL_COLUMN_UNSIGNED_CHAR 251
#define MYSQL_COLUMN_UNSIGNED_SHORT 252
#define MYSQL_COLUMN_UNSIGNED_INT24 253
#define MYSQL_COLUMN_UNSIGNED_INT64 254

#define MYSQL_SERVER_MORE_RESULTS_EXISTS 8

// 2**24 - 1
#define MYSQL_MAX_PACKET_LEN 16777215

#define ACCEL_OPTION_TIME_TYPE_TIMEDELTA 0
#define ACCEL_OPTION_TIME_TYPE_TIME 1
#define ACCEL_OPTION_JSON_TYPE_STRING 0
#define ACCEL_OPTION_JSON_TYPE_OBJ 1
#define ACCEL_OPTION_BIT_TYPE_BYTES 0
#define ACCEL_OPTION_BIT_TYPE_INT 1

#define CHR2INT1(x) ((x)[1] - '0')
#define CHR2INT2(x) ((((x)[0] - '0') * 10) + ((x)[1] - '0'))
#define CHR2INT3(x) ((((x)[0] - '0') * 1e2) + (((x)[1] - '0') * 10) + ((x)[2] - '0'))
#define CHR2INT4(x) ((((x)[0] - '0') * 1e3) + (((x)[1] - '0') * 1e2) + (((x)[2] - '0') * 10) + ((x)[3] - '0'))
#define CHR2INT6(x) ((((x)[0] - '0') * 1e5) + (((x)[1] - '0') * 1e4) + (((x)[2] - '0') * 1e3) + (((x)[3] - '0') * 1e2) + (((x)[4] - '0') * 10) + (((x)[5] - '0')))

#define CHECK_DATE_STR(s, s_l) \
    ((s_l) == 10 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] == '-' && \
     (((s)[5] == '1' && ((s)[6] >= '0' && (s)[6] <= '2')) || \
      ((s)[5] == '0' && ((s)[6] >= '1' && (s)[6] <= '9'))) && \
     (s)[7] == '-' && \
     ((((s)[8] >= '0' && (s)[8] <= '2') && ((s)[9] >= '0' && (s)[9] <= '9')) || \
       ((s)[8] == '3' && ((s)[9] >= '0' && (s)[9] <= '1'))) && \
       !((s)[0] == '0' && (s)[1] == '0' && (s)[2] == '0' && (s)[3] == '0') && \
       !((s)[5] == '0' && (s)[6] == '0') && \
       !((s)[8] == '0' && (s)[9] == '0'))

#define CHECK_TIME_STR(s, s_l) \
    ((s_l) == 8 && \
     ((((s)[0] >= '0' && (s)[0] <= '1') && ((s)[1] >= '0' && (s)[1] <= '9')) || \
       ((s)[0] == '2' && ((s)[1] >= '0' && (s)[1] <= '3'))) && \
     (s)[2] == ':' && \
     (((s)[3] >= '0' && (s)[3] <= '5') && ((s)[4] >= '0' && (s)[4] <= '9')) && \
     (s)[5] == ':' && \
     (((s)[6] >= '0' && (s)[6] <= '5') && ((s)[7] >= '0' && (s)[7] <= '9')))

#define CHECK_MICROSECONDS_STR(s, s_l) \
    ((s_l) == 7 && \
     (s)[0] == '.' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] >= '0' && (s)[4] <= '9' && \
     (s)[5] >= '0' && (s)[5] <= '9' && \
     (s)[6] >= '0' && (s)[6] <= '9')

#define CHECK_MILLISECONDS_STR(s, s_l) \
    ((s_l) == 4 && \
     (s)[0] == '.' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9')

#define CHECK_MICRO_TIME_STR(s, s_l) \
    ((s_l) == 15 && CHECK_TIME_STR(s, 8) && CHECK_MICROSECONDS_STR((s)+8, 7))

#define CHECK_MILLI_TIME_STR(s, s_l) \
    ((s_l) == 12 && CHECK_TIME_STR(s, 8) && CHECK_MILLISECONDS_STR((s)+8, 4))

#define CHECK_DATETIME_STR(s, s_l) \
    ((s_l) == 19 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_TIME_STR((s)+11, 8))

#define CHECK_MICRO_DATETIME_STR(s, s_l) \
    ((s_l) == 26 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_MICRO_TIME_STR((s)+11, 15))

#define CHECK_MILLI_DATETIME_STR(s, s_l) \
    ((s_l) == 23 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_MICRO_TIME_STR((s)+11, 12))

#define CHECK_ANY_DATETIME_STR(s, s_l) \
    (((s_l) == 19 && CHECK_DATETIME_STR(s, s_l)) || \
     ((s_l) == 23 && CHECK_MILLI_DATETIME_STR(s, s_l)) || \
     ((s_l) == 26 && CHECK_MICRO_DATETIME_STR(s, s_l)))

#define DATETIME_SIZE (19)
#define DATETIME_MILLI_SIZE (23)
#define DATETIME_MICRO_SIZE (26)

#define IS_DATETIME_MILLI(s, s_l) ((s_l) == 23)
#define IS_DATETIME_MICRO(s, s_l) ((s_l) == 26)

#define CHECK_ANY_TIME_STR(s, s_l) \
    (((s_l) == 8 && CHECK_TIME_STR(s, s_l)) || \
     ((s_l) == 12 && CHECK_MILLI_TIME_STR(s, s_l)) || \
     ((s_l) == 15 && CHECK_MICRO_TIME_STR(s, s_l)))

#define TIME_SIZE (8)
#define TIME_MILLI_SIZE (12)
#define TIME_MICRO_SIZE (15)

#define IS_TIME_MILLI(s, s_l) ((s_l) == 12)
#define IS_TIME_MICRO(s, s_l) ((s_l) == 15)


// 0000-00-00 00:00:00
// 0000-00-00 00:00:00.000
// 0000-00-00 00:00:00.000000
#define CHECK_ANY_ZERO_DATETIME_STR(s, s_l) \
    (((s_l) == 19 && CHECK_ZERO_DATETIME_STR(s, s_l)) || \
     ((s_l) == 23 && CHECK_ZERO_MILLI_DATETIME_STR(s, s_l)) || \
     ((s_l) == 26 && CHECK_ZERO_MICRO_DATETIME_STR(s, s_l)))

#define CHECK_ZERO_DATETIME_STR(s, s_l) \
    (s_l == 19 && \
     CHECK_ZERO_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_ZERO_TIME_STR((s)+11, 8))

#define CHECK_ZERO_MILLI_DATETIME_STR(s, s_l) \
    (s_l == 23 && \
     CHECK_ZERO_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_ZERO_MILLI_TIME_STR((s)+11, 12))

#define CHECK_ZERO_MICRO_DATETIME_STR(s, s_l) \
    (s_l == 26 && \
     CHECK_ZERO_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_ZERO_MICRO_TIME_STR((s)+11, 15))

#define CHECK_ZERO_DATE_STR(s, s_l) \
    (s_l == 10 && ((s)[0] == '0' && (s)[1] == '0' && (s)[2] == '0' && (s)[3] == '0' && \
     (s)[4] == '-' && (s)[5] == '0' && (s)[6] == '0' && (s)[7] == '-' && \
     (s)[8] == '0' && (s)[9] == '0'))

#define CHECK_ZERO_TIME_STR(s, s_l) \
    (s_l == 8 && ((s)[0] == '0' && (s)[1] == '0' && (s)[2] == ':' && \
     (s)[3] == '0' && (s)[4] == '0' && (s)[5] == ':' && \
     (s)[6] == '0' && (s)[7] == '0'))

#define CHECK_ZERO_MILLI_TIME_STR(s, s_l) \
    (s_l == 12 && CHECK_ZERO_TIME_STR(s, 8) && \
     (s)[8] == '.' && (s)[9] == '0' && (s)[10] == '0' && (s)[11] == '0')

#define CHECK_ZERO_MICRO_TIME_STR(s, s_l) \
    (s_l == 15 && CHECK_ZERO_TIME_STR(s, 8) && \
     (s)[8] == '.' && (s)[9] == '0' && (s)[10] == '0' && (s)[11] == '0' && \
                      (s)[12] == '0' && (s)[13] == '0' && (s)[14] == '0')


#define CHECK_TIMEDELTA1_STR(s, s_l) \
    ((s_l) == 7 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] == ':' && \
     (s)[2] >= '0' && (s)[2] <= '5' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] == ':' && \
     (s)[5] >= '0' && (s)[5] <= '5' && \
     (s)[6] >= '0' && (s)[6] <= '9')

#define CHECK_TIMEDELTA1_MILLI_STR(s, s_l) \
    ((s_l) == 11 && CHECK_TIMEDELTA1_STR(s, 7) && CHECK_MILLISECONDS_STR((s)+7, 4))

#define CHECK_TIMEDELTA1_MICRO_STR(s, s_l) \
    ((s_l) == 14 && CHECK_TIMEDELTA1_STR(s, 7) && CHECK_MICROSECONDS_STR((s)+7, 7))

#define CHECK_TIMEDELTA2_STR(s, s_l) \
    ((s_l) == 8 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     CHECK_TIMEDELTA1_STR((s)+1, 7))

#define CHECK_TIMEDELTA2_MILLI_STR(s, s_l) \
    ((s_l) == 12 && CHECK_TIMEDELTA2_STR(s, 8) && CHECK_MILLISECONDS_STR((s)+8, 4))

#define CHECK_TIMEDELTA2_MICRO_STR(s, s_l) \
    ((s_l) == 15 && CHECK_TIMEDELTA2_STR(s, 8) && CHECK_MICROSECONDS_STR((s)+8, 7))

#define CHECK_TIMEDELTA3_STR(s, s_l) \
    ((s_l) == 9 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     CHECK_TIMEDELTA1_STR((s)+2, 7))

#define CHECK_TIMEDELTA3_MILLI_STR(s, s_l) \
    ((s_l) == 13 && CHECK_TIMEDELTA3_STR(s, 9) && CHECK_MILLISECONDS_STR((s)+9, 4))

#define CHECK_TIMEDELTA3_MICRO_STR(s, s_l) \
    ((s_l) == 16 && CHECK_TIMEDELTA3_STR(s, 9) && CHECK_MICROSECONDS_STR((s)+9, 7))

//
// 0:00:00 / 0:00:00.000 / 0:00:00.000000
// 00:00:00 / 00:00:00.000 / 00:00:00.000000
// 000:00:00 / 000:00:00.000 / 000:00:00.000000
//
#define CHECK_ANY_TIMEDELTA_STR(s, s_l) \
    (((s_l) > 0 && (s)[0] == '-') ? \
     (-1 * (_CHECK_ANY_TIMEDELTA_STR((s)+1, (s_l)-1))) : \
     (_CHECK_ANY_TIMEDELTA_STR((s), (s_l))))

#define _CHECK_ANY_TIMEDELTA_STR(s, s_l) \
    (CHECK_TIMEDELTA1_STR(s, s_l) || \
     CHECK_TIMEDELTA2_STR(s, s_l) || \
     CHECK_TIMEDELTA3_STR(s, s_l) || \
     CHECK_TIMEDELTA1_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA2_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA3_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA1_MICRO_STR(s, s_l) || \
     CHECK_TIMEDELTA2_MICRO_STR(s, s_l) || \
     CHECK_TIMEDELTA3_MICRO_STR(s, s_l))

#define TIMEDELTA1_SIZE (7)
#define TIMEDELTA2_SIZE (8)
#define TIMEDELTA3_SIZE (9)
#define TIMEDELTA1_MILLI_SIZE (11)
#define TIMEDELTA2_MILLI_SIZE (12)
#define TIMEDELTA3_MILLI_SIZE (13)
#define TIMEDELTA1_MICRO_SIZE (14)
#define TIMEDELTA2_MICRO_SIZE (15)
#define TIMEDELTA3_MICRO_SIZE (16)

#define IS_TIMEDELTA1(s, s_l) ((s_l) == 7 || (s_l) == 11 || (s_l) == 14)
#define IS_TIMEDELTA2(s, s_l) ((s_l) == 8 || (s_l) == 12 || (s_l) == 15)
#define IS_TIMEDELTA3(s, s_l) ((s_l) == 9 || (s_l) == 13 || (s_l) == 16)

#define IS_TIMEDELTA_MILLI(s, s_l) ((s_l) == 11 || (s_l) == 12 || (s_l) == 13)
#define IS_TIMEDELTA_MICRO(s, s_l) ((s_l) == 14 || (s_l) == 15 || (s_l) == 16)

#define CHECKRC(x) if ((x) < 0) goto error;

typedef struct {
    int results_type;
    int parse_json;
    PyObject *invalid_values;
} MySQLAccelOptions;

inline int IMAX(int a, int b) { return((a) > (b) ? a : b); }
inline int IMIN(int a, int b) { return((a) < (b) ? a : b); }

char *_PyUnicode_AsUTF8(PyObject *unicode) {
    PyObject *bytes = PyUnicode_AsEncodedString(unicode, "utf-8", "strict");
    if (!bytes) return NULL;

    char *str = NULL;
    Py_ssize_t str_l = 0;
    if (PyBytes_AsStringAndSize(bytes, &str, &str_l) < 0) {
        return NULL;
    }

    char *out = calloc(str_l + 1, 1);
    memcpy(out, str, str_l);
    return out;
}

//
// Cached int values for date/time components
//
static PyObject *PyInts[62] = {0};

//
// Cached string values
//
typedef struct {
    PyObject *unbuffered_active;
    PyObject *active_idx;
    PyObject *_state;
    PyObject *affected_rows;
    PyObject *warning_count;
    PyObject *connection;
    PyObject *has_next;
    PyObject *options;
    PyObject *Decimal;
    PyObject *date;
    PyObject *timedelta;
    PyObject *time;
    PyObject *datetime;
    PyObject *loads;
    PyObject *field_count;
    PyObject *converters;
    PyObject *fields;
    PyObject *flags;
    PyObject *scale;
    PyObject *type_code;
    PyObject *name;
    PyObject *table_name;
    PyObject *_sock;
    PyObject *settimeout;
    PyObject *_rfile;
    PyObject *read;
    PyObject *x_errno;
    PyObject *_result;
    PyObject *_read_timeout;
    PyObject *_next_seq_id;
    PyObject *rows;
    PyObject *namedtuple;
    PyObject *Row;
    PyObject *Series;
    PyObject *array;
    PyObject *vectorize;
} PyStrings;

static PyStrings PyStr = {0};

//
// Cached Python functions
//
typedef struct {
    PyObject *json_loads;
    PyObject *decimal_Decimal;
    PyObject *datetime_date;
    PyObject *datetime_time;
    PyObject *datetime_timedelta;
    PyObject *datetime_datetime;
    PyObject *collections_namedtuple;
    PyObject *numpy_array;
    PyObject *numpy_vectorize;
} PyFunctions;

static PyFunctions PyFunc = {0};

//
// Cached Python objects
//
typedef struct {
    PyObject *namedtuple_kwargs;
    PyObject *create_numpy_array_args;
    PyObject *create_numpy_array_kwargs;
} PyObjects;

static PyObjects PyObj = {0};

//
// State
//

static PyTypeObject *StateType = NULL;

typedef struct {
    PyObject_HEAD
    PyObject *py_conn; // Database connection
    PyObject *py_fields; // List of table fields
    PyObject *py_rows; // Output object
    PyObject *py_rfile; // Socket file I/O
    PyObject *py_read; // File I/O read method
    PyObject *py_sock; // Socket
    PyObject *py_read_timeout; // Socket read timeout value
    PyObject *py_settimeout; // Socket settimeout method
    PyObject **py_converters; // List of converter functions
    PyObject **py_names; // Column names
    PyObject *py_names_list; // Python list of column names
    PyObject *py_default_converters; // Dict of default converters
    PyObject *py_namedtuple; // Generated namedtuple type
    PyObject *py_namedtuple_args; // Pre-allocated tuple for namedtuple args
    PyTypeObject *structsequence; // StructSequence type (like C namedtuple)
    PyStructSequence_Desc structsequence_desc;
    PyObject **py_encodings; // Encoding for each column as Python string
    PyObject **py_invalid_values; // Values to use when invalid data exists in a cell
    const char **encodings; // Encoding for each column
    unsigned long long n_cols; // Total number of columns
    unsigned long long n_rows; // Total number of rows read
    unsigned long long n_rows_in_batch; // Number of rows in current batch (fetchmany size)
    unsigned long *type_codes; // Type code for each column
    unsigned long *flags; // Column flags
    unsigned long *scales; // Column scales
    unsigned long *offsets; // Column offsets in buffer
    unsigned long long next_seq_id; // MySQL packet sequence number
    MySQLAccelOptions options; // Packet reader options
    int unbuffered; // Are we running in unbuffered mode?
    int is_eof; // Have we hit the eof packet yet?
    struct {
        PyObject *_next_seq_id;
        PyObject *rows;
    } py_str;
    char *encoding_errors;
} StateObject;

static void read_options(MySQLAccelOptions *options, PyObject *dict);

#define DESTROY(x) do { if (x) { free((void*)x); (x) = NULL; } } while (0)

static void State_clear_fields(StateObject *self) {
    if (!self) return;
    DESTROY(self->offsets);
    DESTROY(self->scales);
    DESTROY(self->flags);
    DESTROY(self->type_codes);
    DESTROY(self->encodings);
    DESTROY(self->structsequence_desc.fields);
    DESTROY(self->encoding_errors);
    if (self->py_converters) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_converters[i]);
        }
        DESTROY(self->py_converters);
    }
    if (self->py_names) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_names[i]);
        }
        DESTROY(self->py_names);
    }
    if (self->py_encodings) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_encodings[i]);
        }
        DESTROY(self->py_encodings);
    }
    if (self->py_invalid_values) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_invalid_values[i]);
        }
        DESTROY(self->py_invalid_values);
    }
    Py_CLEAR(self->structsequence);
    Py_CLEAR(self->py_namedtuple);
    Py_CLEAR(self->py_namedtuple_args);
    Py_CLEAR(self->py_names_list);
    Py_CLEAR(self->py_default_converters);
    Py_CLEAR(self->py_settimeout);
    Py_CLEAR(self->py_read_timeout);
    Py_CLEAR(self->py_sock);
    Py_CLEAR(self->py_read);
    Py_CLEAR(self->py_rfile);
    Py_CLEAR(self->py_rows);
    Py_CLEAR(self->py_fields);
    Py_CLEAR(self->py_conn);
}

static void State_dealloc(StateObject *self) {
    State_clear_fields(self);
    PyObject_Del(self);
}

static int State_init(StateObject *self, PyObject *args, PyObject *kwds) {
    int rc = 0;
    PyObject *py_res = NULL;
    PyObject *py_converters = NULL;
    PyObject *py_options = NULL;
    PyObject *py_args = NULL;
    unsigned long long requested_n_rows = 0;

    if (!PyArg_ParseTuple(args, "OK", &py_res, &requested_n_rows)) {
        return -1;
    }

    py_options = PyObject_GetAttr(py_res, PyStr.options);
    if (!py_options) {
        Py_INCREF(Py_None);
        py_options = Py_None;
    }

    if (PyDict_Check(py_options)) {
        self->py_default_converters = PyDict_GetItemString(py_options, "default_converters");
        if (self->py_default_converters && !PyDict_Check(self->py_default_converters)) {
            self->py_default_converters = NULL;
        }
        Py_XINCREF(self->py_default_converters);
        PyObject *py_unbuffered = PyDict_GetItemString(py_options, "unbuffered");
        if (py_unbuffered && PyObject_IsTrue(py_unbuffered)) {
            self->unbuffered = 1;
        }
        PyObject *py_encoding_errors = PyDict_GetItemString(py_options, "encoding_errors");
        if (py_encoding_errors) {
            self->encoding_errors = _PyUnicode_AsUTF8(py_encoding_errors);
            if (!self->encoding_errors) goto error;
        }
    }

    if (!self->encoding_errors) {
        self->encoding_errors = calloc(7, 1);
        if (!self->encoding_errors) goto error;
        memcpy(self->encoding_errors, "strict", 6);
    }

    if (self->unbuffered) {
        PyObject *unbuffered_active = PyObject_GetAttr(py_res, PyStr.unbuffered_active);
        if (!unbuffered_active || !PyObject_IsTrue(unbuffered_active)) {
            Py_XDECREF(unbuffered_active);
            goto error;
        }
        Py_XDECREF(unbuffered_active);
    }

    // Retrieve type codes for each column.
    PyObject *py_field_count = PyObject_GetAttr(py_res, PyStr.field_count);
    if (!py_field_count) goto error;
    self->n_cols = PyLong_AsUnsignedLong(py_field_count);
    Py_XDECREF(py_field_count);

    py_converters = PyObject_GetAttr(py_res, PyStr.converters);
    if (!py_converters) goto error;

    self->py_converters = calloc(self->n_cols, sizeof(PyObject*));
    if (!self->py_converters) goto error;

    self->type_codes = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->type_codes) goto error;

    self->flags = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->flags) goto error;

    self->scales = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->scales) goto error;

    self->encodings = calloc(self->n_cols, sizeof(char*));
    if (!self->encodings) goto error;

    self->py_encodings = calloc(self->n_cols, sizeof(char*));
    if (!self->py_encodings) goto error;

    self->py_invalid_values = calloc(self->n_cols, sizeof(char*));
    if (!self->py_invalid_values) goto error;

    self->py_names = calloc(self->n_cols, sizeof(PyObject*));
    if (!self->py_names) goto error;

    self->py_fields = PyObject_GetAttr(py_res, PyStr.fields);
    if (!self->py_fields) goto error;

    self->py_names_list = PyList_New(self->n_cols);
    if (!self->py_names_list) goto error;

    for (unsigned long i = 0; i < self->n_cols; i++) {
        // Get type codes.
        PyObject *py_field = PyList_GetItem(self->py_fields, i);
        if (!py_field) goto error;

        PyObject *py_flags = PyObject_GetAttr(py_field, PyStr.flags);
        if (!py_flags) goto error;
        self->flags[i] = PyLong_AsUnsignedLong(py_flags);
        Py_XDECREF(py_flags);

        PyObject *py_scale = PyObject_GetAttr(py_field, PyStr.scale);
        if (!py_scale) goto error;
        self->scales[i] = PyLong_AsUnsignedLong(py_scale);
        Py_XDECREF(py_scale);

        PyObject *py_field_type = PyObject_GetAttr(py_field, PyStr.type_code);
        if (!py_field_type) goto error;
        self->type_codes[i] = PyLong_AsUnsignedLong(py_field_type);
        PyObject *py_default_converter = (self->py_default_converters) ?
                      PyDict_GetItem(self->py_default_converters, py_field_type) : NULL;
        PyObject *py_invalid_value = (self->options.invalid_values) ?
                      PyDict_GetItem(self->options.invalid_values, py_field_type) : NULL;
        Py_XDECREF(py_field_type);

        // Get field name.
        PyObject *py_field_name = PyObject_GetAttr(py_field, PyStr.name);
        if (!py_field_name) goto error;

        // Make sure field name is not a duplicate.
        int dup_found = 0;
        for (unsigned long j = 0; j < i; j++) {
            if (PyUnicode_Compare(self->py_names[j], py_field_name) == 0) {
                dup_found = 1;
                break;
            }
        }
        if (dup_found) {
            PyObject *py_table_name = PyObject_GetAttr(py_field, PyStr.table_name);
            self->py_names[i] = PyUnicode_FromFormat("%U.%U", py_table_name, py_field_name);
            Py_XDECREF(py_table_name);
            if (!self->py_names[i]) goto error;
        } else {
            self->py_names[i] = py_field_name;
        }

        Py_INCREF(self->py_names[i]);  // Extra ref since SetItem steals one
        rc = PyList_SetItem(self->py_names_list, i, self->py_names[i]);
        if (rc) goto error;

        // Get field encodings (NULL means binary) and default converters.
        PyObject *py_tmp = PyList_GetItem(py_converters, i);
        if (!py_tmp) goto error;
        PyObject *py_encoding = PyTuple_GetItem(py_tmp, 0);
        if (!py_encoding) goto error;
        PyObject *py_converter = PyTuple_GetItem(py_tmp, 1);
        if (!py_converter) goto error;

        self->py_encodings[i] = (py_encoding == Py_None) ? NULL : py_encoding;
        Py_XINCREF(self->py_encodings[i]);

        self->encodings[i] = (!py_encoding || py_encoding == Py_None) ?
                              NULL : _PyUnicode_AsUTF8(py_encoding);

        self->py_invalid_values[i] = (!py_invalid_value || py_invalid_value == Py_None) ?
                                      NULL : py_converter;
        Py_XINCREF(self->py_invalid_values[i]);

        self->py_converters[i] = (!py_converter
                                  || py_converter == Py_None
                                  || py_converter == py_default_converter) ?
                                 NULL : py_converter;
        Py_XINCREF(self->py_converters[i]);
    }

    // Loop over all data packets.
    self->py_conn = PyObject_GetAttr(py_res, PyStr.connection);
    if (!self->py_conn) goto error;

    // Cache socket timeout and read methods.
    self->py_sock = PyObject_GetAttr(self->py_conn, PyStr._sock);
    if (!self->py_sock) goto error;
    self->py_settimeout = PyObject_GetAttr(self->py_sock, PyStr.settimeout);
    if (!self->py_settimeout) goto error;
    self->py_read_timeout = PyObject_GetAttr(self->py_conn, PyStr._read_timeout);
    if (!self->py_read_timeout) goto error;

    self->py_rfile = PyObject_GetAttr(self->py_conn, PyStr._rfile);
    if (!self->py_rfile) goto error;
    self->py_read = PyObject_GetAttr(self->py_rfile, PyStr.read);
    if (!self->py_read) goto error;

    PyObject *py_next_seq_id = PyObject_GetAttr(self->py_conn, PyStr._next_seq_id);
    if (!py_next_seq_id) goto error;
    self->next_seq_id = PyLong_AsUnsignedLongLong(py_next_seq_id);
    Py_XDECREF(py_next_seq_id);

    if (py_options && PyDict_Check(py_options)) {
        read_options(&self->options, py_options);
    }

    switch (self->options.results_type) {
    case ACCEL_OUT_NAMEDTUPLES:
    case ACCEL_OUT_STRUCTSEQUENCES:
        if (self->options.results_type == ACCEL_OUT_NAMEDTUPLES)
        {
            py_args = PyTuple_New(2);
            if (!py_args) goto error;

            rc = PyTuple_SetItem(py_args, 0, PyStr.Row);
            if (rc) goto error;
            Py_INCREF(PyStr.Row);

            rc = PyTuple_SetItem(py_args, 1, self->py_names_list);
            if (rc) goto error;
            Py_INCREF(self->py_names_list);

            self->py_namedtuple = PyObject_Call(
                                      PyFunc.collections_namedtuple,
                                      py_args, PyObj.namedtuple_kwargs);
            if (!self->py_namedtuple) goto error;

            self->py_namedtuple_args = PyTuple_New(self->n_cols);
            if (!self->py_namedtuple_args) goto error;
        }
        else
        {
            self->structsequence_desc.name = "singlestoredb.Row";
            self->structsequence_desc.doc = "Row of data values";
            self->structsequence_desc.n_in_sequence = (int)self->n_cols;
            self->structsequence_desc.fields = calloc(self->n_cols + 1, sizeof(PyStructSequence_Field));
            if (!self->structsequence_desc.fields) goto error;
            for (unsigned long i = 0; i < self->n_cols; i++) {
                self->structsequence_desc.fields[i].name = _PyUnicode_AsUTF8(self->py_names[i]);
                self->structsequence_desc.fields[i].doc = NULL;
            }
            self->structsequence = PyStructSequence_NewType(&self->structsequence_desc);
            if (!self->structsequence) goto error;
        }

        // Fall through

    default:
        // For fetchone, reuse the same list every time.
        //if (requested_n_rows == 1) {
        //    self->py_rows = PyList_New(1);
        //    PyList_SetItem(self->py_rows, 0, Py_None);
        //} else {
            self->py_rows = PyList_New(0);
        //}
        if (!self->py_rows) goto error;

        PyObject_SetAttr(py_res, PyStr.rows, self->py_rows);
    }

exit:
    Py_XDECREF(py_args);
    Py_XDECREF(py_converters);
    Py_XDECREF(py_options);
    if (PyErr_Occurred()) {
        PyErr_Print();
    }
    return rc;

error:
    State_clear_fields(self);
    rc = -1;
    goto exit;
}

static int State_reset_batch(
    StateObject *self,
    PyObject *py_res,
    unsigned long long requested_n_rows
) {
    int rc = 0;
    PyObject *py_tmp = NULL;

    self->n_rows_in_batch = 0;

    //if (requested_n_rows != 1) {
        py_tmp = self->py_rows;
        self->py_rows = PyList_New(0);
        Py_XDECREF(py_tmp);
        if (!self->py_rows) { rc = -1; goto error; }
        rc = PyObject_SetAttr(py_res, PyStr.rows, self->py_rows);
    //}

exit:
    return rc;

error:
    goto exit;
}

static PyType_Slot StateType_slots[] = {
    {Py_tp_init, (initproc)State_init},
    {Py_tp_dealloc, (destructor)State_dealloc},
    {Py_tp_doc, "PyMySQL accelerator"},
    {0, NULL},
};

static PyType_Spec StateType_spec = {
    .name = "_singlestoredb_accel.State",
    .basicsize = sizeof(StateObject),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots = StateType_slots,
};

//
// End State
//

static void read_options(MySQLAccelOptions *options, PyObject *dict) {
    if (!options || !dict) return;

    PyObject *key = NULL;
    PyObject *value = NULL;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &key, &value)) {
        if (PyUnicode_CompareWithASCIIString(key, "results_type") == 0) {
            if (PyUnicode_CompareWithASCIIString(value, "dict") == 0 ||
                PyUnicode_CompareWithASCIIString(value, "dicts") == 0 ) {
                options->results_type = ACCEL_OUT_DICTS;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "namedtuple") == 0 ||
                     PyUnicode_CompareWithASCIIString(value, "namedtuples") == 0) {
                options->results_type = ACCEL_OUT_NAMEDTUPLES;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "structsequence") == 0 ||
                     PyUnicode_CompareWithASCIIString(value, "structsequences") == 0) {
                options->results_type = ACCEL_OUT_STRUCTSEQUENCES;
            }
            else {
                options->results_type = ACCEL_OUT_TUPLES;
            }
        } else if (PyUnicode_CompareWithASCIIString(key, "parse_json") == 0) {
            options->parse_json = PyObject_IsTrue(value);
        } else if (PyUnicode_CompareWithASCIIString(key, "invalid_values") == 0) {
            if (PyDict_Check(value)) {
                options->invalid_values = value;
            }
        }
    }
}

static void raise_exception(
    PyObject *self,
    char *err_type,
    unsigned long long err_code,
    char *err_str
) {
    PyObject *py_exc = NULL;
    PyObject *py_val = NULL;

    py_exc = PyObject_GetAttrString(self, err_type);
    if (!py_exc) goto error;

    py_val = Py_BuildValue("(Ks)", err_code, err_str);
    if (!py_val) goto error;

    PyErr_SetObject(py_exc, py_val);

exit:
    if (py_exc) { Py_DECREF(py_exc); }
    if (py_val) { Py_DECREF(py_val); }
    return;

error:
    goto exit;
}

static int is_error_packet(char *buff_bytes) {
    return buff_bytes && *(uint8_t*)buff_bytes == 0xFF;
}

static void force_close(PyObject *py_conn) {
    PyObject *py_sock = NULL;

    py_sock = PyObject_GetAttr(py_conn, PyStr._sock);
    if (!py_sock) goto error;

    Py_XDECREF(PyObject_CallMethod(py_sock, "close", NULL));
    PyErr_Clear();

    PyObject_SetAttr(py_conn, PyStr._sock, Py_None);
    PyObject_SetAttr(py_conn, PyStr._rfile, Py_None);

exit:
    Py_XDECREF(py_sock);
    return;

error:
    goto exit;
}

static PyObject *read_bytes(StateObject *py_state, unsigned long long num_bytes) {
    PyObject *py_num_bytes = NULL;
    PyObject *py_data = NULL;
    PyObject *py_exc = NULL;

    if (py_state->py_read_timeout && py_state->py_read_timeout != Py_None) {
        Py_XDECREF(PyObject_CallFunctionObjArgs(py_state->py_settimeout,
                                                py_state->py_read_timeout, NULL));
        if (PyErr_Occurred()) goto error;
    }

    py_num_bytes = PyLong_FromUnsignedLongLong(num_bytes);
    if (!py_num_bytes) goto error;

    while (1) {
        py_data = PyObject_CallFunctionObjArgs(py_state->py_read, py_num_bytes, NULL);

        if ((py_exc = PyErr_Occurred())) {
            if (PyErr_ExceptionMatches(PyExc_IOError) || PyErr_ExceptionMatches(PyExc_OSError)) {
                PyObject *py_errno = PyObject_GetAttr(py_exc, PyStr.x_errno);
                if (!py_errno) goto error;

                unsigned long long err = PyLong_AsUnsignedLongLong(py_errno);
                Py_DECREF(py_errno);

                if (err == 4 /* errno.EINTER */) {
                    continue;
                }

                force_close(py_state->py_conn);
                raise_exception(py_state->py_conn, "OperationalError", 0,
                                "Lost connection to SingleStoreDB server during query");
                goto error;
            }
            else if (PyErr_ExceptionMatches(PyExc_BaseException)) {
                // Don't convert unknown exception to MySQLError.
                force_close(py_state->py_conn);
                goto error;
            }
        }

        if (py_data) {
            break;
        }
    }

    if (PyBytes_Size(py_data) < (long int)num_bytes) {
        force_close(py_state->py_conn);
        raise_exception(py_state->py_conn, "OperationalError", 0,
                        "Lost connection to SingleStoreDB server during query");
        goto error;
    }

exit:
    Py_XDECREF(py_num_bytes);
    return py_data;

error:
    Py_CLEAR(py_data);
    goto exit;
}

static PyObject *read_packet(StateObject *py_state) {
    PyObject *py_buff = NULL;
    PyObject *py_new_buff = NULL;
    PyObject *py_packet_header = NULL;
    PyObject *py_bytes_to_read = NULL;
    PyObject *py_recv_data = NULL;
    unsigned long long bytes_to_read = 0;
    char *buff = NULL;
    uint64_t btrl = 0;
    uint8_t btrh = 0;
    uint8_t packet_number = 0;

    py_buff = PyByteArray_FromStringAndSize(NULL, 0);
    if (!py_buff) goto error;

    while (1) {
        py_packet_header = read_bytes(py_state, 4);
        if (!py_packet_header) goto error;

        buff = PyBytes_AsString(py_packet_header);

        btrl = *(uint16_t*)buff;
        btrh = *(uint8_t*)(buff+2);
        packet_number = *(uint8_t*)(buff+3);
        bytes_to_read = btrl + (btrh << 16);

        Py_CLEAR(py_packet_header);

        if (packet_number != py_state->next_seq_id) {
            force_close(py_state->py_conn);
            if (packet_number == 0) {
                raise_exception(py_state->py_conn, "OperationalError", 0,
                                "Lost connection to SingleStoreDB server during query");

                goto error;
            }
            raise_exception(py_state->py_conn, "InternalError", 0,
                            "Packet sequence number wrong");
            goto error;
        }

        py_state->next_seq_id = (py_state->next_seq_id + 1) % 256;

        py_recv_data = read_bytes(py_state, bytes_to_read);
        if (!py_recv_data) goto error;

        py_new_buff = PyByteArray_Concat(py_buff, py_recv_data);
        Py_CLEAR(py_recv_data);
        Py_CLEAR(py_buff);
        if (!py_new_buff) goto error;

        py_buff = py_new_buff;
        py_new_buff = NULL;

        if (bytes_to_read == 0xFFFFFF) {
            continue;
        }

        if (bytes_to_read < MYSQL_MAX_PACKET_LEN) {
            break;
        }
    }

    if (is_error_packet(PyByteArray_AsString(py_buff))) {
        PyObject *py_result = PyObject_GetAttr(py_state->py_conn, PyStr._result);
        if (py_result && py_result != Py_None) {
            PyObject *py_unbuffered_active = PyObject_GetAttr(py_result, PyStr.unbuffered_active);
            if (py_unbuffered_active == Py_True) {
                PyObject_SetAttr(py_result, PyStr.unbuffered_active, Py_False);
            }
            Py_XDECREF(py_unbuffered_active);
        }
        Py_XDECREF(py_result);
        Py_XDECREF(PyObject_CallMethod(py_state->py_conn, "_raise_mysql_exception",
                                       "O", py_buff, NULL));
        goto error;
    }

exit:
    Py_XDECREF(py_new_buff);
    Py_XDECREF(py_bytes_to_read);
    Py_XDECREF(py_recv_data);
    Py_XDECREF(py_packet_header);
    return py_buff;

error:
    Py_CLEAR(py_buff);
    goto exit;
}

static int is_eof_packet(char *data, unsigned long long data_l) {
    return data && (uint8_t)*(uint8_t*)data == 0xFE && data_l < 9;
}

static int check_packet_is_eof(
    char **data,
    unsigned long long *data_l,
    unsigned long long *warning_count,
    int *has_next
) {
    uint16_t server_status = 0;
    if (!data || !data_l) {
        if (has_next) *has_next = 0;
        if (warning_count) *warning_count = 0;
        return 0;
    }
    if (!is_eof_packet(*data, *data_l)) {
        return 0;
    }
    *data += 1; *data_l -= 1;
    if (warning_count) *warning_count = **(uint16_t**)data;
    *data += 2; *data_l -= 2;
    server_status = **(uint16_t**)data;
    *data += 2; *data_l -= 2;
    if (has_next) *has_next = server_status & MYSQL_SERVER_MORE_RESULTS_EXISTS;
    return 1;
}

static unsigned long long read_length_encoded_integer(
    char **data,
    unsigned long long *data_l,
    int *is_null
) {
    if (is_null) *is_null = 0;

    if (!data || !data_l || *data_l == 0) {
        if (is_null) *is_null = 1;
        return 0;
    }

    uint8_t c = **(uint8_t**)data;
    *data += 1; *data_l -= 1;

    if (c == MYSQL_COLUMN_NULL) {
        if (is_null) *is_null = 1;
        return 0;
    }

    if (c < MYSQL_COLUMN_UNSIGNED_CHAR) {
        return c;
    }

    if (c == MYSQL_COLUMN_UNSIGNED_SHORT) {
        if (*data_l < 2) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint16_t out = **(uint16_t**)data;
        *data += 2; *data_l -= 2;
        return out;
    }

    if (c == MYSQL_COLUMN_UNSIGNED_INT24) {
        if (*data_l < 3) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint16_t low = **(uint8_t**)data;
        *data += 1; *data_l -= 1;
        uint16_t high = **(uint16_t**)data;
        *data += 2; *data_l -= 2;
        return low + (high << 16);
    }

    if (c == MYSQL_COLUMN_UNSIGNED_INT64) {
        if (*data_l < 8) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint64_t out = **(uint64_t**)data;
        *data += 8; *data_l -= 8;
        return out;
    }

    if (is_null) *is_null = 1;
    return 0;
}

static void read_length_coded_string(
    char **data,
    unsigned long long *data_l,
    char **out,
    unsigned long long *out_l,
    int *is_null
) {
    if (is_null) *is_null = 0;

    if (!data || !data_l || !out || !out_l) {
        if (is_null) *is_null = 1;
        return;
    }

    unsigned long long length = read_length_encoded_integer(data, data_l, is_null);

    if (is_null && *is_null) {
        return;
    }

    length = (length > *data_l) ? *data_l : length;

    *out = *data;
    *out_l = length;

    *data += length;
    *data_l -= length;

    return;
}

#ifdef Py_LIMITED_API

static PyObject *PyDate_FromDate(
    StateObject *py_state,
    int year,
    int month,
    int day
) {
    PyObject *out = NULL;
    PyObject *py_year = NULL;
    int free_year = 0;

    if (year >= 0 && year <= 60) {
       py_year = PyInts[year];
    } else {
       free_year = 1;
       py_year = PyLong_FromLong(year);
    }

    out = PyObject_CallFunctionObjArgs(
        PyFunc.datetime_date, py_year, PyInts[month], PyInts[day], NULL
    );

    if (free_year) Py_XDECREF(py_year);

    return out;
}

static PyObject *PyDelta_FromDSU(
    StateObject *py_state,
    int days,
    int seconds,
    int microseconds
) {
    PyObject *out = NULL;
    PyObject *py_days = NULL;
    PyObject *py_seconds = NULL;
    PyObject *py_microseconds = NULL;
    int free_days = 0;
    int free_seconds = 0;
    int free_microseconds = 0;

    if (days >= 0 && days <= 60) {
        py_days = PyInts[days];
    } else {
        free_days = 1;
        py_days = PyLong_FromLong(days);
    }

    if (seconds >= 0 && seconds <= 60) {
        py_seconds = PyInts[seconds];
    } else {
        free_seconds = 1;
        py_seconds = PyLong_FromLong(seconds);
    }

    if (microseconds >= 0 && microseconds <= 60) {
        py_microseconds = PyInts[microseconds];
    } else {
        free_microseconds = 1;
        py_microseconds = PyLong_FromLong(microseconds);
    }

    out = PyObject_CallFunctionObjArgs(
        PyFunc.datetime_timedelta, py_days, py_seconds, py_microseconds, NULL
    );

    if (free_days) Py_XDECREF(py_days);
    if (free_seconds) Py_XDECREF(py_seconds);
    if (free_microseconds) Py_XDECREF(py_microseconds);

    return out;
}

static PyObject *PyDateTime_FromDateAndTime(
    StateObject *py_state,
    int year,
    int month,
    int day,
    int hour,
    int minute,
    int second,
    int microsecond
) {
    PyObject *out = NULL;
    PyObject *py_year = NULL;
    PyObject *py_microsecond = NULL;
    int free_year = 0;
    int free_microsecond = 0;

    if (year >= 0 && year <= 60) {
       py_year = PyInts[year];
    } else {
       free_year = 1;
       py_year = PyLong_FromLong(year);
    }

    if (microsecond >= 0 && microsecond <= 60) {
       py_microsecond = PyInts[microsecond];
    } else {
       free_microsecond = 1;
       py_microsecond = PyLong_FromLong(microsecond);
    }

    out = PyObject_CallFunctionObjArgs(
        PyFunc.datetime_datetime, py_year, PyInts[month], PyInts[day],
        PyInts[hour], PyInts[minute], PyInts[second], py_microsecond, NULL
    );

    if (free_microsecond) Py_XDECREF(py_microsecond);
    if (free_year) Py_XDECREF(py_year);

    return out;
}

#endif

static PyObject *read_row_from_packet(
    StateObject *py_state,
    char *data,
    unsigned long long data_l
) {
    char *out = NULL;
    char *orig_out = NULL;
    unsigned long long out_l = 0;
    unsigned long long orig_out_l = 0;
    int is_null = 0;
    PyObject *py_result = NULL;
    PyObject *py_item = NULL;
    PyObject *py_str = NULL;
    char end = '\0';

    int sign = 1;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;

    switch (py_state->options.results_type) {
    case ACCEL_OUT_DICTS:
        py_result = PyDict_New();
        break;
   case ACCEL_OUT_STRUCTSEQUENCES: {
        if (!py_state->structsequence) goto error;
        py_result = PyStructSequence_New(py_state->structsequence);
        break;
        }
    case ACCEL_OUT_NAMEDTUPLES:
        if (!py_state->py_namedtuple) goto error;
        if (!py_state->py_namedtuple_args) goto error;
        py_result = py_state->py_namedtuple_args;
        break;
    default:
        py_result = PyTuple_New(py_state->n_cols);
    }

    for (unsigned long i = 0; i < py_state->n_cols; i++) {

        read_length_coded_string(&data, &data_l, &out, &out_l, &is_null);
        end = out[out_l];

        orig_out = out;
        orig_out_l = out_l;

        py_item = Py_None;

        // Don't convert if it's a NULL.
        if (!is_null) {

            // If a converter was passed in, use it.
            if (py_state->py_converters[i]) {
                py_str = NULL;
                if (py_state->encodings[i] == NULL) {
                    py_str = PyBytes_FromStringAndSize(out, out_l);
                    if (!py_str) goto error;
                } else {
                    py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], py_state->encoding_errors);
                    if (!py_str) goto error;
                }
                py_item = PyObject_CallFunctionObjArgs(py_state->py_converters[i], py_str, NULL);
                Py_CLEAR(py_str);
                if (!py_item) goto error;
            }

            // If no converter was passed in, do the default processing.
            else {
                switch (py_state->type_codes[i]) {
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_DECIMAL:
                    py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], py_state->encoding_errors);
                    if (!py_str) goto error;

                    py_item = PyObject_CallFunctionObjArgs(PyFunc.decimal_Decimal, py_str, NULL);
                    Py_CLEAR(py_str);
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_INT24:
                    if (data_l) out[out_l] = '\0';
                    if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                        py_item = PyLong_FromUnsignedLongLong(strtoull(out, NULL, 10));
                    } else {
                        py_item = PyLong_FromLongLong(strtoll(out, NULL, 10));
                    }
                    if (data_l) out[out_l] = end;
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                    if (data_l) out[out_l] = '\0';
                    py_item = PyFloat_FromDouble(strtod(out, NULL));
                    if (data_l) out[out_l] = end;
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_NULL:
                    py_item = Py_None;
                    break;

                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_TIMESTAMP:
                    if (CHECK_ANY_ZERO_DATETIME_STR(out, out_l)) {
                        py_item = Py_None;
                        Py_INCREF(Py_None);
                        break;
                    }
                    else if (!CHECK_ANY_DATETIME_STR(out, out_l)) {
                        if (py_state->py_invalid_values[i]) {
                            py_item = py_state->py_invalid_values[i];
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "ascii", py_state->encoding_errors);
                            if (!py_item) goto error;
                        }
                        break;
                    }
                    year = CHR2INT4(out); out += 5;
                    month = CHR2INT2(out); out += 3;
                    day = CHR2INT2(out); out += 3;
                    hour = CHR2INT2(out); out += 3;
                    minute = CHR2INT2(out); out += 3;
                    second = CHR2INT2(out); out += 3;
                    microsecond = (IS_DATETIME_MICRO(out, out_l)) ? CHR2INT6(out) :
                                  (IS_DATETIME_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    py_item = PyDateTime_FromDateAndTime(
#ifdef Py_LIMITED_API
                                    py_state,
#endif
                                    year, month, day, hour, minute, second, microsecond);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "ascii", py_state->encoding_errors);
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_NEWDATE:
                case MYSQL_TYPE_DATE:
                    if (CHECK_ZERO_DATE_STR(out, out_l)) {
                        py_item = Py_None;
                        Py_INCREF(Py_None);
                        break;
                    }
                    else if (!CHECK_DATE_STR(out, out_l)) {
                        if (py_state->py_invalid_values[i]) {
                            py_item = py_state->py_invalid_values[i];
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "ascii", py_state->encoding_errors);
                            if (!py_item) goto error;
                        }
                        break;
                    }
                    year = CHR2INT4(out); out += 5;
                    month = CHR2INT2(out); out += 3;
                    day = CHR2INT2(out); out += 3;
                    py_item = PyDate_FromDate(
#ifdef Py_LIMITED_API
                                    py_state,
#endif
                                    year, month, day);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "ascii", py_state->encoding_errors);
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_TIME:
                    sign = CHECK_ANY_TIMEDELTA_STR(out, out_l);
                    if (!sign) {
                        if (py_state->py_invalid_values[i]) {
                            py_item = py_state->py_invalid_values[i];
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "ascii", py_state->encoding_errors);
                            if (!py_item) goto error;
                        }
                        break;
                    } else if (sign < 0) {
                        out += 1; out_l -= 1;
                    }
                    if (IS_TIMEDELTA1(out, out_l)) {
                        hour = CHR2INT1(out); out += 2;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    else if (IS_TIMEDELTA2(out, out_l)) {
                        hour = CHR2INT2(out); out += 3;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    else if (IS_TIMEDELTA3(out, out_l)) {
                        hour = CHR2INT3(out); out += 4;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    py_item = PyDelta_FromDSU(
#ifdef Py_LIMITED_API
                                    py_state,
#endif
                                    0, sign * hour * 60 * 60 +
                                       sign * minute * 60 +
                                       sign * second,
                                       sign * microsecond);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "ascii", py_state->encoding_errors);
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_YEAR:
                    if (out_l == 0) {
                        goto error;
                        break;
                    }
                    if (data_l) out[out_l] = '\0';
                    year = strtoul(out, NULL, 10);
                    py_item = PyLong_FromLong(year);
                    if (data_l) out[out_l] = end;
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_JSON:
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_GEOMETRY:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING:
                case MYSQL_TYPE_STRING:
                    if (!py_state->encodings[i]) {
                        py_item = PyBytes_FromStringAndSize(out, out_l);
                        if (!py_item) goto error;
                        break;
                    }

                    py_item = PyUnicode_Decode(out, out_l, py_state->encodings[i], py_state->encoding_errors);
                    if (!py_item) goto error;

                    // Parse JSON string.
                    if (py_state->type_codes[i] == MYSQL_TYPE_JSON && py_state->options.parse_json) {
                        py_str = py_item;
                        py_item = PyObject_CallFunctionObjArgs(PyFunc.json_loads, py_str, NULL);
                        Py_CLEAR(py_str);
                        if (!py_item) goto error;
                    }

                    break;

                default:
                    PyErr_Format(PyExc_TypeError, "unknown type code: %ld",
                                 py_state->type_codes[i], NULL);
                    goto error;
                }
            }
        }

        if (py_item == Py_None) {
            Py_INCREF(Py_None);
        }

        switch (py_state->options.results_type) {
        case ACCEL_OUT_STRUCTSEQUENCES:
            PyStructSequence_SetItem(py_result, i, py_item);
            break;
        case ACCEL_OUT_DICTS:
            PyDict_SetItem(py_result, py_state->py_names[i], py_item);
            Py_INCREF(py_state->py_names[i]);
            Py_DECREF(py_item);
            break;
        default:
            PyTuple_SetItem(py_result, i, py_item);
        }
    }

    if (py_state->options.results_type == ACCEL_OUT_NAMEDTUPLES) {
        // We just use py_result above as storage for the parameters to
        // the namedtuple constructor. It gets deleted at the end of the
        // fetch operation.
        py_result = PyObject_CallObject(py_state->py_namedtuple, py_result);
        if (!py_result) goto error;
    }

exit:
    return py_result;

error:
    Py_CLEAR(py_result);
    goto exit;
}

static PyObject *read_rowdata_packet(PyObject *self, PyObject *args, PyObject *kwargs) {
    int rc = 0;
    StateObject *py_state = NULL;
    PyObject *py_res = NULL;
    PyObject *py_unbuffered = NULL;
    PyObject *py_out = NULL;
    PyObject *py_next_seq_id = NULL;
    PyObject *py_zero = PyLong_FromUnsignedLong(0);
    PyObject *py_err_type = NULL;
    PyObject *py_err_value = NULL;
    PyObject *py_err_tb = NULL;
    unsigned long long requested_n_rows = 0;
    unsigned long long row_idx = 0;
    char *keywords[] = {"result", "unbuffered", "size", NULL};

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|K", keywords, &py_res, &py_unbuffered, &requested_n_rows)) {
        goto error;
    }

    if (py_unbuffered && PyObject_IsTrue(py_unbuffered)) {
        PyObject *unbuffered_active = PyObject_GetAttr(py_res, PyStr.unbuffered_active);
        if (!unbuffered_active || !PyObject_IsTrue(unbuffered_active)) {
            Py_XDECREF(unbuffered_active);
            Py_XDECREF(py_zero);
            Py_INCREF(Py_None);
            return Py_None;
         }
        Py_XDECREF(unbuffered_active);
    }

    // Get the rowdata state.
    py_state = (StateObject*)PyObject_GetAttr(py_res, PyStr._state);
    if (!py_state) {
        PyErr_Clear();

        PyObject *py_requested_n_rows = PyLong_FromUnsignedLongLong(requested_n_rows);
        if (!py_requested_n_rows) goto error;

        PyObject *py_args = PyTuple_New(2);
        if (!py_args) goto error;
        PyTuple_SetItem(py_args, 0, py_res);
        PyTuple_SetItem(py_args, 1, py_requested_n_rows);
        Py_INCREF(py_res);
        Py_INCREF(py_requested_n_rows);

        py_state = (StateObject*)PyObject_CallObject((PyObject*)StateType, py_args);
        if (!py_state) { Py_DECREF(py_args); goto error; }
        Py_DECREF(py_args);

        PyObject_SetAttr(py_res, PyStr._state, (PyObject*)py_state);
    }
    else if (requested_n_rows > 0) {
        State_reset_batch(py_state, py_res, requested_n_rows);
    }

    if (requested_n_rows == 0) {
        requested_n_rows = UINTMAX_MAX;
    }

    if (py_state->is_eof) {
        goto exit;
    }

    while (row_idx < requested_n_rows) {
        PyObject *py_buff = read_packet(py_state);
        if (!py_buff) goto error;

        PyObject *py_row = NULL;
        char *data = PyByteArray_AsString(py_buff);
        unsigned long long data_l = PyByteArray_Size(py_buff);
        unsigned long long warning_count = 0;
        int has_next = 0;

        if (check_packet_is_eof(&data, &data_l, &warning_count, &has_next)) {
            Py_CLEAR(py_buff);

            py_state->is_eof = 1;

            PyObject *py_long = NULL;

            py_long = PyLong_FromUnsignedLongLong(warning_count);
            PyObject_SetAttr(py_res, PyStr.warning_count, py_long ? py_long : 0);
            Py_CLEAR(py_long);

            py_long = PyLong_FromLong(has_next);
            PyObject_SetAttr(py_res, PyStr.has_next, py_long ? py_long : 0);
            Py_CLEAR(py_long);

            PyObject_SetAttr(py_res, PyStr.connection, Py_None);
            PyObject_SetAttr(py_res, PyStr.unbuffered_active, Py_False);

            break;
        }

        py_state->n_rows++;
        py_state->n_rows_in_batch++;

        py_row = read_row_from_packet(py_state, data, data_l);
        if (!py_row) { Py_CLEAR(py_buff); goto error; }

        //if (requested_n_rows == 1) {
        //    rc = PyList_SetItem(py_state->py_rows, 0, py_row);
        //} else {
            rc = PyList_Append(py_state->py_rows, py_row);
            Py_DECREF(py_row);
        //}
        if (rc != 0) { Py_CLEAR(py_buff); goto error; }

        row_idx++;

        Py_CLEAR(py_buff);
    }

exit:
    if (!py_state) return NULL;

    py_next_seq_id = PyLong_FromUnsignedLongLong(py_state->next_seq_id);
    if (!py_next_seq_id) goto error;
    PyObject_SetAttr(py_state->py_conn, PyStr._next_seq_id, py_next_seq_id);
    Py_DECREF(py_next_seq_id);

    py_out = NULL;

    if (py_state->unbuffered) {
        if (py_state->is_eof && row_idx == 0) {
            Py_INCREF(Py_None);
            py_out = Py_None;
            PyObject_SetAttr(py_res, PyStr.rows, Py_None);
            PyObject *py_n_rows = PyLong_FromSsize_t(py_state->n_rows);
            PyObject_SetAttr(py_res, PyStr.affected_rows, (py_n_rows) ? py_n_rows : Py_None);
            Py_XDECREF(py_n_rows);
            PyObject_DelAttr(py_res, PyStr._state);
            Py_CLEAR(py_state);
        }
        else {
            py_out = (requested_n_rows == 1) ?
                     PyList_GetItem(py_state->py_rows, 0) : py_state->py_rows;
            Py_XINCREF(py_out);
        }
    }
    else {
        py_out = py_state->py_rows;
        Py_INCREF(py_out);
        PyObject *py_n_rows = PyLong_FromSsize_t(py_state->n_rows);
        PyObject_SetAttr(py_res, PyStr.affected_rows, (py_n_rows) ? py_n_rows : Py_None);
        Py_XDECREF(py_n_rows);
        if (py_state->is_eof) {
            PyObject_DelAttr(py_res, PyStr._state);
            Py_CLEAR(py_state);
        }
    }

    Py_XDECREF(py_zero);

    if (PyErr_Occurred()) {
        Py_CLEAR(py_out);
    }
    else if (py_err_type) {
        Py_CLEAR(py_out);
        PyErr_Restore(py_err_type, py_err_value, py_err_tb);
    }

    return py_out;

error:
    if (PyErr_Occurred()) {
        PyErr_Fetch(&py_err_type, &py_err_value, &py_err_tb);
    }
    goto exit;
}


static PyObject *create_numpy_array(PyObject *py_memview, char *data_format, int data_type, PyObject *py_objs) {
    PyObject *py_memviewc = NULL;
    PyObject *py_in = NULL;
    PyObject *py_out = NULL;
    PyObject *py_vec_func = NULL;

    py_memviewc = PyObject_CallMethod(py_memview, "cast", "s", data_format);
    if (!py_memviewc) goto error;

    CHECKRC(PyTuple_SetItem(PyObj.create_numpy_array_args, 0, py_memviewc));
    Py_INCREF(py_memviewc);

    py_out = PyObject_Call(PyFunc.numpy_array, PyObj.create_numpy_array_args, PyObj.create_numpy_array_kwargs);
    if (!py_out) goto error;

    // Add series to the output, remapping string values as needed
    if (py_objs) {
        switch(data_type) {
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case -MYSQL_TYPE_VARCHAR:
        case -MYSQL_TYPE_JSON:
        case -MYSQL_TYPE_SET:
        case -MYSQL_TYPE_ENUM:
        case -MYSQL_TYPE_VAR_STRING:
        case -MYSQL_TYPE_STRING:
        case -MYSQL_TYPE_GEOMETRY:
        case -MYSQL_TYPE_TINY_BLOB:
        case -MYSQL_TYPE_MEDIUM_BLOB:
        case -MYSQL_TYPE_LONG_BLOB:
        case -MYSQL_TYPE_BLOB:
            py_vec_func = PyObject_CallFunction(PyFunc.numpy_vectorize, "Os", PyObject_GetAttrString(py_objs, "get"), "O");
            py_in = py_out;
            py_out = PyObject_CallFunction(py_vec_func, "O", py_in);
            if (!py_out) goto error;
            break;
        }
    }

exit:
    Py_XDECREF(py_in);
    Py_XDECREF(py_vec_func);
    Py_XDECREF(py_memviewc);

    return py_out;

error:
    Py_XDECREF(py_out);
    py_out = NULL;

    goto exit;
}


int ensure_numpy() {
    if (PyFunc.numpy_array && PyFunc.numpy_vectorize) goto exit;

    // Import numpy if it exists
    PyObject *numpy_mod = PyImport_ImportModule("numpy");
    if (!numpy_mod) goto error;

    PyFunc.numpy_array = PyObject_GetAttr(numpy_mod, PyStr.array);
    if (!PyFunc.numpy_array) goto error;

    PyFunc.numpy_vectorize = PyObject_GetAttr(numpy_mod, PyStr.vectorize);
    if (!PyFunc.numpy_vectorize) goto error;

exit:
    return 0;

error:
    return -1;
}


static PyObject *load_rowdat_1_numpy(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *py_data = NULL;
    PyObject *py_out = NULL;
    PyObject *py_colspec = NULL;
    PyObject *py_str = NULL;
    PyObject *py_blob = NULL;
    PyObject *py_memview = NULL;
    PyObject *py_arr = NULL;
    PyObject *py_out_pairs = NULL;
    PyObject *py_index = NULL;
    PyObject *py_objs = NULL;
    PyObject *py_mask = NULL;
    PyObject *py_pair = NULL;
    Py_ssize_t length = 0;
    uint8_t is_null = 0;
    int8_t i8 = 0;
    int16_t i16 = 0;
    int32_t i32 = 0;
    int64_t i64 = 0;
    uint8_t u8 = 0;
    uint16_t u16 = 0;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    float flt = 0;
    double dbl = 0;
    int *ctypes = NULL;
    char *data = NULL;
    char *orig_data = NULL;
    char *end = NULL;
    unsigned long long n_cols = 0;
    unsigned long long i = 0;
    unsigned long long j = 0;
    char *keywords[] = {"colspec", "data", NULL};
    uint64_t n_rows = 0;
    int *item_sizes = NULL;
    char **data_formats = NULL;
    char **out_cols = NULL;
    char **mask_cols = NULL;
    int64_t *out_row_ids = NULL;

    if (ensure_numpy() < 0) goto error;

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", keywords, &py_colspec, &py_data)) {
        goto error;
    }

    CHECKRC(PyBytes_AsStringAndSize(py_data, &data, &length));
    end = data + (unsigned long long)length;
    orig_data = data;

    // Get number of columns
    n_cols = PyObject_Length(py_colspec);
    if (n_cols == 0) {
        goto error;
    }

    // Determine column types
    ctypes = calloc(sizeof(int), n_cols);
    if (!ctypes) goto error;
    for (i = 0; i < n_cols; i++) {
        PyObject *py_cspec = PySequence_GetItem(py_colspec, i);
        if (!py_cspec) goto error;
        PyObject *py_ctype = PySequence_GetItem(py_cspec, 1);
        if (!py_ctype) { Py_DECREF(py_cspec); goto error; }
        ctypes[i] = (int)PyLong_AsLong(py_ctype);
        Py_DECREF(py_ctype);
        Py_DECREF(py_cspec);
        if (PyErr_Occurred()) { goto error; }
    }

#define CHECKSIZE(x) \
    if ((data + x) > end) { \
        PyErr_SetString(PyExc_ValueError, "data length does not align with specified column values"); \
        goto error; \
    }

    // Determine number of rows
    item_sizes = malloc(sizeof(int) * n_cols);
    if (!item_sizes) goto error;
    data_formats = malloc(sizeof(char*) * n_cols);
    if (!data_formats) goto error;
    while (end > data) {
        // Row ID
        CHECKSIZE(8);
        data += 8;

        for (i = 0; i < n_cols; i++) {
            // Null slot
            CHECKSIZE(1);
            data += 1;

            // Data row
            switch (ctypes[i]) {
            case MYSQL_TYPE_NULL:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: NULL");
                goto error;
                break;

            case MYSQL_TYPE_BIT:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: BIT");
                goto error;
                break;

            case MYSQL_TYPE_TINY:
            case -MYSQL_TYPE_TINY:
                CHECKSIZE(1);
                item_sizes[i] = 1;
                data_formats[i] = (ctypes[i] < 0) ? "B" : "b";
                data += 1;
                break;

            case MYSQL_TYPE_SHORT:
            case -MYSQL_TYPE_SHORT:
                CHECKSIZE(2);
                item_sizes[i] = 2;
                data_formats[i] = (ctypes[i] < 0) ? "H" : "h";
                data += 2;
                break;

            case MYSQL_TYPE_LONG:
            case -MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
            case -MYSQL_TYPE_INT24:
                CHECKSIZE(4);
                item_sizes[i] = 4;
                data_formats[i] = (ctypes[i] < 0) ? "I" : "i";
                data += 4;
                break;

            case MYSQL_TYPE_LONGLONG:
            case -MYSQL_TYPE_LONGLONG:
                CHECKSIZE(8);
                item_sizes[i] = 8;
                data_formats[i] = (ctypes[i] < 0) ? "Q" : "q";
                data += 8;
                break;

            case MYSQL_TYPE_FLOAT:
                CHECKSIZE(4);
                item_sizes[i] = 4;
                data_formats[i] = "f";
                data += 4;
                break;

            case MYSQL_TYPE_DOUBLE:
                CHECKSIZE(8);
                item_sizes[i] = 8;
                data_formats[i] = "d";
                data += 8;
                break;

            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: DECIMAL");
                goto error;
                break;

            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: DATE");
                goto error;
                break;

            case MYSQL_TYPE_TIME:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: TIME");
                goto error;
                break;

            case MYSQL_TYPE_DATETIME:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: DATETIME");
                goto error;
                break;

            case MYSQL_TYPE_TIMESTAMP:
                PyErr_SetString(PyExc_TypeError, "unsupported data type: TIMESTAMP");
                goto error;
                break;

            case MYSQL_TYPE_YEAR:
                CHECKSIZE(2);
                data += 2;
                break;

            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                CHECKSIZE(8);
                item_sizes[i] = 8;
                data_formats[i] = "Q";
                i64 = *(int64_t*)data;
                data += 8;
                CHECKSIZE(i64);
                data += i64;
                break;

            // Use negative to indicate binary
            case -MYSQL_TYPE_VARCHAR:
            case -MYSQL_TYPE_JSON:
            case -MYSQL_TYPE_SET:
            case -MYSQL_TYPE_ENUM:
            case -MYSQL_TYPE_VAR_STRING:
            case -MYSQL_TYPE_STRING:
            case -MYSQL_TYPE_GEOMETRY:
            case -MYSQL_TYPE_TINY_BLOB:
            case -MYSQL_TYPE_MEDIUM_BLOB:
            case -MYSQL_TYPE_LONG_BLOB:
                CHECKSIZE(8);
                item_sizes[i] = 8;
                data_formats[i] = "Q";
                i64 = *(int64_t*)data;
                data += 8;
                CHECKSIZE(i64);
                data += i64;
                break;
            }
        }

        n_rows += 1;
    }

    // Reset data pointer
    data = orig_data;

    // Allocate data columns
    out_cols = malloc(n_cols * sizeof(char*));
    if (!out_cols) goto error;
    mask_cols = malloc(n_cols * sizeof(char*));
    if (!mask_cols) goto error;
    for (i = 0; i < n_cols; i++) {
        out_cols[i] = malloc(item_sizes[i] * n_rows);
        if (!out_cols[i]) goto error;
        mask_cols[i] = malloc(1 * n_rows);
        if (!mask_cols[i]) goto error;
    }

    // Allocate row ID array
    out_row_ids = malloc(sizeof(int64_t) * n_rows);
    if (!out_row_ids) goto error;

    // Create dict for strings/blobs
    py_objs = PyDict_New();
    if (!py_objs) goto error;
    CHECKRC(PyDict_SetItem(py_objs, PyLong_FromUnsignedLongLong(0), Py_None));

    // Build output arrays
    j = 0;
    while (end > data) {
        if (j >= n_rows) goto error;

        out_row_ids[j] = *(int64_t*)data; data += 8;

        for (i = 0; i < n_cols; i++) {
            is_null = (data[0] == '\x01');
            data += 1;

            ((char*)mask_cols[i])[j] = (is_null) ? '\x01' : '\x00';

            switch (ctypes[i]) {
            case MYSQL_TYPE_NULL:
                i8 = 0; data += 1;
                memcpy(out_cols[i] + j * 1, &i8, 1);
                break;

            case MYSQL_TYPE_BIT:
                // TODO
                break;

            case MYSQL_TYPE_TINY:
                i8 = (is_null) ? 0 : *(int8_t*)data; data += 1;
                memcpy(out_cols[i] + j * 1, &i8, 1);
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_TINY:
                u8 = (is_null) ? 0 : *(uint8_t*)data; data += 1;
                memcpy(out_cols[i] + j * 1, &u8, 1);
                break;

            case MYSQL_TYPE_SHORT:
                i16 = (is_null) ? 0 : *(int16_t*)data; data += 2;
                memcpy(out_cols[i] + j * 2, &i16, 2);
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_SHORT:
                u16 = (is_null) ? 0 : *(uint16_t*)data; data += 2;
                memcpy(out_cols[i] + j * 2, &u16, 2);
                break;

            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
                i32 = (is_null) ? 0 : *(int32_t*)data; data += 4;
                memcpy(out_cols[i] + j * 4, &i32, 4);
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONG:
            case -MYSQL_TYPE_INT24:
                u32 = (is_null) ? 0 : *(uint32_t*)data; data += 4;
                memcpy(out_cols[i] + j * 4, &u32, 4);
                break;

            case MYSQL_TYPE_LONGLONG:
                i64 = (is_null) ? 0 : *(int64_t*)data; data += 8;
                memcpy(out_cols[i] + j * 8, &i64, 8);
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONGLONG:
                u64 = (is_null) ? 0 : *(uint64_t*)data; data += 8;
                memcpy(out_cols[i] + j * 8, &u64, 8);
                break;

            case MYSQL_TYPE_FLOAT:
                flt = (is_null) ? NAN : *(float*)data; data += 4;
                memcpy(out_cols[i] + j * 4, &flt, 4);
                break;

            case MYSQL_TYPE_DOUBLE:
                dbl = (is_null) ? NAN : *(double*)data; data += 8;
                memcpy(out_cols[i] + j * 8, &dbl, 8);
                break;

            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
                // TODO
                break;

            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                // TODO
                break;

            case MYSQL_TYPE_TIME:
                // TODO
                break;

            case MYSQL_TYPE_DATETIME:
                // TODO
                break;

            case MYSQL_TYPE_TIMESTAMP:
                // TODO
                break;

            case MYSQL_TYPE_YEAR:
                u16 = (is_null) ? 0 : *(uint16_t*)data; data += 2;
                memcpy(out_cols[i] + j * 2, &u16, 2);
                break;

            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                i64 = *(int64_t*)data; data += 8;
                if (is_null) {
                    u64 = 0;
                    memcpy(out_cols[i] + j * 8, &u64, 8);
                } else {
                    py_str = PyUnicode_FromStringAndSize(data, (Py_ssize_t)i64);
                    data += i64;
                    if (!py_str) goto error;
                    u64 = (uint64_t)py_str;
                    memcpy(out_cols[i] + j * 8, &u64, 8);
                    CHECKRC(PyDict_SetItem(py_objs, PyLong_FromUnsignedLongLong(u64), py_str));
                    Py_CLEAR(py_str);
                }
                break;

            // Use negative to indicate binary
            case -MYSQL_TYPE_VARCHAR:
            case -MYSQL_TYPE_JSON:
            case -MYSQL_TYPE_SET:
            case -MYSQL_TYPE_ENUM:
            case -MYSQL_TYPE_VAR_STRING:
            case -MYSQL_TYPE_STRING:
            case -MYSQL_TYPE_GEOMETRY:
            case -MYSQL_TYPE_TINY_BLOB:
            case -MYSQL_TYPE_MEDIUM_BLOB:
            case -MYSQL_TYPE_LONG_BLOB:
            case -MYSQL_TYPE_BLOB:
                i64 = *(int64_t*)data; data += 8;
                if (is_null) {
                    u64 = 0;
                    memcpy(out_cols[i] + j * 8, &u64, 8);
                } else {
                    py_blob = PyBytes_FromStringAndSize(data, (Py_ssize_t)i64);
                    data += i64;
                    if (!py_blob) goto error;
                    u64 = (uint64_t)py_blob;
                    memcpy(out_cols[i] + j * 8, &u64, 8);
                    CHECKRC(PyDict_SetItem(py_objs, PyLong_FromUnsignedLongLong(u64), py_blob));
                    Py_CLEAR(py_blob);
                }
                break;

            default:
                goto error;
            }
        }

        j += 1;
    }

    py_out = PyTuple_New(2);
    if (!py_out) goto error;

    py_out_pairs = PyList_New(n_cols);
    if (!py_out_pairs) goto error;

    // Create Series of row IDs
    py_memview = PyMemoryView_FromMemory((char*)out_row_ids, n_rows * 8, PyBUF_WRITE);
    if (!py_memview) goto error;

    py_index = create_numpy_array(py_memview, "Q", 0, NULL);
    Py_CLEAR(py_memview);
    if (!py_index) goto error;

    CHECKRC(PyTuple_SetItem(py_out, 0, py_index));
    CHECKRC(PyTuple_SetItem(py_out, 1, py_out_pairs));

    // Convert C buffers to numpy arrays and masks
    for (i = 0; i < n_cols; i++) {
        py_pair = PyTuple_New(2);
        if (!py_pair) goto error;

        py_memview = PyMemoryView_FromMemory(out_cols[i], n_rows * item_sizes[i], PyBUF_WRITE);
        if (!py_memview) goto error;

        py_arr = create_numpy_array(py_memview, data_formats[i], ctypes[i], py_objs);
        Py_CLEAR(py_memview);
        if (!py_arr) goto error;

        py_memview = PyMemoryView_FromMemory(mask_cols[i], n_rows * 1, PyBUF_WRITE);
        if (!py_memview) goto error;

        py_mask = create_numpy_array(py_memview, "?", 0, NULL);
        Py_CLEAR(py_memview);
        if (!py_mask) goto error;

        if (!py_arr) goto error;
        if (!py_mask) goto error;

        CHECKRC(PyTuple_SetItem(py_pair, 0, py_arr));
        py_arr = NULL;

        CHECKRC(PyTuple_SetItem(py_pair, 1, py_mask));
        py_mask = NULL;

        CHECKRC(PyList_SetItem(py_out_pairs, i, py_pair));
        py_pair = NULL;
    }

exit:
    if (ctypes) free(ctypes);
    if (out_cols) free(out_cols);
    if (mask_cols) free(mask_cols);
    if (data_formats) free(data_formats);
    if (item_sizes) free(item_sizes);

    Py_XDECREF(py_str);
    Py_XDECREF(py_arr);
    Py_XDECREF(py_mask);
    Py_XDECREF(py_pair);
    Py_XDECREF(py_blob);
    Py_XDECREF(py_objs);
    Py_XDECREF(py_memview);

    return py_out;

error:
    Py_XDECREF(py_out);
    py_out = NULL;

    goto exit;
}


static char *get_array_base_address(PyObject *py_array) {
    char *out = NULL;
    PyObject *py_array_interface = NULL;
    PyObject *py_data = NULL;
    PyObject *py_address = NULL;

    if (py_array == Py_None) goto exit;

    py_array_interface = PyObject_GetAttrString(py_array, "__array_interface__");
    if (!py_array_interface) goto error;

    py_data = PyDict_GetItemString(py_array_interface, "data");
    if (!py_data) goto error;

    py_address = PyTuple_GetItem(py_data, 0);
    if (!py_address) goto error;

    out = (char*)PyLong_AsUnsignedLongLong(py_address);

exit:
    Py_XDECREF(py_array_interface);

    return out;

error:
    goto exit;
}


static int get_numpy_col_type(PyObject *py_array) {
    int out = 0;
    char *str = NULL;
    PyObject *py_array_interface = NULL;
    PyObject *py_typestr = NULL;

    if (py_array == Py_None) goto error;

    py_array_interface = PyObject_GetAttrString(py_array, "__array_interface__");
    if (!py_array_interface) goto error;

    py_typestr = PyDict_GetItemString(py_array_interface, "typestr");
    if (!py_typestr) goto error;

    str = _PyUnicode_AsUTF8(py_typestr);
    if (!str) goto error;

    switch (str[1]) {
    case 'b':
        out = NUMPY_BOOL;
        break;
    case 'i':
        switch (str[2]) {
        case '1':
            out = NUMPY_INT8;
            break;
        case '2':
            out = NUMPY_INT16;
            break;
        case '4':
            out = NUMPY_INT32;
            break;
        case '8':
            out = NUMPY_INT64;
            break;
        }
        break;
    case 'u':
        switch (str[2]) {
        case '1':
            out = NUMPY_UINT8;
            break;
        case '2':
            out = NUMPY_UINT16;
            break;
        case '4':
            out = NUMPY_UINT32;
            break;
        case '8':
            out = NUMPY_UINT64;
            break;
        }
        break;
    case 'f':
        switch (str[2]) {
        case '4':
            out = NUMPY_FLOAT32;
            break;
        case '8':
            out = NUMPY_FLOAT64;
            break;
        }
        break;
    case 'O':
        out = NUMPY_OBJECT;
        break;
    case 'm':
        out = NUMPY_TIMEDELTA;
        break;
    case 'M':
        out = NUMPY_DATETIME;
        break;
    default:
        goto error;
    }

exit:
    Py_XDECREF(py_array_interface);

    if (str) free(str);

    return out;

error:
    out = 0;
    goto exit;
}


//
// Convert Python objects to rowdat_1 format
//
// The inputs must look like:
//
// [mysql-type-1, mysql-type-2, ...], row-id-array, [(array-1, mask-1), (array-2, mask-2), ...]
//
// The number of elements in the first argument must be the same as the number
// of elements in the last parameter. The number of elements in the second
// parameter must equal the number of elements in each of the array-1 and mask-1
// parameters. The mask parameters may be Py_None.
//
static PyObject *dump_rowdat_1_numpy(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *py_returns = NULL;
    PyObject *py_row_ids = NULL;
    PyObject *py_cols = NULL;
    PyObject *py_out = NULL;
    unsigned long long n_cols = 0;
    unsigned long long n_rows = 0;
    uint8_t is_null = 0;
    int8_t i8 = 0;
    int16_t i16 = 0;
    int32_t i32 = 0;
    int64_t i64 = 0;
    uint8_t u8 = 0;
    uint16_t u16 = 0;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    float flt = 0;
    double dbl = 0;
    char *out = NULL;
    unsigned long long out_l = 0;
    unsigned long long out_idx = 0;
    int *returns = NULL;
    char *keywords[] = {"returns", "row_ids", "cols", NULL};
    unsigned long long i = 0;
    unsigned long long j = 0;
    char **cols = NULL;
    char **masks = NULL;
    int *col_types = NULL;
    int64_t *row_ids = NULL;

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOO", keywords, &py_returns, &py_row_ids, &py_cols)) {
        goto error;
    }

    if (PyObject_Length(py_returns) != PyObject_Length(py_cols)) {
        PyErr_SetString(PyExc_ValueError, "number of return values does not match number of returned columns");
        goto error;
    }

    n_rows = (unsigned long long)PyObject_Length(py_row_ids);
    if (n_rows == 0) {
        py_out = PyBytes_FromStringAndSize("", 0);
        goto exit;
    }

    // Verify all data lengths agree
    n_cols = (unsigned long long)PyObject_Length(py_returns);
    if (n_cols == 0) {
        py_out = PyBytes_FromStringAndSize("", 0);
        goto exit;
    }
    for (i = 0; i < n_cols; i++) {
        PyObject *py_item = PyList_GetItem(py_cols, i);
        if (!py_item) goto error;

        PyObject *py_data = PyTuple_GetItem(py_item, 0);
        if (!py_data) goto error;

        if ((unsigned long long)PyObject_Length(py_data) != n_rows) {
            PyErr_SetString(PyExc_ValueError, "mismatched lengths of column values");
            goto error;
        }

        PyObject *py_mask = PyTuple_GetItem(py_item, 1);
        if (!py_mask) goto error;

        if (py_mask != Py_None && (unsigned long long)PyObject_Length(py_mask) != n_rows) {
            PyErr_SetString(PyExc_ValueError, "length of mask values does not match the length of data rows");
            goto error;
        }
    }

    row_ids = (int64_t*)get_array_base_address(py_row_ids);
    if (!row_ids) {
        PyErr_SetString(PyExc_ValueError, "unable to get base address of row IDs");
        goto error;
    }

    // Starting size, it will be resized later
    out_l = 256 * n_cols;
    out_idx = 0;
    out = malloc(out_l);
    if (!out) goto error;

    // Get return types
    returns = malloc(sizeof(int) * n_cols);
    if (!returns) goto error;

    for (i = 0; i < n_cols; i++) {
        PyObject *py_item = PySequence_GetItem(py_returns, i);
        if (!py_item) goto error;
        returns[i] = (int)PyLong_AsLong(py_item);
        Py_DECREF(py_item);
        if (PyErr_Occurred()) { goto error; }
    }

    // Get column array memory
    cols = calloc(sizeof(char*), n_cols);
    if (!cols) goto error;
    col_types = calloc(sizeof(int), n_cols);
    if (!col_types) goto error;
    masks = calloc(sizeof(char*), n_cols);
    if (!masks) goto error;
    for (i = 0; i < n_cols; i++) {
        PyObject *py_item = PyList_GetItem(py_cols, i);
        if (!py_item) goto error;

        PyObject *py_data = PyTuple_GetItem(py_item, 0);
        if (!py_data) goto error;

        cols[i] = get_array_base_address(py_data);
        if (!cols[i]) {
            PyErr_SetString(PyExc_ValueError, "unable to get base address of data column");
            goto error;
        }

        col_types[i] = get_numpy_col_type(py_data);
        if (!col_types[i]) {
            PyErr_SetString(PyExc_ValueError, "unable to get column type of data column");
            goto error;
        }

        PyObject *py_mask = PyTuple_GetItem(py_item, 1);
        if (!py_mask) goto error;

        masks[i] = get_array_base_address(py_mask);
        if (masks[i] && get_numpy_col_type(py_mask) != NUMPY_BOOL) {
            PyErr_SetString(PyExc_ValueError, "mask must only contain boolean values");
            goto error;
        }
    }

#define CHECKMEM(x) \
    if ((out_idx + x) > out_l) { \
        out_l = out_l * 2 + x; \
        out = realloc(out, out_l); \
        if (!out) goto error; \
    }

    for (j = 0; j < n_rows; j++) {

        CHECKMEM(8);
        memcpy(out+out_idx, &row_ids[j], 8);
        out_idx += 8;

        for (i = 0; i < n_cols; i++) {

            if (masks[i]) {
                is_null = *(masks[i] + j) != '\x00';
            } else {
                is_null = 0;
            }

            CHECKMEM(1);
            memcpy(out+out_idx, &is_null, 1);
            out_idx += 1;

#define CHECK_TINYINT(x, unsigned_input) if ((x) < ((unsigned_input) ? 0 : -128) || (x) > 127) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for TINYINT"); \
        goto error; \
    }
#define CHECK_UNSIGNED_TINYINT(x, unsigned_input) if ((x) < 0 || (x) > 255) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for UNSIGNED TINYINT"); \
        goto error; \
    }
#define CHECK_SMALLINT(x, unsigned_input) if ((x) < ((unsigned_input) ? 0 : -32768) || (x) > 32767) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for SMALLINT"); \
        goto error; \
    }
#define CHECK_UNSIGNED_SMALLINT(x, unsigned_input) if ((x) < 0 || (x) > 65535) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for UNSIGNED SMALLINT"); \
        goto error; \
    }
#define CHECK_MEDIUMINT(x, unsigned_input) if ((x) < ((unsigned_input) ? 0 : -8388608) || (x) > 8388607) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for MEDIUMINT"); \
        goto error; \
    }
#define CHECK_UNSIGNED_MEDIUMINT(x, unsigned_input) if ((x) < 0 || (x) > 16777215) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for UNSIGNED MEDIUMINT"); \
        goto error; \
    }
#define CHECK_INT(x, unsigned_input) if ((x) < ((unsigned_input) ? 0 : -2147483648) || (x) > 2147483647) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for INT"); \
        goto error; \
    }
#define CHECK_UNSIGNED_INT(x, unsigned_input) if ((x) < 0 || (x) > 4294967295) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for UNSIGNED INT"); \
        goto error; \
    }
#define CHECK_BIGINT(x, unsigned_input) if ((x) < ((unsigned_input) ? 0 : -9223372036854775808) || (x) > 9223372036854775807) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for BIGINT"); \
        goto error; \
    }
#define CHECK_UNSIGNED_BIGINT(x, unsigned_input) if ((x) < 0 || (x) > 18446744073709551615) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for UNSIGNED BIGINT"); \
        goto error; \
    }
#define CHECK_YEAR(x) if (!(((x) >= 0 && (x) <= 99) || ((x) >= 1901 && (x) <= 2155))) { \
        PyErr_SetString(PyExc_ValueError, "value is outside the valid range for YEAR"); \
        goto error; \
    }

            switch (returns[i]) {
            case MYSQL_TYPE_BIT:
                PyErr_SetString(PyExc_ValueError, "unsupported data type: BIT");
                goto error;
                break;

            case MYSQL_TYPE_TINY:
                CHECKMEM(1);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_TINYINT(i8, 0);
                    i8 = (int8_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_TINYINT(i8, 0);
                    i8 = (int8_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_TINYINT(i16, 0);
                    i8 = (int8_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_TINYINT(i32, 0);
                    i8 = (int8_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_TINYINT(i64, 0);
                    i8 = (int8_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_TINYINT(u8, 1);
                    i8 = (int8_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_TINYINT(u16, 1);
                    i8 = (int8_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_TINYINT(u32, 1);
                    i8 = (int8_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_TINYINT(u64, 1);
                    i8 = (int8_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_TINYINT(flt, 0);
                    i8 = (int8_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_TINYINT(dbl, 0);
                    i8 = (int8_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type TINYINT");
                    goto error;
                }
                memcpy(out+out_idx, &i8, 1);
                out_idx += 1;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_TINY:
                CHECKMEM(1);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_TINYINT(i8, 0);
                    u8 = (uint8_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_TINYINT(i8, 0);
                    u8 = (uint8_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_TINYINT(i16, 0);
                    u8 = (uint8_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_TINYINT(i32, 0);
                    u8 = (uint8_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_TINYINT(i64, 0);
                    u8 = (uint8_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_TINYINT(u8, 1);
                    u8 = (uint8_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_TINYINT(u16, 1);
                    u8 = (uint8_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_TINYINT(u32, 1);
                    u8 = (uint8_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_TINYINT(u64, 1);
                    u8 = (uint8_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_TINYINT(flt, 0);
                    u8 = (uint8_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_TINYINT(dbl, 0);
                    u8 = (uint8_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type UNSIGNED TINYINT");
                    goto error;
                }
                memcpy(out+out_idx, &u8, 1);
                out_idx += 1;
                break;

            case MYSQL_TYPE_SHORT:
                CHECKMEM(2);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_SMALLINT(i8, 0);
                    i16 = (int16_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_SMALLINT(i8, 0);
                    i16 = (int16_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_SMALLINT(i16, 0);
                    i16 = (int16_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_SMALLINT(i32, 0);
                    i16 = (int16_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_SMALLINT(i64, 0);
                    i16 = (int16_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_SMALLINT(u8, 1);
                    i16 = (int16_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_SMALLINT(u16, 1);
                    i16 = (int16_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_SMALLINT(u32, 1);
                    i16 = (int16_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_SMALLINT(u64, 1);
                    i16 = (int16_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_SMALLINT(flt, 0);
                    i16 = (int16_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_SMALLINT(dbl, 0);
                    i16 = (int16_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type SMALLINT");
                    goto error;
                }
                memcpy(out+out_idx, &i16, 2);
                out_idx += 2;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_SHORT:
                CHECKMEM(2);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_SMALLINT(i8, 0);
                    u16 = (uint16_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_SMALLINT(i8, 0);
                    u16 = (uint16_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_SMALLINT(i16, 0);
                    u16 = (uint16_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_SMALLINT(i32, 0);
                    u16 = (uint16_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_SMALLINT(i64, 0);
                    u16 = (uint16_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_SMALLINT(u8, 1);
                    u16 = (uint16_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_SMALLINT(u16, 1);
                    u16 = (uint16_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_SMALLINT(u32, 1);
                    u16 = (uint16_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_SMALLINT(u64, 1);
                    u16 = (uint16_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_SMALLINT(flt, 0);
                    u16 = (uint16_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_SMALLINT(dbl, 0);
                    u16 = (uint16_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type UNSIGNED MEDIUMINT");
                    goto error;
                }
                memcpy(out+out_idx, &u16, 2);
                out_idx += 2;
                break;

            case MYSQL_TYPE_INT24:
                CHECKMEM(4);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_MEDIUMINT(i8, 0);
                    i32 = (int32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_MEDIUMINT(i8, 0);
                    i32 = (int32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_MEDIUMINT(i16, 0);
                    i32 = (int32_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_MEDIUMINT(i32, 0);
                    i32 = (int32_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_MEDIUMINT(i64, 0);
                    i32 = (int32_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_MEDIUMINT(u8, 1);
                    i32 = (int32_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_MEDIUMINT(u16, 1);
                    i32 = (int32_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_MEDIUMINT(u32, 1);
                    i32 = (int32_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_MEDIUMINT(u64, 1);
                    i32 = (int32_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_MEDIUMINT(flt, 0);
                    i32 = (int32_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_MEDIUMINT(dbl, 0);
                    i32 = (int32_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type MEDIUMINT");
                    goto error;
                }
                memcpy(out+out_idx, &i32, 4);
                out_idx += 4;
                break;

            case MYSQL_TYPE_LONG:
                CHECKMEM(4);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_INT(i8, 0);
                    i32 = (int32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_INT(i8, 0);
                    i32 = (int32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_INT(i16, 0);
                    i32 = (int32_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_INT(i32, 0);
                    i32 = (int32_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_INT(i64, 0);
                    i32 = (int32_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_INT(u8, 1);
                    i32 = (int32_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_INT(u16, 1);
                    i32 = (int32_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_INT(u32, 1);
                    i32 = (int32_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_INT(u64, 1);
                    i32 = (int32_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_INT(flt, 0);
                    i32 = (int32_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_INT(dbl, 0);
                    i32 = (int32_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type INT");
                    goto error;
                }
                memcpy(out+out_idx, &i32, 4);
                out_idx += 4;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_INT24:
                CHECKMEM(4);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_MEDIUMINT(i8, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_MEDIUMINT(i8, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_MEDIUMINT(i16, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_MEDIUMINT(i32, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_MEDIUMINT(i64, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_MEDIUMINT(u8, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_MEDIUMINT(u16, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_MEDIUMINT(u32, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_MEDIUMINT(u64, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_MEDIUMINT(flt, 0);
                    u32 = (uint32_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_MEDIUMINT(dbl, 0);
                    u32 = (uint32_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type UNSIGNED MEDIUMINT");
                    goto error;
                }
                memcpy(out+out_idx, &u32, 4);
                out_idx += 4;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONG:
                CHECKMEM(4);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_INT(i8, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_INT(i8, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_INT(i16, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_INT(i32, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_INT(i64, 0);
                    u32 = (uint32_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_INT(u8, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_INT(u16, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_INT(u32, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_INT(u64, 1);
                    u32 = (uint32_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_INT(flt, 0);
                    u32 = (uint32_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_INT(dbl, 0);
                    u32 = (uint32_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type UNSIGNED INT");
                    goto error;
                }
                memcpy(out+out_idx, &u32, 4);
                out_idx += 4;
                break;

            case MYSQL_TYPE_LONGLONG:
                CHECKMEM(8);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_BIGINT(i8, 0);
                    i64 = (int64_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_BIGINT(i8, 0);
                    i64 = (int64_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_BIGINT(i16, 0);
                    i64 = (int64_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_BIGINT(i32, 0);
                    i64 = (int64_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_BIGINT(i64, 0);
                    i64 = (int64_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_BIGINT(u8, 1);
                    i64 = (int64_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_BIGINT(u16, 1);
                    i64 = (int64_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_BIGINT(u32, 1);
                    i64 = (int64_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_BIGINT(u64, 1);
                    i64 = (int64_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_BIGINT(flt, 0);
                    i64 = (int64_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_BIGINT(dbl, 0);
                    i64 = (int64_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type BIGINT");
                    goto error;
                }
                memcpy(out+out_idx, &i64, 8);
                out_idx += 8;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONGLONG:
                CHECKMEM(8);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_BIGINT(i8, 0);
                    u64 = (uint64_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_BIGINT(i8, 0);
                    u64 = (uint64_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_BIGINT(i16, 0);
                    u64 = (uint64_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_BIGINT(i32, 0);
                    u64 = (uint64_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_BIGINT(i64, 0);
                    u64 = (uint64_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_UNSIGNED_BIGINT(u8, 1);
                    u64 = (uint64_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_UNSIGNED_BIGINT(u16, 1);
                    u64 = (uint64_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_BIGINT(u32, 1);
                    u64 = (uint64_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_BIGINT(u64, 1);
                    u64 = (uint64_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_UNSIGNED_BIGINT(flt, 0);
                    u64 = (uint64_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_UNSIGNED_BIGINT(dbl, 0);
                    u64 = (uint64_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type UNSIGNED BIGINT");
                    goto error;
                }
                memcpy(out+out_idx, &u64, 8);
                out_idx += 8;
                break;

            case MYSQL_TYPE_FLOAT:
                CHECKMEM(4);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    flt = (float)((is_null) ? 0 : *(int8_t*)(cols[i] + j * 1));
                    break;
                case NUMPY_INT8:
                    flt = (float)((is_null) ? 0 : *(int8_t*)(cols[i] + j * 1));
                    break;
                case NUMPY_INT16:
                    flt = (float)((is_null) ? 0 : *(int16_t*)(cols[i] + j * 2));
                    break;
                case NUMPY_INT32:
                    flt = (float)((is_null) ? 0 : *(int32_t*)(cols[i] + j * 4));
                    break;
                case NUMPY_INT64:
                    flt = (float)((is_null) ? 0 : *(int64_t*)(cols[i] + j * 8));
                    break;
                case NUMPY_UINT8:
                    flt = (float)((is_null) ? 0 : *(uint8_t*)(cols[i] + j * 1));
                    break;
                case NUMPY_UINT16:
                    flt = (float)((is_null) ? 0 : *(uint16_t*)(cols[i] + j * 2));
                    break;
                case NUMPY_UINT32:
                    flt = (float)((is_null) ? 0 : *(uint32_t*)(cols[i] + j * 4));
                    break;
                case NUMPY_UINT64:
                    flt = (float)((is_null) ? 0 : *(uint64_t*)(cols[i] + j * 8));
                    break;
                case NUMPY_FLOAT32:
                    flt = (float)((is_null) ? 0 : *(float*)(cols[i] + j * 4));
                    break;
                case NUMPY_FLOAT64:
                    flt = (float)((is_null) ? 0 : *(double*)(cols[i] + j * 8));
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type FLOAT");
                    goto error;
                }
                memcpy(out+out_idx, &flt, 4);
                out_idx += 4;
                break;

            case MYSQL_TYPE_DOUBLE:
                CHECKMEM(8);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    dbl = (double)((is_null) ? 0 : *(int8_t*)(cols[i] + j * 1));
                    break;
                case NUMPY_INT8:
                    dbl = (double)((is_null) ? 0 : *(int8_t*)(cols[i] + j * 1));
                    break;
                case NUMPY_INT16:
                    dbl = (double)((is_null) ? 0 : *(int16_t*)(cols[i] + j * 2));
                    break;
                case NUMPY_INT32:
                    dbl = (double)((is_null) ? 0 : *(int32_t*)(cols[i] + j * 4));
                    break;
                case NUMPY_INT64:
                    dbl = (double)((is_null) ? 0 : *(int64_t*)(cols[i] + j * 8));
                    break;
                case NUMPY_UINT8:
                    dbl = (double)((is_null) ? 0 : *(uint8_t*)(cols[i] + j * 1));
                    break;
                case NUMPY_UINT16:
                    dbl = (double)((is_null) ? 0 : *(uint16_t*)(cols[i] + j * 2));
                    break;
                case NUMPY_UINT32:
                    dbl = (double)((is_null) ? 0 : *(uint32_t*)(cols[i] + j * 4));
                    break;
                case NUMPY_UINT64:
                    dbl = (double)((is_null) ? 0 : *(uint64_t*)(cols[i] + j * 8));
                    break;
                case NUMPY_FLOAT32:
                    dbl = (double)((is_null) ? 0 : *(float*)(cols[i] + j * 4));
                    break;
                case NUMPY_FLOAT64:
                    dbl = (double)((is_null) ? 0 : *(double*)(cols[i] + j * 8));
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type FLOAT");
                    goto error;
                }
                memcpy(out+out_idx, &dbl, 8);
                out_idx += 8;
                break;

            case MYSQL_TYPE_DECIMAL:
                // TODO
                PyErr_SetString(PyExc_ValueError, "unsupported data type: DECIMAL");
                goto error;
                break;

            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                // TODO
                PyErr_SetString(PyExc_ValueError, "unsupported data type: DATE");
                goto error;
                break;

            case MYSQL_TYPE_TIME:
                // TODO
                PyErr_SetString(PyExc_ValueError, "unsupported data type: TIME");
                goto error;
                break;

            case MYSQL_TYPE_DATETIME:
                // TODO
                PyErr_SetString(PyExc_ValueError, "unsupported data type: DATETIME");
                goto error;
                break;

            case MYSQL_TYPE_TIMESTAMP:
                // TODO
                PyErr_SetString(PyExc_ValueError, "unsupported data type: TIMESTAMP");
                goto error;
                break;

            case MYSQL_TYPE_YEAR:
                CHECKMEM(2);
                switch (col_types[i]) {
                case NUMPY_BOOL:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_YEAR(i8);
                    i16 = (int16_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT8:
                    i8 = *(int8_t*)(cols[i] + j * 1);
                    CHECK_YEAR(i8);
                    i16 = (int16_t)((is_null) ? 0 : i8);
                    break;
                case NUMPY_INT16:
                    i16 = *(int16_t*)(cols[i] + j * 2);
                    CHECK_YEAR(i16);
                    i16 = (int16_t)((is_null) ? 0 : i16);
                    break;
                case NUMPY_INT32:
                    i32 = *(int32_t*)(cols[i] + j * 4);
                    CHECK_YEAR(i32);
                    i16 = (int16_t)((is_null) ? 0 : i32);
                    break;
                case NUMPY_INT64:
                    i64 = *(int64_t*)(cols[i] + j * 8);
                    CHECK_YEAR(i64);
                    i16 = (int16_t)((is_null) ? 0 : i64);
                    break;
                case NUMPY_UINT8:
                    u8 = *(uint8_t*)(cols[i] + j * 1);
                    CHECK_YEAR(u8);
                    i16 = (int16_t)((is_null) ? 0 : u8);
                    break;
                case NUMPY_UINT16:
                    u16 = *(uint16_t*)(cols[i] + j * 2);
                    CHECK_YEAR(u16);
                    i16 = (int16_t)((is_null) ? 0 : u16);
                    break;
                case NUMPY_UINT32:
                    u32 = *(uint32_t*)(cols[i] + j * 4);
                    CHECK_YEAR(u32);
                    i16 = (int16_t)((is_null) ? 0 : u32);
                    break;
                case NUMPY_UINT64:
                    u64 = *(uint64_t*)(cols[i] + j * 8);
                    CHECK_YEAR(u64);
                    i16 = (int16_t)((is_null) ? 0 : u64);
                    break;
                case NUMPY_FLOAT32:
                    flt = *(float*)(cols[i] + j * 4);
                    CHECK_YEAR(flt);
                    i16 = (int16_t)((is_null) ? 0 : flt);
                    break;
                case NUMPY_FLOAT64:
                    dbl = *(double*)(cols[i] + j * 8);
                    CHECK_YEAR(dbl);
                    i16 = (int16_t)((is_null) ? 0 : dbl);
                    break;
                default:
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for output type YEAR");
                    goto error;
                }
                memcpy(out+out_idx, &i16, 2);
                out_idx += 2;
                break;

            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                if  (col_types[i] != NUMPY_OBJECT) {
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for character output types");
                    goto error;
                }

                if (is_null) {
                    CHECKMEM(8);
                    i64 = 0;
                    memcpy(out+out_idx, &i64, 8);
                    out_idx += 8;

                } else {
                    u64 = *(uint64_t*)(cols[i] + j * 8);

                    PyObject *py_str = (PyObject*)u64;
                    if (!py_str) goto error;

                    if (py_str == Py_None) {
                        CHECKMEM(8);
                        i64 = 0;
                        memcpy(out+out_idx, &i64, 8);
                        out_idx += 8;
                    } else {
                        PyObject *py_bytes = PyUnicode_AsEncodedString(py_str, "utf-8", "strict");
                        if (!py_bytes) goto error;

                        char *str = NULL;
                        Py_ssize_t str_l = 0;
                        if (PyBytes_AsStringAndSize(py_bytes, &str, &str_l) < 0) {
                            Py_DECREF(py_bytes);
                            goto error;
                        }

                        CHECKMEM(8+str_l);
                        i64 = str_l;
                        memcpy(out+out_idx, &i64, 8);
                        out_idx += 8;
                        memcpy(out+out_idx, str, str_l);
                        out_idx += str_l;
                        Py_DECREF(py_bytes);
                    }
                }
                break;

            // Use negative to indicate binary
            case -MYSQL_TYPE_VARCHAR:
            case -MYSQL_TYPE_JSON:
            case -MYSQL_TYPE_SET:
            case -MYSQL_TYPE_ENUM:
            case -MYSQL_TYPE_VAR_STRING:
            case -MYSQL_TYPE_STRING:
            case -MYSQL_TYPE_GEOMETRY:
            case -MYSQL_TYPE_TINY_BLOB:
            case -MYSQL_TYPE_MEDIUM_BLOB:
            case -MYSQL_TYPE_LONG_BLOB:
            case -MYSQL_TYPE_BLOB:
                if  (col_types[i] != NUMPY_OBJECT) {
                    PyErr_SetString(PyExc_ValueError, "unsupported numpy data type for binary output types");
                    goto error;
                }

                if (is_null) {
                    CHECKMEM(8);
                    i64 = 0;
                    memcpy(out+out_idx, &i64, 8);
                    out_idx += 8;

                } else {
                    u64 = *(uint64_t*)(cols[i] + j * 8);

                    PyObject *py_bytes = (PyObject*)u64;

                    if (py_bytes == Py_None) {
                        CHECKMEM(8);
                        i64 = 0;
                        memcpy(out+out_idx, &i64, 8);
                        out_idx += 8;
                    } else {
                        char *str = NULL;
                        Py_ssize_t str_l = 0;
                        if (PyBytes_AsStringAndSize(py_bytes, &str, &str_l) < 0) {
                            goto error;
                        }

                        CHECKMEM(8+str_l);
                        i64 = str_l;
                        memcpy(out+out_idx, &i64, 8);
                        out_idx += 8;
                        memcpy(out+out_idx, str, str_l);
                        out_idx += str_l;
                    }
                }
                break;

            default:
                PyErr_Format(PyExc_ValueError, "unrecognized database data type: %d", returns[i]);
                goto error;
            }
        }
    }

    py_out = PyMemoryView_FromMemory(out, out_idx, PyBUF_WRITE);
    if (!py_out) goto error;

exit:
    if (returns) free(returns);
    if (masks) free(masks);
    if (cols) free(cols);
    if (col_types) free(col_types);

    return py_out;

error:
    if (!py_out && out) free(out);
    Py_XDECREF(py_out);
    py_out = NULL;

    goto exit;
}


static PyObject *load_rowdat_1(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *py_data = NULL;
    PyObject *py_out = NULL;
    PyObject *py_out_row_ids = NULL;
    PyObject *py_out_rows = NULL;
    PyObject *py_row = NULL;
    PyObject *py_colspec = NULL;
    PyObject *py_str = NULL;
    PyObject *py_blob = NULL;
    Py_ssize_t length = 0;
    uint64_t row_id = 0;
    uint8_t is_null = 0;
    int8_t i8 = 0;
    int16_t i16 = 0;
    int32_t i32 = 0;
    int64_t i64 = 0;
    uint8_t u8 = 0;
    uint16_t u16 = 0;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    float flt = 0;
    double dbl = 0;
    int *ctypes = NULL;
    char *data = NULL;
    char *end = NULL;
    unsigned long long colspec_l = 0;
    unsigned long long i = 0;
    char *keywords[] = {"colspec", "data", NULL};

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", keywords, &py_colspec, &py_data)) {
        goto error;
    }

    CHECKRC(PyBytes_AsStringAndSize(py_data, &data, &length));
    end = data + (unsigned long long)length;

    colspec_l = PyObject_Length(py_colspec);
    if (colspec_l == 0) {
        goto error;
    }

    ctypes = malloc(sizeof(int) * colspec_l);

    for (i = 0; i < colspec_l; i++) {
        PyObject *py_cspec = PySequence_GetItem(py_colspec, i);
        if (!py_cspec) goto error;
        PyObject *py_ctype = PySequence_GetItem(py_cspec, 1);
        if (!py_ctype) { Py_DECREF(py_cspec); goto error; }
        ctypes[i] = (int)PyLong_AsLong(py_ctype);
        Py_DECREF(py_ctype);
        Py_DECREF(py_cspec);
        if (PyErr_Occurred()) { goto error; }
    }

    py_out_row_ids = PyList_New(0);
    if (!py_out_row_ids) goto error;

    py_out_rows = PyList_New(0);
    if (!py_out_rows) { Py_DECREF(py_out_row_ids); goto error; }

    py_out = PyTuple_New(2);
    if (!py_out) { Py_DECREF(py_out_row_ids); Py_DECREF(py_out_rows); goto error; }

    if (PyTuple_SetItem(py_out, 0, py_out_row_ids) < 0) {
        Py_DECREF(py_out_row_ids);
        Py_DECREF(py_out_rows);
        goto error;
    }

    if (PyTuple_SetItem(py_out, 1, py_out_rows) < 0) {
        Py_DECREF(py_out_rows);
        goto error;
    }

    while (end > data) {
        py_row = PyTuple_New(colspec_l);
        if (!py_row) goto error;

        row_id = *(int64_t*)data; data += 8;
        CHECKRC(PyList_Append(py_out_row_ids, PyLong_FromLongLong(row_id)));

        for (unsigned long long i = 0; i < colspec_l; i++) {
            is_null = data[0] == '\x01'; data += 1;
            if (is_null) Py_INCREF(Py_None);

            switch (ctypes[i]) {
            case MYSQL_TYPE_NULL:
                data += 1;
                CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                Py_INCREF(Py_None);
                break;

            case MYSQL_TYPE_BIT:
                // TODO
                break;

            case MYSQL_TYPE_TINY:
                i8 = *(int8_t*)data; data += 1;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromLong((long)i8)));
                }
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_TINY:
                u8 = *(uint8_t*)data; data += 1;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromUnsignedLong((unsigned long)u8)));
                }
                break;

            case MYSQL_TYPE_SHORT:
                i16 = *(int16_t*)data; data += 2;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromLong((long)i16)));
                }
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_SHORT:
                u16 = *(uint16_t*)data; data += 2;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromUnsignedLong((unsigned long)u16)));
                }
                break;

            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
                i32 = *(int32_t*)data; data += 4;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromLong((long)i32)));
                }
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONG:
            case -MYSQL_TYPE_INT24:
                u32 = *(uint32_t*)data; data += 4;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromUnsignedLong((unsigned long)u32)));
                }
                break;

            case MYSQL_TYPE_LONGLONG:
                i64 = *(int64_t*)data; data += 8;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromLongLong((long long)i64)));
                }
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONGLONG:
                u64 = *(uint64_t*)data; data += 8;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromUnsignedLongLong((unsigned long long)u64)));
                }
                break;

            case MYSQL_TYPE_FLOAT:
                flt = *(float*)data; data += 4;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyFloat_FromDouble((double)flt)));
                }
                break;

            case MYSQL_TYPE_DOUBLE:
                dbl = *(double*)data; data += 8;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyFloat_FromDouble((double)dbl)));
                }
                break;

            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
                // TODO
                break;

            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                // TODO
                break;

            case MYSQL_TYPE_TIME:
                // TODO
                break;

            case MYSQL_TYPE_DATETIME:
                // TODO
                break;

            case MYSQL_TYPE_TIMESTAMP:
                // TODO
                break;

            case MYSQL_TYPE_YEAR:
                u16 = *(uint16_t*)data; data += 2;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    CHECKRC(PyTuple_SetItem(py_row, i, PyLong_FromUnsignedLong((unsigned long)u16)));
                }
                break;

            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                i64 = *(int64_t*)data; data += 8;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    py_str = PyUnicode_FromStringAndSize(data, (Py_ssize_t)i64);
                    data += i64;
                    if (!py_str) goto error;
                    CHECKRC(PyTuple_SetItem(py_row, i, py_str));
                }
                break;

            // Use negative to indicate binary
            case -MYSQL_TYPE_VARCHAR:
            case -MYSQL_TYPE_JSON:
            case -MYSQL_TYPE_SET:
            case -MYSQL_TYPE_ENUM:
            case -MYSQL_TYPE_VAR_STRING:
            case -MYSQL_TYPE_STRING:
            case -MYSQL_TYPE_GEOMETRY:
            case -MYSQL_TYPE_TINY_BLOB:
            case -MYSQL_TYPE_MEDIUM_BLOB:
            case -MYSQL_TYPE_LONG_BLOB:
            case -MYSQL_TYPE_BLOB:
                i64 = *(int64_t*)data; data += 8;
                if (is_null) {
                    CHECKRC(PyTuple_SetItem(py_row, i, Py_None));
                    Py_INCREF(Py_None);
                } else {
                    py_blob = PyBytes_FromStringAndSize(data, (Py_ssize_t)i64);
                    data += i64;
                    if (!py_blob) goto error;
                    CHECKRC(PyTuple_SetItem(py_row, i, py_blob));
                }
                break;

            default:
                goto error;
            }
        }

        CHECKRC(PyList_Append(py_out_rows, py_row));
        Py_DECREF(py_row);
        py_row = NULL;
    }

exit:
    if (ctypes) free(ctypes);

    Py_XDECREF(py_row);

    return py_out;

error:
    Py_XDECREF(py_out);
    py_out = NULL;

    goto exit;
}


static PyObject *dump_rowdat_1(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *py_returns = NULL;
    PyObject *py_rows = NULL;
    PyObject *py_out = NULL;
    PyObject *py_rows_iter = NULL;
    PyObject *py_row = NULL;
    PyObject *py_row_iter = NULL;
    PyObject *py_row_ids = NULL;
    PyObject *py_row_ids_iter = NULL;
    PyObject *py_item = NULL;
    uint64_t row_id = 0;
    uint8_t is_null = 0;
    int8_t i8 = 0;
    int16_t i16 = 0;
    int32_t i32 = 0;
    int64_t i64 = 0;
    uint8_t u8 = 0;
    uint16_t u16 = 0;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    float flt = 0;
    double dbl = 0;
    char *out = NULL;
    unsigned long long out_l = 0;
    unsigned long long out_idx = 0;
    int *returns = NULL;
    char *keywords[] = {"returns", "row_ids", "data", NULL};
    unsigned long long i = 0;
    unsigned long long n_cols = 0;
    unsigned long long n_rows = 0;

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOO", keywords, &py_returns, &py_row_ids, &py_rows)) {
        goto error;
    }

    n_rows = (unsigned long long)PyObject_Length(py_rows);
    if (n_rows == 0) {
        py_out = PyBytes_FromStringAndSize("", 0);
        goto exit;
    }

    // Starting size, it will be resized later
    out_l = 256 * n_rows;
    out_idx = 0;
    out = malloc(out_l);
    if (!out) goto error;

    // Get return types
    n_cols = (unsigned long long)PyObject_Length(py_returns);
    if (n_cols == 0) goto error;

    returns = malloc(sizeof(int) * n_cols);
    if (!returns) goto error;

    for (i = 0; i < n_cols; i++) {
        PyObject *py_item = PySequence_GetItem(py_returns, i);
        if (!py_item) goto error;
        returns[i] = (int)PyLong_AsLong(py_item);
        Py_DECREF(py_item);
        if (PyErr_Occurred()) { goto error; }
    }

#define CHECKMEM(x) \
    if ((out_idx + x) > out_l) { \
        out_l = out_l * 2 + x; \
        out = realloc(out, out_l); \
        if (!out) goto error; \
    }

    py_rows_iter = PyObject_GetIter(py_rows);
    if (!py_rows_iter) goto error;

    py_row_ids_iter = PyObject_GetIter(py_row_ids);
    if (!py_row_ids_iter) goto error;

    while ((py_row = PyIter_Next(py_rows_iter))) {
        py_row_iter = PyObject_GetIter(py_row);
        if (!py_row_iter) goto error;

        // First item is always a row ID
        py_item = PyIter_Next(py_row_ids_iter);
        if (!py_item) goto error;
        row_id = (int64_t)PyLong_AsLongLong(py_item);

        CHECKMEM(8);
        memcpy(out+out_idx, &row_id, 8);
        out_idx += 8;

        i = 0;

        while ((py_item = PyIter_Next(py_row_iter))) {

            is_null = (uint8_t)(py_item == Py_None);

            CHECKMEM(1);
            memcpy(out+out_idx, &is_null, 1);
            out_idx += 1;

            switch (returns[i]) {
            case MYSQL_TYPE_BIT:
                // TODO
                break;

            case MYSQL_TYPE_TINY:
                CHECKMEM(1);
                i8 = (is_null) ? 0 : (int8_t)PyLong_AsLong(py_item);
                memcpy(out+out_idx, &i8, 1);
                out_idx += 1;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_TINY:
                CHECKMEM(1);
                u8 = (is_null) ? 0 : (uint8_t)PyLong_AsUnsignedLong(py_item);
                memcpy(out+out_idx, &u8, 1);
                out_idx += 1;
                break;

            case MYSQL_TYPE_SHORT:
                CHECKMEM(2);
                i16 = (is_null) ? 0 : (int16_t)PyLong_AsLong(py_item);
                memcpy(out+out_idx, &i16, 2);
                out_idx += 2;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_SHORT:
                CHECKMEM(2);
                u16 = (is_null) ? 0 : (uint16_t)PyLong_AsUnsignedLong(py_item);
                memcpy(out+out_idx, &u16, 2);
                out_idx += 2;
                break;

            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
                CHECKMEM(4);
                i32 = (is_null) ? 0 : (int32_t)PyLong_AsLong(py_item);
                memcpy(out+out_idx, &i32, 4);
                out_idx += 4;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONG:
            case -MYSQL_TYPE_INT24:
                CHECKMEM(4);
                u32 = (is_null) ? 0 : (uint32_t)PyLong_AsUnsignedLong(py_item);
                memcpy(out+out_idx, &u32, 4);
                out_idx += 4;
                break;

            case MYSQL_TYPE_LONGLONG:
                CHECKMEM(8);
                i64 = (is_null) ? 0 : (int64_t)PyLong_AsLongLong(py_item);
                memcpy(out+out_idx, &i64, 8);
                out_idx += 8;
                break;

            // Use negative to indicate unsigned
            case -MYSQL_TYPE_LONGLONG:
                CHECKMEM(8);
                u64 = (is_null) ? 0 : (uint64_t)PyLong_AsUnsignedLongLong(py_item);
                memcpy(out+out_idx, &u64, 8);
                out_idx += 8;
                break;

            case MYSQL_TYPE_FLOAT:
                CHECKMEM(4);
                flt = (is_null) ? 0 : (float)PyFloat_AsDouble(py_item);
                memcpy(out+out_idx, &flt, 4);
                out_idx += 4;
                break;

            case MYSQL_TYPE_DOUBLE:
                CHECKMEM(8);
                dbl = (is_null) ? 0 : (double)PyFloat_AsDouble(py_item);
                memcpy(out+out_idx, &dbl, 8);
                out_idx += 8;
                break;

            case MYSQL_TYPE_DECIMAL:
                // TODO
                break;

            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                // TODO
                break;

            case MYSQL_TYPE_TIME:
                // TODO
                break;

            case MYSQL_TYPE_DATETIME:
                // TODO
                break;

            case MYSQL_TYPE_TIMESTAMP:
                // TODO
                break;

            case MYSQL_TYPE_YEAR:
                CHECKMEM(2);
                i16 = (is_null) ? 0 : (int16_t)PyLong_AsLong(py_item);
                memcpy(out+out_idx, &i16, 2);
                out_idx += 2;
                break;

            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                if (is_null) {
                    CHECKMEM(8);
                    i64 = 0;
                    memcpy(out+out_idx, &i64, 8);
                    out_idx += 8;
                } else {
                    PyObject *py_bytes = PyUnicode_AsEncodedString(py_item, "utf-8", "strict");
                    if (!py_bytes) goto error;

                    char *str = NULL;
                    Py_ssize_t str_l = 0;
                    if (PyBytes_AsStringAndSize(py_bytes, &str, &str_l) < 0) {
                        Py_DECREF(py_bytes);
                        goto error;
                    }

                    CHECKMEM(8+str_l);
                    i64 = str_l;
                    memcpy(out+out_idx, &i64, 8);
                    out_idx += 8;
                    memcpy(out+out_idx, str, str_l);
                    out_idx += str_l;
                    Py_DECREF(py_bytes);
                }
                break;

            // Use negative to indicate binary
            case -MYSQL_TYPE_VARCHAR:
            case -MYSQL_TYPE_JSON:
            case -MYSQL_TYPE_SET:
            case -MYSQL_TYPE_ENUM:
            case -MYSQL_TYPE_VAR_STRING:
            case -MYSQL_TYPE_STRING:
            case -MYSQL_TYPE_GEOMETRY:
            case -MYSQL_TYPE_TINY_BLOB:
            case -MYSQL_TYPE_MEDIUM_BLOB:
            case -MYSQL_TYPE_LONG_BLOB:
            case -MYSQL_TYPE_BLOB:
                if (is_null) {
                    CHECKMEM(8);
                    i64 = 0;
                    memcpy(out+out_idx, &i64, 8);
                    out_idx += 8;
                } else {
                    char *str = NULL;
                    Py_ssize_t str_l = 0;
                    if (PyBytes_AsStringAndSize(py_item, &str, &str_l) < 0) {
                        goto error;
                    }

                    CHECKMEM(8+str_l);
                    i64 = str_l;
                    memcpy(out+out_idx, &i64, 8);
                    out_idx += 8;
                    memcpy(out+out_idx, str, str_l);
                    out_idx += str_l;
                }
                break;

            default:
                goto error;
            }

            Py_DECREF(py_item);
            py_item = NULL;

            i++;
        }

        Py_DECREF(py_row);
        py_row = NULL;
    }

    py_out = PyMemoryView_FromMemory(out, out_idx, PyBUF_WRITE);
    if (!py_out) goto error;

exit:
    if (returns) free(returns);

    Py_XDECREF(py_item);
    Py_XDECREF(py_row_iter);
    Py_XDECREF(py_row_ids_iter);
    Py_XDECREF(py_row);
    Py_XDECREF(py_rows_iter);

    return py_out;

error:
    if (!py_out && out) free(out);
    Py_XDECREF(py_out);
    py_out = NULL;

    goto exit;
}


static PyMethodDef PyMySQLAccelMethods[] = {
    {"read_rowdata_packet", (PyCFunction)read_rowdata_packet, METH_VARARGS | METH_KEYWORDS, "PyMySQL row data packet reader"},
    {"dump_rowdat_1", (PyCFunction)dump_rowdat_1, METH_VARARGS | METH_KEYWORDS, "ROWDAT_1 formatter for external functions"},
    {"load_rowdat_1", (PyCFunction)load_rowdat_1, METH_VARARGS | METH_KEYWORDS, "ROWDAT_1 parser for external functions"},
    {"dump_rowdat_1_numpy", (PyCFunction)dump_rowdat_1_numpy, METH_VARARGS | METH_KEYWORDS, "ROWDAT_1 formatter for external functions which takes numpy.arrays"},
    {"load_rowdat_1_numpy", (PyCFunction)load_rowdat_1_numpy, METH_VARARGS | METH_KEYWORDS, "ROWDAT_1 parser for external functions which creates numpy.arrays"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef _singlestoredb_accelmodule = {
    PyModuleDef_HEAD_INIT,
    "_singlestoredb_accel",
    "PyMySQL row data packet reader accelerator",
    -1,
    PyMySQLAccelMethods
};

PyMODINIT_FUNC PyInit__singlestoredb_accel(void) {
#ifndef Py_LIMITED_API
    PyDateTime_IMPORT;
#endif

    StateType = (PyTypeObject*)PyType_FromSpec(&StateType_spec);
    if (StateType == NULL || PyType_Ready(StateType) < 0) {
        return NULL;
    }

    // Populate ints
    for (int i = 0; i < 62; i++) {
        PyInts[i] = PyLong_FromLong(i);
    }

    PyStr.unbuffered_active = PyUnicode_FromString("unbuffered_active");
    PyStr._state = PyUnicode_FromString("_state");
    PyStr.affected_rows = PyUnicode_FromString("affected_rows");
    PyStr.warning_count = PyUnicode_FromString("warning_count");
    PyStr.connection = PyUnicode_FromString("connection");
    PyStr.has_next = PyUnicode_FromString("has_next");
    PyStr.options = PyUnicode_FromString("options");
    PyStr.Decimal = PyUnicode_FromString("Decimal");
    PyStr.date = PyUnicode_FromString("date");
    PyStr.time = PyUnicode_FromString("time");
    PyStr.timedelta = PyUnicode_FromString("timedelta");
    PyStr.datetime = PyUnicode_FromString("datetime");
    PyStr.loads = PyUnicode_FromString("loads");
    PyStr.field_count = PyUnicode_FromString("field_count");
    PyStr.converters = PyUnicode_FromString("converters");
    PyStr.fields = PyUnicode_FromString("fields");
    PyStr.flags = PyUnicode_FromString("flags");
    PyStr.scale = PyUnicode_FromString("scale");
    PyStr.type_code = PyUnicode_FromString("type_code");
    PyStr.name = PyUnicode_FromString("name");
    PyStr.table_name = PyUnicode_FromString("table_name");
    PyStr._sock = PyUnicode_FromString("_sock");
    PyStr.settimeout = PyUnicode_FromString("settimeout");
    PyStr._read_timeout = PyUnicode_FromString("_read_timeout");
    PyStr._rfile = PyUnicode_FromString("_rfile");
    PyStr.read = PyUnicode_FromString("read");
    PyStr.x_errno = PyUnicode_FromString("errno");
    PyStr._result = PyUnicode_FromString("_result");
    PyStr._next_seq_id = PyUnicode_FromString("_next_seq_id");
    PyStr.rows = PyUnicode_FromString("rows");
    PyStr.namedtuple = PyUnicode_FromString("namedtuple");
    PyStr.Row = PyUnicode_FromString("Row");
    PyStr.Series = PyUnicode_FromString("Series");
    PyStr.array = PyUnicode_FromString("array");
    PyStr.vectorize = PyUnicode_FromString("vectorize");

    PyObject *decimal_mod = PyImport_ImportModule("decimal");
    if (!decimal_mod) goto error;
    PyObject *datetime_mod = PyImport_ImportModule("datetime");
    if (!datetime_mod) goto error;
    PyObject *json_mod = PyImport_ImportModule("json");
    if (!json_mod) goto error;
    PyObject *collections_mod = PyImport_ImportModule("collections");
    if (!collections_mod) goto error;

    PyFunc.decimal_Decimal = PyObject_GetAttr(decimal_mod, PyStr.Decimal);
    if (!PyFunc.decimal_Decimal) goto error;
    PyFunc.datetime_date = PyObject_GetAttr(datetime_mod, PyStr.date);
    if (!PyFunc.datetime_date) goto error;
    PyFunc.datetime_timedelta = PyObject_GetAttr(datetime_mod, PyStr.timedelta);
    if (!PyFunc.datetime_timedelta) goto error;
    PyFunc.datetime_time = PyObject_GetAttr(datetime_mod, PyStr.time);
    if (!PyFunc.datetime_time) goto error;
    PyFunc.datetime_datetime = PyObject_GetAttr(datetime_mod, PyStr.datetime);
    if (!PyFunc.datetime_datetime) goto error;
    PyFunc.json_loads = PyObject_GetAttr(json_mod, PyStr.loads);
    if (!PyFunc.json_loads) goto error;
    PyFunc.collections_namedtuple = PyObject_GetAttr(collections_mod, PyStr.namedtuple);
    if (!PyFunc.collections_namedtuple) goto error;

    PyObj.namedtuple_kwargs = PyDict_New();
    if (!PyObj.namedtuple_kwargs) goto error;
    if (PyDict_SetItemString(PyObj.namedtuple_kwargs, "rename", Py_True)) {
        goto error;
    }

    PyObj.create_numpy_array_args = PyTuple_New(1);
    if (!PyObj.create_numpy_array_args) goto error;

    PyObj.create_numpy_array_kwargs = PyDict_New();
    if (!PyObj.create_numpy_array_kwargs) goto error;
    if (PyDict_SetItemString(PyObj.create_numpy_array_kwargs, "copy", Py_False)) {
        goto error;
    }

    return PyModule_Create(&_singlestoredb_accelmodule);

error:
    return NULL;
}
