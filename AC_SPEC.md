# Summary

We need to support the JSON data type in the UDF application. This means that functions decorated with the @udf decorator need to have a type that corresponds to the JSON type in singlestoredb.

For scalar-valued parameters in UDFs, we will use the Dict[str, Any] type. For vector-valued params, we have to use a generic object type, but in the @udf decorator we can support a Dict[str, Any] override. We can also add type aliases like for other vector types (functions/typing).

Make sure that the SQL type mappings in CREATE EXTERNAL FUNCTION map to JSON.

Make sure that Function signatures in the show/create-function have the JSON type.

## Code locations
• singlestoredb/functions/

---

## Visual Description

This appears to be a handwritten specification or development note on white paper with black ink. The document is structured with a bullet-pointed summary section followed by implementation notes and a code locations section. The handwriting is clear and legible, written in a casual style typical of technical notes or planning documents.

The content focuses on implementing JSON data type support in UDF (User Defined Functions) applications, specifically for SingleStoreDB, with technical details about type mappings and decorator usage.
