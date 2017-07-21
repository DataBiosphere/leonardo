Guidelines For Swagger Editors
==============================

`path`s and `definition`s are organized alphabetically.

each `path` and `definition` should be separated by a newline. If you have multiple
operations (e.g. GET and POST) for a single `path`, they should not be separated.

`security` and `produces: application/json` are defined at the top level. You should only
include those keys in your endpoints if you need to override the global definitions.

We keep the entire swagger definition in a single file so it can be edited/validated via
http://editor.swagger.io. When you make changes to swagger, you are responsible for validating
the file. Warnings should be avoided; errors MUST be eliminated.

Wherever possible, use `$ref:`s to centralize reusable blocks of code and reduce file size.
