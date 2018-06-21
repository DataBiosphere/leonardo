#!/bin/bash

# This script cycles through string and boolean configuration
# options (found and documented in ui/public/config.json) and,
# if matching environmental variables are defined, it uses jq
# to substitute the original value with the environment's value.

CONFIG_FILE="${1}"

# Lists of values to check, separated by type.
declare -a BOOL_VALUES=("FILTER_TO_CURRENT_USER" "DISABLE_PROJECT_ENTRY")
declare -a STRING_VALUES=("DEPLOY_ENVIRONMENT" "OAUTH_CLIENT_ID" "DEFAULT_PROJECT" "STARTUP_SCRIPT_URI" "EXTRA_GOOGLE_SCOPES")

# Load config file to string.
CONTENTS="$(cat "$CONFIG_FILE")"

# Use indirect expansion of the bool parameter to check if the value
# is explicitly set, then, if so, modify the config contents.
for bool_param in "${BOOL_VALUES[@]}"; do
    if ! [ -z "${!bool_param}" ]; then
        CONTENTS="$(echo "$CONTENTS" | jq ".${bool_param} = ${!bool_param}")"
    fi
done

# Use indirect expansion of the string parameter to check if the value
# is explicitly set, then, if so, modify the config contents.
for param in "${STRING_VALUES[@]}"; do
    if ! [ -z "${!param}" ]; then
        CONTENTS="$(echo "$CONTENTS" | jq ".${param} = \"${!param}\"")"
    fi
done

# Delete comments.
for param_key in $(echo "$CONTENTS" | jq -c "keys[]"); do
    if grep -Fq "_COMMENT" <<< "$param_key"; then
        CONTENTS="$(echo "$CONTENTS" | jq "del(.${param_key})")"
    fi
done


# Write config to file.
echo "${CONTENTS}" > "${CONFIG_FILE}"
