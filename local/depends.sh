#!/bin/bash
# Build helm-scala-sdk and render configs.

# Stop immediately on any non-zero exit codes.
set -e

export REPO_ROOT="$(git rev-parse --show-toplevel)"
export LOCAL_DIR="${REPO_ROOT}/local"
export RENDER_DIR="${REPO_ROOT}/http/src/main/resources/rendered"
export HELM_BUILD_DIR="${LOCAL_DIR}/helm-scala-sdk/out"

# Try to create a temp dir and prompt the user with a msg
# if it already exists, then run a function after, if desired.
# Args:
# 1. Prompt to show the user (should be a yes/no question).
# 2. Default response if the user presses enter.
# 3. Function to run if the temp dir doesn't exist or the
#    user wishes to redo work.
# 4. Absolute path to the directory to create.
ask_and_run() {
	local _prompt="${1}"
	local _default_answer="${2}"
	local _func="${3}"
	local _tmp_dir="${4}"

	local _yes_no_str=""
	if [ "${_default_answer}" = "Y" ]; then
		_yes_no_str="Y/n"
	elif [ "${_default_answer}" = "N" ]; then
		_yes_no_str="y/N"
	else
		echo "Error: Given default answer '${_default_answer}' is invalid. Exiting..."
		return 1
	fi

	# Check if the dir already exists, and if so, give the user prompt.
	if [ -d "${_tmp_dir}" ]; then
		# If the user wants auto default values, just print the
		# default behavior and move on, otherwise ask.
		if ! ${USE_PROMPT_DEFAULTS}; then
			read -p "${_prompt} [${_yes_no_str}] " _proceed
		fi

		_proceed=${_proceed:-${_default_answer}}
		if [ "${_proceed}" = "y" ] || [ "${_proceed}" = "Y" ]; then
			rm -rf "${_tmp_dir}"
		elif [ "${_proceed}" = "n" ] || [ "${_proceed}" = "N" ]; then
			return 0
		else
			echo "Invalid answer: '${_proceed}' - exiting..."
			return 1
		fi
	fi
	mkdir "${_tmp_dir}"

	# Run the provided function with the temp dir as an arg.
	eval "${_func}" "${_tmp_dir}"
}

# Build the Go helm library locally
build_helm_golib() {
	echo \
"
########################################
##### BUILDING THE GO HELM LIBRARY #####
########################################
"
	local _build_dir="${1}"

	# Save current working dir.
	local _cwd=$(pwd)

	# Build the helm Go library.
	cd "${LOCAL_DIR}/helm-scala-sdk/helm-go-lib"
	local _commit=$(git rev-parse --short HEAD)
	local _branch=$(git branch --show-current)
	if [ -z "${_branch}" ]; then
		echo "Building Helm Go library from commit ${_commit}"
	else
		echo "Building Helm Go library from branch '${_branch}' (${_commit})"
	fi
	go build -o "${_build_dir}/libhelm.dylib" -buildmode=c-shared main.go
	cd "${_cwd}"
}

# Pull config files and env vars from k8s and adapt and override them
# for local development
render_configs() {
	echo \
"
##########################################
##### RENDERING CONFIG AND ENV FILES #####
##########################################
"
	local _out_dir="${1}"

	local _cluster=terra-dev
	echo "Configuring access to ${_cluster}..."
	echo "(Note: non-split-tunnel VPN is required when working remotely)"
	gcloud container clusters get-credentials --zone us-central1-a --project broad-dsde-dev ${_cluster}

	# Get CloudSQL proxy GOOGLE_PROJECT and CLOUDSQL_ZONE from dev as defaults,
	# CLOUDSQL_INSTANCE must be set by the user in their environment.
	local _csp_gproj="$(kubectl -n terra-dev get secret leonardo-cloudsql-instance-eso -o 'go-template={{ .data.project | base64decode }}')"
	local _csp_zone="$(kubectl -n terra-dev get secret leonardo-cloudsql-instance-eso -o 'go-template={{ .data.region | base64decode }}')"
	local _csp_dev_instance="$(kubectl -n terra-dev get secret leonardo-cloudsql-instance-eso -o 'go-template={{ .data.name | base64decode }}')"

	# Check for CloudSQL proxy env settings, tell user to set if missing.
	if [ -z "${CLOUDSQL_INSTANCE}" ]; then
		echo "ERROR: CLOUDSQL_INSTANCE is unset.
	This is used for the CloudSQL proxy environment. There is no default.
	To set it, run \`export CLOUDSQL_INSTANCE=...\` in this shell, or set it in your shell rc file."
		return 1
	elif [ "${CLOUDSQL_INSTANCE}" = "${_csp_dev_instance}" ]; then
		echo "ERROR: CLOUDSQL_INSTANCE is set to the dev database, which is forbidden.
	If you need to work with it, use other tools."
		return 1
	fi
	if [ -z "${DB_PASSWORD}" ]; then
		echo "ERROR: DB_PASSWORD is unset.
	This is used to authenticate to the database. There is no default."
		return 1
	fi
	if [ -z "${GOOGLE_PROJECT}" ]; then
		echo "INFO: GOOGLE_PROJECT is unset.
	This is used for the CloudSQL proxy environment. The default is ${_csp_gproj}.
	To override, run \`export GOOGLE_PROJECT=...\` in this shell, or set it in your shell rc file."
		export GOOGLE_PROJECT="${_csp_gproj}"
	fi
	if [ -z "${CLOUDSQL_ZONE}" ]; then
		echo "INFO: CLOUDSQL_ZONE is unset.
	This is used for the CloudSQL proxy environment. The default is ${_csp_zone}.
	To override, run \`export CLOUDSQL_ZONE=...\` in this shell, or set it in your shell rc file."
		export CLOUDSQL_ZONE="${_csp_zone}"
	fi

	echo "Copying resources from kubernetes..."

	# Get secret backend env vars
	kubectl -n terra-dev get secret leonardo-backend-env-secrets-eso -o go-template='
{{- range $k, $v := .data }}
	{{- printf "%s=" $k }}
	{{- if not $v}}
		{{- $v }}
	{{- else }}
		{{- $v | base64decode }}
	{{- end }}
	{{- "\n" }}
{{- end }}' > "${_out_dir}/k8s.env"

	# Get non-secret backend env vars
	kubectl -n terra-dev get pods -o go-template='
{{- range $pod := .items }}
	{{- range $container := $pod.spec.containers }}
		{{- if (eq $container.name "leonardo-backend") }}
			{{- range $var := $container.env }}
				{{- if (and $var.name $var.value) }}
					{{- printf "%s=%s\n" $var.name $var.value }}
				{{- end }}
			{{- end }}
		{{- end }}
	{{- end }}
{{- end }}' >> "${_out_dir}/k8s.env"

	# Remove comments and empty lines from unset.env
	grep -v '^$\|^\s*\#' "${LOCAL_DIR}/unset.env" | \
	sed '/^$/d' > "${_out_dir}/unset.env"

	# Remove vars from k8s.env using what's listed in unset.env
	local _unset_sed_cmd=""
	while read env_var; do
		_unset_sed_cmd="${_unset_sed_cmd}/^${env_var}/d;"
	done < "${_out_dir}/unset.env"
	cat "${_out_dir}/k8s.env" | sed "${_unset_sed_cmd}" > "${_out_dir}/k8s-unset.env"

	# Render overrides template using current env
	envsubst < "${LOCAL_DIR}/overrides.env" > "${_out_dir}/overrides.env"

	# Replace k8s env vars with local overrides
	# Ignore empty lines and comment lines
	# Remove empty lines
	# Alphabetize everything
	sort -u -t '=' -k 1,1 "${_out_dir}/overrides.env" "${_out_dir}/k8s-unset.env" | \
	grep -v '^$\|^\s*\#' | \
	sed '/^$/d' | \
	sort > "${_out_dir}/sbt.env"

	# Create an IntelliJ-specific env file that includes the VALID_HOSTS which shells hate
	cp "${_out_dir}/sbt.env" "${_out_dir}/intellij.env"
	echo "VALID_HOSTS.0=local.dsde-dev.broadinstitute.org" >> "${_out_dir}/intellij.env"

	echo "IntelliJ info:"
	echo -e "\tSet JVM options on the Application config to: $(grep JAVA_OPTS "${_out_dir}/intellij.env" | sed 's/JAVA_OPTS=//')"
	echo -e "\tUse the following file for EnvFile: ${_out_dir}/intellij.env"

	echo "Pubsub info:"
	echo -n -e '\t' # Whitespace for readability.
	grep 'TOPIC_NAME' "${_out_dir}/sbt.env"
	echo -n -e '\t'
	grep 'NON_LEO_SUBSCRIPTION_NAME' "${_out_dir}/sbt.env"

	# Create source-able env file (i.e. add "export ...")
	# Quote all values bc this is for shells
	sed 's/^/export /g
		 s/=/="/;s/$/"/' \
		"${_out_dir}/sbt.env" > "${_out_dir}/sbt.env.sh"

	# Remove comments and empty lines
	# Render sqlproxy template using current env
	grep -v '^$\|^\s*\#' "${LOCAL_DIR}/sqlproxy.env" | \
	sed '/^$/d' | \
	envsubst \
		> "${_out_dir}/sqlproxy.env"

	echo "CloudSQL db name: ${CLOUDSQL_INSTANCE}"

	# Tunneling certs
	kubectl -n terra-dev get secret leonardo-sa-secret -o 'go-template={{ index .data "leonardo-account.json" | base64decode }}' > ${_out_dir}/leonardo-account.json
	kubectl -n terra-dev get secret leonardo-sa-secret -o 'go-template={{ index .data "leonardo-account.pem" | base64decode }}' > ${_out_dir}/leonardo-account.pem
	kubectl -n terra-dev get secret leonardo-application-secret-eso -o 'go-template={{ index .data "jupyter-server.crt" | base64decode }}' > ${_out_dir}/jupyter-server.crt
	kubectl -n terra-dev get secret leonardo-application-secret-eso -o 'go-template={{ index .data "jupyter-server.key" | base64decode }}' > ${_out_dir}/jupyter-server.key
	kubectl -n terra-dev get secret leonardo-application-secret-eso -o 'go-template={{ index .data "leo-client.p12" | base64decode }}' > ${_out_dir}/leo-client.p12
	kubectl -n terra-dev get secret leonardo-application-secret-eso -o 'go-template={{ index .data "rootCA.key" | base64decode }}' > ${_out_dir}/rootCA.key
	kubectl -n terra-dev get secret leonardo-application-secret-eso -o 'go-template={{ index .data "rootCA.pem" | base64decode }}' > ${_out_dir}/rootCA.pem

	# OAuth and proxy configs
	kubectl -n terra-dev get configmap leonardo-oauth2-configmap -o 'go-template={{index .data "oauth2.conf"}}' > ${_out_dir}/oauth2.conf
	# Local dev uses a macOS-specific docker replacement hostname for locahost, so replace all instances in the proxy config.
	kubectl -n terra-dev get configmap leonardo-site-configmap -o 'go-template={{index .data "site.conf"}}' | sed 's/localhost/host\.docker\.internal/g' > ${_out_dir}/site.conf

	# local.dsde-dev.broadinstitute.org cert
	kubectl -n local-dev get secrets local-dev-cert -o 'go-template={{ index .data "tls.crt" | base64decode }}' > ${_out_dir}/server.crt
	kubectl -n local-dev get secrets local-dev-cert -o 'go-template={{ index .data "tls.key" | base64decode }}' > ${_out_dir}/server.key

	# Get proxy env vars
	{
	echo B2C_APPLICATION_ID=$(kubectl -n terra-dev get secret leonardo-proxy-b2c-secrets -o 'go-template={{ index .data "application-id" }}' | base64 --decode)
	} > ${_out_dir}/proxy.env
}

BUILD_HELM_GOLIB=false
RENDER_CONFIGS=false
USE_PROMPT_DEFAULTS=false

HELP_TEXT=$(cat <<EOF
 ${0} [command] [flags]
 Build helm Go library and/or render resources from kubernetes.

 Commands:
   helm:    Build the Golang helm library.
   configs: Render application resource files from kubernetes.
 Flags:
   -y | --yes:  Use default values instead of prompting for input.
   -h | --help: Print this help message.
 Examples:
   1. Build the helm Go library and render resources.
      $ ${0} -y
   2. Only build the helm Go library
      $ ${0} helm
   3. Only render resources from kubernetes.
      $ ${0} render
EOF
)

print_help() {
	echo -e "${HELP_TEXT}"
    exit 0
}

# If no command or flags are specified, ask about everything.
if [ -z "${1}" ]; then
	RENDER_CONFIGS=true
    BUILD_HELM_GOLIB=true
fi

while [ "${1}" != "" ]; do
    case ${1} in
		configs)
			if ${BUILD_HELM_GOLIB}; then
				echo "Error: You can specify up to one action at a time."
				print_help
			fi
			RENDER_CONFIGS=true
			USE_PROMPT_DEFAULTS=true # Individual commands default to Y
			;;
        helm)
			if ${RENDER_CONFIGS}; then
				echo "Error: You can specify up to one action at a time."
				print_help
			fi
            BUILD_HELM_GOLIB=true
            USE_PROMPT_DEFAULTS=true # Individual commands default to Y
            ;;
		-y | --yes)
			USE_PROMPT_DEFAULTS=true
			;;
        -h | --help)
            print_help
            ;;
        *)
            echo "Unrecognized argument '${1}'."
            echo "run '${0} -h' to see available arguments."
            exit 1
            ;;
    esac
    shift
done

# If no commands were specified but -y|--yes was given,
# do both helm and config commands, but with default answers.
if ${USE_PROMPT_DEFAULTS} && ! ${RENDER_CONFIGS} && ! ${BUILD_HELM_GOLIB}; then
	RENDER_CONFIGS=true
	BUILD_HELM_GOLIB=true
fi

if ${RENDER_CONFIGS}; then
	ask_and_run \
		"Re-render configs?" "Y" \
		"render_configs" "${RENDER_DIR}"
fi

if ${BUILD_HELM_GOLIB}; then
	ask_and_run \
		"Rebuild Helm Go library?" "Y" \
		"build_helm_golib" "${HELM_BUILD_DIR}"
fi
