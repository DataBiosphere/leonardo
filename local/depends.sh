#!/bin/bash
# Build helm-scala-sdk and render configs.

# Stop immediately on any non-zero exit codes.
set -e

REPO_ROOT="$(git rev-parse --show-toplevel)"
LOCAL_DIR="${REPO_ROOT}/local"

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

	# Check if the dir already exists,
	# and if so, give the user prompt.
	if [ -d "${_tmp_dir}" ]; then
		read -p "${_prompt} [${_yes_no_str}] " _proceed
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

build_helm_golib() {
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
	go build -o "${_build_dir}/libhelm.so" -buildmode=c-shared main.go
	cd "${_cwd}"
}

get_file() {
	local _secret="${1}"
	local _file_name="${2}"
	local _output="${3}"
	kubectl -n terra-dev get secret "${_secret}" -o 'go-template={{ index .data "${_file_name}" | base64decode }}' > "${_output}"
}

render_configs() {
	local _out_dir="${1}"

	local _cluster=terra-dev
	echo "Configuring access to ${_cluster}..."
	gcloud container clusters get-credentials --zone us-central1-a --project broad-dsde-dev ${_cluster}

	echo "Copying resources from kubernetes..."

	# Get secret backend env vars
	kubectl -n terra-dev get secret leonardo-backend-env-secrets -o go-template='
{{- range $k, $v := .data }}
	{{- printf "%s=" $k }}
	{{- if not $v}}
		{{- $v }}
	{{- else }}
		{{- $v | base64decode }}
	{{- end }}
	{{- "\n" }}
{{- end }}' > "${_out_dir}/k8s-secrets.env"

	# Get non-secret backend env vars
	kubectl -n terra-dev get pods -o go-template='
{{- range $pod := .items }}
	{{- range $container := $pod.spec.containers }}
		{{- if (eq $container.name "leonardo-backend") }}
			{{- range $var := $container.env }}
				{{- printf "%s=%s\n" $var.name $var.value }}
			{{- end }}
		{{- end }}
	{{- end }}
{{- end }}' | \
	sed '/^VALID_HOSTS/s/leonardo\.dsde-dev\.broadinstitute\.org$/local\.dsde-dev\.broadinstitute\.org:30433/' \
		> "${_out_dir}/k8s-clear.env"

	get_file leonardo-sa-secret leonardo-account.json "${_out_dir}/leonardo-account.json"
}

BUILD_HELM_GOLIB=false
RENDER_CONFIGS=false
PRINT_HELP=false

HELP_TEXT=$(cat <<EOF
 Build helm Go library and/or render resources from kubernetes.
 ${0} [command]

 Commands:
   helm: Build the Golang helm library.
   configs:  Render application resource files from kubernetes.
   -h | --help: print help text.
 Examples:
   1. Build the helm Go library and render resources.
      $ ${0}
   2. Only build the helm Go library
      $ ${0} helm
   3. Only render resources from kubernetes.
      $ ${0} render
EOF
)

if [ -z "${1}" ]; then
    BUILD_HELM_GOLIB=true
    RENDER_CONFIGS=true
fi

while [ "${1}" != "" ]; do
    case ${1} in
        helm)
            BUILD_HELM_GOLIB=true
            ;;
        configs)
			RENDER_CONFIGS=true
			;;
        -h | --help)
            PRINT_HELP=true
            ;;
        *)
            echo "Unrecognized argument '${1}'."
            echo "run '${0} -h' to see available arguments."
            exit 1
            ;;
    esac
    shift
done

if ${PRINT_HELP}; then
    echo -e "${HELP_TEXT}"
    exit 0
fi

if ${BUILD_HELM_GOLIB}; then
	ask_and_run \
		"Rebuild Helm Go library?" "N" \
		"build_helm_golib" \
		"${LOCAL_DIR}/helm-scala-sdk/out"
fi

if ${RENDER_CONFIGS}; then
	ask_and_run \
		"Re-render configs?" "Y" \
		"render_configs" \
		"${REPO_ROOT}/http/src/main/resources/rendered"
fi
