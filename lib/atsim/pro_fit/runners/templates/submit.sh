#! /bin/bash
#PBS -o {{submissionpath}}/job.out
#PBS -e {{submissionpath}}/job.err
#PBS -S /bin/bash
#PBS -h
{% if arrayEnd > 1 -%}
#PBS {{arrayFlag}} {{arrayStart}}-{{arrayEnd}}
{%- endif %}
{% if pbsinclude -%}
{{pbsinclude}}
{%- endif %}

{%- if arrayEnd == 1 -%}
{{arrayIDVariable}}=1
{%- endif -%}

{% for lrt in localToRemotePathTuples %}
JOBS[{{loop.index}}]="{{submissionpath}}/{{lrt.remotePath}}"
{%- endfor %}

UUID="{{uuid}}"

export JOB_DIR="${JOBS[{{arrayIDVariable}}]}"

# Make temporary directory
TMPDIR="$(mktemp -d)"

# Define some functions

function bscp(){
	scp -o "BatchMode yes" $@
}

function scp_copyin(){
	bscp -r "${PBS_O_HOST}":"${JOB_DIR}/job_files" "${TMPDIR}"
	bscp -r "${PBS_O_HOST}":"${JOB_DIR}/runner_files" "${TMPDIR}"
}

function scp_copyout(){
	bscp -r "${TMPDIR}/job_files" "${PBS_O_HOST}":"${JOB_DIR}/job_files/output"
}

function cp_copyin(){
	cp -r "${JOB_DIR}/"* "${TMPDIR}"
}

function cp_copyout(){
	cp -r "${TMPDIR}/job_files" "${JOB_DIR}/job_files/output"
}


# If the job directory is cross mounted, we can simply copy to the temporary directory
# otherwise we will need to scp to the execution host.
#
# Check for cross mount by comparing the uuid embedded in this file to that stored in 
# in PBS_O_WORKDIR

COPY_IN=scp_copyin
COPY_OUT=scp_copyout

if [ -f "${PBS_O_WORKDIR}/uuid" ]; then
	# Check that the contents of the UUID variable matches what's in the file.
	FILE_UUID="$(cat "${PBS_O_WORKDIR}/uuid")"
	if [ "$UUID" = "$FILE_UUID" ]; then
		# Source file system is cross mounted
		COPY_IN=cp_copyin
		COPY_OUT=cp_copyout
	fi
fi

# Set a trap to trigger clean-up no matter what.
function end_handler(){
	$COPY_OUT
	cd /
	rm -rf "$TMPDIR"
}

trap end_handler SIGTERM SIGKILL EXIT

$COPY_IN

if [ ! -d "${TMPDIR}/job_files" ]; then
	exit 10
fi

cd "${TMPDIR}/job_files"
chmod u+x runjob
./runjob
echo $? > STATUS
