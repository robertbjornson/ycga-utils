#!/bin/bash

set -e

function usage
{
  echo "Usage: $0 dir sharename user desc"
  exit 1
}

function error_exit
{
  echo "Error: ${1:-"Unknown Error"}" 1>&2
  exit 1
}

if [ $# -ne 4 ] 
then
    usage
else
    dir=$1
    name=$2
    user=$3
    desc="$4"
fi

user_id=$(globus get-identities ${user}) || error_exit "User lookup failed"

[ "${user_id}" == "NO_SUCH_IDENTITY" ] && error_exit "User lookup for $user failed"

share_id=$(globus endpoint create --shared 71dc837e-1561-11e7-bb85-22000b9a448b:$dir \
          --description "$desc" $name | awk '/Endpoint ID/ {print $3}') || error_exit "Create Failed"

access_id=$(globus endpoint permission create --identity $user_id --permissions=r ${share_id}:/) || "Permission Failed"

echo "Success! Share created"
echo "https://app.globus.org/file-manager?origin_id=${share_id}&origin_path=%2F"
