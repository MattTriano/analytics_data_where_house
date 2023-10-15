#!/usr/bin/env bash

set -e

# Always install local overrides first
/app/docker/docker-bootstrap.sh

STEP_CNT=4

echo_step() {
cat <<EOF
######################################################################
Init Step ${1}/${STEP_CNT} [${2}] -- ${3}
######################################################################
EOF
}
# Initialize the database
echo_step "1" "Starting" "Applying DB migrations"
superset db upgrade
echo_step "1" "Complete" "Applying DB migrations"

# Create an admin user
echo_step "2" "Starting" "Setting up admin user ( admin / $ADMIN_USERNAME )"
superset fab create-admin \
              --username $ADMIN_USERNAME \
              --firstname $ADMIN_FIRST_NAME \
              --lastname $ADMIN_LAST_NAME \
              --email $ADMIN_EMAIL \
              --password $ADMIN_PASSWORD
echo_step "2" "Complete" "Setting up admin user"
# Create default roles and permissions
echo_step "3" "Starting" "Setting up roles and perms"
superset init
echo_step "3" "Complete" "Setting up roles and perms"

if [ "$SUPERSET_LOAD_EXAMPLES" = "true" ]; then
    # Load some data to play with
    echo_step "4" "Starting" "Loading examples"
    superset load_examples
    echo_step "4" "Complete" "Loading examples"
fi
