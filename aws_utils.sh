#! /bin/bash

ariados_prepare_admin_env() {
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "You need to be inside a virtual environment"
        return 1
    fi

    echo "Your venv is $VIRTUAL_ENV"
    local sourcedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
    local curdir=$(pwd)

    cd ${VIRTUAL_ENV}
    rm -rf ariados-admin-env
    mkdir ariados-admin-env && cd $_
    git clone --depth=1 https://github.com/JFox/aws-lambda-lxml.git
    pip install -r "${sourcedir}/requirements.txt"
    echo "#please paste your configuration details here" > admin.conf
    echo "aws_profile" >> admin.conf
    echo "s3_bucket=" >> admin.conf
    echo "s3_path_to_zipfile=" >> admin.conf
    echo "lambda_role=" >> admin.conf
    echo "lambda_timeout=" >> admin.conf
    cd $curdir
    echo "Please update variables at ${VIRTUAL_ENV}/ariados-admin-env/admin.conf"
    # TODO ensure that cockroachdb server is installed
}

ariados_create_zip() {
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "You need to be inside a virtual environment"
        return 1
    fi

    if [ ! -d "${VIRTUAL_ENV}/ariados-admin-env" ]; then
        echo "prepare_admin_env first"
        return 1
    fi

    local sourcedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
    local curdir=$(pwd)
    local zipfile=${sourcedir}/ariados.zip

    cd ${VIRTUAL_ENV}/lib/python2.7/site-packages

    # get every python library except lxml
    rm -rf /tmp/create_zip_scratch && mkdir -p /tmp/create_zip_scratch
    mv lxml /tmp/create_zip_scratch/
    zip -r9 ${zipfile} .
    mv /tmp/create_zip_scratch/lxml lxml

    # use aws specific lxml
    cd ${VIRTUAL_ENV}/ariados-admin-env/aws-lambda-lxml/3.8.0/py27/
    zip -r9 -g ${zipfile} .

    # add our custom code
    cd ${sourcedir}
    zip -g ${zipfile} ariados/*.py
    zip -g -r9 ${zipfile} ariados/utils/
    zip -g -r9 ${zipfile} ariados/common/
    zip -g -r9 ${zipfile} ariados/handlers/

    cd ${curdir}
}

ariados_upload_zip() {
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "You need to be inside a virtual environment"
        return 1
    fi

    if [ ! -d "${VIRTUAL_ENV}/ariados-admin-env" ]; then
        echo "prepare_admin_env first"
        return 1
    fi

    . ${VIRTUAL_ENV}/ariados-admin-env/admin.conf

    local sourcedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
    local zipfile=${sourcedir}/ariados.zip
    aws --profile ${aws_profile} s3 cp $zipfile "s3://${s3_bucket}/${s3_path_to_zipfile}"
}

ariados_register_lambdas() {
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "You need to be inside a virtual environment"
        return 1
    fi

    if [ ! -d "${VIRTUAL_ENV}/ariados-admin-env" ]; then
        echo "prepare_admin_env first"
        return 1
    fi

    . ${VIRTUAL_ENV}/ariados-admin-env/admin.conf

    local bucket=${s3_bucket}
    local path=${s3_path_to_zipfile}
    local role=${lambda_role}
    local timeout=${lambda_timeout}

    # TODO get role and timeout from environment set up by admin?
    for i in "handle_single_url" "handle_multiple_urls"; do
        aws --profile ${aws_profile} lambda create-function \
            --runtime "python2.7"  \
            --timeout "${timeout}" \
            --role "${role}"       \
            --function-name "${i}" \
            --handler "ariados.lambda_handlers.${i}" \
            --code "S3Bucket=${bucket},S3Key=${path}"
    done
}

ariados_unregister_lambdas() {
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "You need to be inside a virtual environment"
        return 1
    fi

    if [ ! -d "${VIRTUAL_ENV}/ariados-admin-env" ]; then
        echo "prepare_admin_env first"
        return 1
    fi

    . ${VIRTUAL_ENV}/ariados-admin-env/admin.conf

    for i in "handle_single_url" "handle_multiple_urls"; do
        aws --profile ${aws_profile} lambda delete-function \
            --function-name "${i}"
    done
}
