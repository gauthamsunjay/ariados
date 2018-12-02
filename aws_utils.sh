#! /bin/bash

create_zip() {
    if [[ $# -ne 3 ]]; then
        echo "require <path_to_env> <path_to_aws_lxml> <bucket_name>"
        return 1
    fi

    local path_to_env=$1
    local path_to_lxml=$2
    local bucket=$3
    local site_packages="${path_to_env}/lib/python2.7/site-packages"
    
    rm -rf tempdir
    cp -r ${site_packages} tempdir
    rm -rf tempdir/lxml*

    cp -r ${path_to_lxml} tempdir
    cp -r ariados tempdir

    cd tempdir
    zip -r ariados.zip *
    cd -

    mv tempdir/ariados.zip .
    rm -rf tempdir

    aws --profile ariados s3 cp ariados.zip s3://${bucket}/ariados.zip
}

register_fn() {
    if [[ $# -ne 4 ]]; then
        echo "require <fn_name> <bucket> <handler> <role>"
        return 1
    fi

    local fn_name=$1
    local bucket=$2
    local handler=$3
    local role=$4

    aws --profile ariados lambda create-function --function-name ${fn_name} \
        --code S3Bucket=${bucket},S3Key=ariados.zip                         \
        --handler ${handler} --role ${role} --runtime python2.7
}

