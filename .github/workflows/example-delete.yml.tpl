name: 'Delete AWS Resources and Self-Destruct Github Runner'

env:
  PROJECT_NAME: example-project
  MODULE_NAME: example1-example2
  PROJECT_AWS_REGION: project-aws-region

on: [workflow_dispatch]

jobs:
  base:
    name: 'Delete S3 Bucket'
    runs-on: self-hosted
    environment: production
    container:
      image: ghcr.io/kelseymok/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}
    steps:
      - name: Checkout
        uses: actions/checkout@v2

      - name: Assume Role
        run: assume-role ${PROJECT_NAME}-${MODULE_NAME}-github-runner-aws

      - name: Delete S3 Bucket
        run: |
          aws s3 rm "s3://${PROJECT_NAME}-${MODULE_NAME}" --recursive
          stack_name="${PROJECT_NAME}-${MODULE_NAME}-co2-tmp-s3-bucket"
          if [[ $(AWS_PROFILE="${aws_profile}"  aws cloudformation describe-stacks --stack-name "${stack_name}" --region "${PROJECT_AWS_REGION}") ]]; then
            echo "Stack (${stack_name}) exists. Deleting..."
            aws cloudformation delete-stack --stack-name "${stack_name}" \
              --stack-name "${stack_name}" \
              --region "${PROJECT_AWS_REGION}"
          else
            echo "Stack (${stack_name}) does not exist. Nothing to do here!"
          fi

          until $(AWS_PROFILE="${aws_profile}" aws cloudformation describe-stacks --stack-name "${stack_name}" --region "${PROJECT_AWS_REGION}" 2>&1 | grep -q "${stack_name} does not exist")
          do
            echo "Deleting..."
            sleep 10
          done
    github-runner:
      name: 'Delete Github Runner'
      runs-on: self-hosted
      environment: production
      needs: ["base"]
      container:
        image: ghcr.io/kelseymok/terraform-workspace:latest
        credentials:
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      steps:
        - name: Checkout
          uses: actions/checkout@v2

        - name: Assume Role
          run: assume-role ${PROJECT_NAME}-${MODULE_NAME}-github-runner-aws

        - name: Delete Github Runner
          run: |
            if $(ls github-runner-aws-cloudformation); then
                ./github-runner-aws-cloudformation/delete-stack.sh -p "${PROJECT_NAME}" "${MODULE_NAME}" "${PROJECT_AWS_REGION}"
            fi
