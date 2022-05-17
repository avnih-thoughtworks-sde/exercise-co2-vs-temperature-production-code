name: 'Build, Test, Deploy'

env:
  PROJECT_NAME: example-project
  MODULE_NAME: example1-example2
  PROJECT_AWS_REGION: project-aws-region

on:
  push:
    branches:
      - master

jobs:
  base:
    name: 'Create S3 Bucket'
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
        with:
          submodules: recursive

      - name: Assume Role
        run: assume-role ${PROJECT_NAME}-${MODULE_NAME}-github-runner-aws

      - name: Create S3 Bucket
        run: |
            if [ -d "s3-bucket-aws-cloudformation" ]; then
                ./s3-bucket-aws-cloudformation/create-stack -p "${PROJECT_NAME}" -m "${MODULE_NAME}" -r "${PROJECT_AWS_REGION}"
                sleep 20
            fi

      - name: Upload Data
        run: |
          if [ -d "s3-bucket-aws-cloudformation" ]; then
              aws s3 cp ./datasets/ingestion/inputs/EmissionsByCountry.csv s3://${PROJECT_NAME}-${MODULE_NAME}/data-source/EmissionsByCountry.csv
              aws s3 cp ./datasets/ingestion/inputs/GlobalTemperatures.csv s3://${PROJECT_NAME}-${MODULE_NAME}/data-source/GlobalTemperatures.csv
              aws s3 cp ./datasets/ingestion/inputs/TemperaturesByCountry.csv s3://${PROJECT_NAME}-${MODULE_NAME}/data-source/TemperaturesByCountry.csv
          fi

  data-ingestion-test:
    name: 'Test Data Ingestion'
    runs-on: self-hosted
    environment: production
    container:
      image: ghcr.io/kelseymok/pyspark-testing-env:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2
        with:
          submodules: recursive
      - name: Test Data Ingestion
        env:
          SUBDIR: data-ingestion
        uses: ./.github/composite-actions/pytest

  data-ingestion:
    name: 'Deploy Data Ingestion Artifacts'
    runs-on: self-hosted
    environment: production
    env:
      SUBDIR: data-ingestion
    needs: ["base", "data-ingestion-test"]
    container:
      image: ghcr.io/kelseymok/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2
        with:
          submodules: recursive

      - name: Assume Role
        run: assume-role ${PROJECT_NAME}-${MODULE_NAME}-github-runner-aws

      - name: Upload Main.py
        run: |
          cd ${SUBDIR}/src
          aws s3 cp main.py s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/main.py

      - name: Upload Data Ingestion lib
        run: |
          cd ${SUBDIR}
          python setup.py bdist_egg
          filename=$(ls dist)
          aws s3 cp dist/${filename} s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/data_ingestion-0.1-py3.egg

  data-transformation-test:
    name: 'Test Data Transformation'
    runs-on: self-hosted
    environment: production
    needs: ["data-ingestion"]
    container:
      image: ghcr.io/kelseymok/pyspark-testing-env:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2
        with:
          submodules: recursive

      - name: Test Data Transformation
        env:
          SUBDIR: data-transformation
        uses: ./.github/composite-actions/pytest

  data-transformation:
    name: 'Deploy Data Transformation Artifacts'
    runs-on: self-hosted
    environment: production
    env:
      SUBDIR: data-transformation
    needs: ["base", "data-transformation-test"]
    container:
      image: ghcr.io/kelseymok/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2
        with:
          submodules: recursive

      - name: Assume Role
        run: assume-role ${PROJECT_NAME}-${MODULE_NAME}-github-runner-aws

      - name: Upload Main.py
        run: |
          cd ${SUBDIR}/src
          if ! aws s3 ls "s3://${PROJECT_NAME}-${MODULE_NAME}" 2>&1 | grep -q 'NoSuchBucket'; then
            echo "Uploading s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/main.py"
            aws s3 cp main.py s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/main.py
          else
            aws s3api create-bucket --bucket s3://${PROJECT_NAME}-${MODULE_NAME} --acl private
          fi

      - name: Upload Data Transformation lib
        run: |
          cd ${SUBDIR}
          python setup.py bdist_egg
          filename=$(ls dist)
          aws s3 cp dist/${filename} s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/data_transformation-0.1-py3.egg