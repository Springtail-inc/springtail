Dockerfiles and Tools
====

This directory contains Dockerfiles and supporting assets. We use Gcc-13 as the toolchain.

## Overview

### Dockerfile.gcc.amd64.base
        
Dockerfile to build the base builder image to run on x86_64 architecture. It does not build `springtail` binary, 
instead it installs the necessary tools and libraries to build the `springtail` binary.

### Dockerfile.gcc.arm.base
   
Same as above but for ARM architecture, such as AWS Graviton instances.

### Dockerfile.gcc.test.template

Dockerfile template to build an image to run the test cases.

You need to replace the `{{ BASE_IMAGE }}` with the base image you want to use. 

As of writing, we have the following base image you could use:

```891377357651.dkr.ecr.us-east-1.amazonaws.com/springtail:builder-base-gcc-13-2024-04-16-16-16```

In orde to pull the image, you need to have the AWS CLI installed and configured with the SSO, as 
mentioned in this [AWS CLI with SSO guide](https://www.notion.so/AWS-CLI-with-SSO-96cf87ee2deb44e9ad6164415be9eb91).

Then you can do,

```shell
$ aws --profile <your profile> ecr get-login-password | docker login --username AWS --password-stdin 891377357651.dkr.ecr.us-east-1.amazonaws.com
```
to login to the ECR.
