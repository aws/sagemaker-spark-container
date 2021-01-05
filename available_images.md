# Available SageMaker Spark Container Images

The following table lists the ECR repositories that are managed by Amazon SageMaker for the prebuilt Spark containers. These repositories will be automatically used when creating jobs via the SageMaker Python SDK.

To see the list of available image tags for a given Spark container release, check the release documentation in [SageMaker Spark Container Releases](https://github.com/aws/sagemaker-spark-container/releases).

Images are available in the following regions:
|Region |Code |Repository URL |
|--- |--- |--- |
|US East (N. Virginia) |us-east-1 |173754725891.dkr.ecr.us-east-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|US East (Ohio) |us-east-2 |314815235551.dkr.ecr.us-east-2.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|US West (N. California) |us-west-1 |667973535471.dkr.ecr.us-west-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|US West (Oregon) |us-west-2 |153931337802.dkr.ecr.us-west-2.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Asia Pacific (Tokyo) |ap-northeast-1 |411782140378.dkr.ecr.ap-northeast-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Asia Pacific (Seoul) |ap-northeast-2 |860869212795.dkr.ecr.ap-northeast-2.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Asia Pacific (Hong Kong) |ap-east-1 |732049463269.dkr.ecr.ap-east-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Asia Pacific (Mumbai) |ap-south-1 |105495057255.dkr.ecr.ap-south-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Asia Pacific (Singapore) |ap-southeast-1 |759080221371.dkr.ecr.ap-southeast-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Asia Pacific (Sydney) |ap-southeast-2 |440695851116.dkr.ecr.ap-southeast-2.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Africa (Cape Town) |af-south-1 |309385258863.dkr.ecr.af-south-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Middle East (Bahrain) |me-south-1 |750251592176.dkr.ecr.me-south-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|South America (Sao Paulo) |sa-east-1 |737130764395.dkr.ecr.sa-east-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|Canada (Central) |ca-central-1 |446299261295.dkr.ecr.ca-central-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|EU (Frankfurt) |eu-central-1 |906073651304.dkr.ecr.eu-central-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|EU (Stockholm) |eu-north-1 |330188676905.dkr.ecr.eu-north-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|EU (Ireland) |eu-west-1 |571004829621.dkr.ecr.eu-west-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|EU (London) |eu-west-2 |836651553127.dkr.ecr.eu-west-2.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|EU (Milano) |eu-south-1 |753923664805.dkr.ecr.eu-south-1.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|EU (Paris) |eu-west-3 |136845547031.dkr.ecr.eu-west-3.amazonaws.com/&lt;repository-name>:&lt;image-tag> |
|China (Beijing) |cn-north-1 |671472414489.dkr.ecr.cn-north-1.amazonaws.com.cn/&lt;repository-name>:&lt;image-tag> |
|China (Ningxia) |cn-northwest-1 |844356804704.dkr.ecr.cn-northwest-1.amazonaws.com.cn/&lt;repository-name>:&lt;image-tag>|

SageMaker releases Spark containers into multiple repositories and will tag
images based on their version. To pull from our repository, insert the repository
name and version tag into the repository URL. For example:



     173754725891.dkr.ecr.<region>.amazonaws.com/sagemaker-spark-processing:2.4-cpu-py37-v1.0

**Important**

You must login to access to the Spark image repository before pulling
the image. Ensure your CLI is up to date using the steps in [Installing the current AWS CLI Version](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv1.html#install-tool-bundled)
    Then, specify your region and its corresponding ECR Registry from
    the previous table in the following command:



        aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 173754725891.dkr.ecr.us-east-1.amazonaws.com

You can then pull these Docker images from ECR by running:



    docker pull <name of container image>

