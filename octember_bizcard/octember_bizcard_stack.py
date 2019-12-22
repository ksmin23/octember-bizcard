# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import os

from aws_cdk import (
  core,
  aws_ec2,
  aws_apigateway as apigw,
  aws_iam,
  aws_s3 as s3,
  aws_lambda as _lambda,
  aws_kinesis as kinesis,
  aws_dynamodb as dynamodb,
  aws_logs,
  aws_elasticsearch,
  aws_kinesisfirehose,
  aws_elasticache,
  aws_neptune
)

from aws_cdk.aws_lambda_event_sources import (
  S3EventSource,
  KinesisEventSource
)

S3_BUCKET_LAMBDA_LAYER_LIB = os.getenv('S3_BUCKET_LAMBDA_LAYER_LIB', 'octember-resources')

class OctemberBizcardStack(core.Stack):

  def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
    super().__init__(scope, id, **kwargs)

    vpc = aws_ec2.Vpc(self, "OctemberVPC",
      max_azs=2,
#      subnet_configuration=[{
#          "cidrMask": 24,
#          "name": "Public",
#          "subnetType": aws_ec2.SubnetType.PUBLIC,
#        },
#        {
#          "cidrMask": 24,
#          "name": "Private",
#          "subnetType": aws_ec2.SubnetType.PRIVATE
#        },
#        {
#          "cidrMask": 28,
#          "name": "Isolated",
#          "subnetType": aws_ec2.SubnetType.ISOLATED,
#          "reserved": True
#        }
#      ],
      gateway_endpoints={
        "S3": aws_ec2.GatewayVpcEndpointOptions(
          service=aws_ec2.GatewayVpcEndpointAwsService.S3
        )
      }
    )

    dynamo_db_endpoint = vpc.add_gateway_endpoint("DynamoDbEndpoint",
      service=aws_ec2.GatewayVpcEndpointAwsService.DYNAMODB
    )

    s3_bucket = s3.Bucket(self, "s3bucket",
      bucket_name="octember-bizcard-{region}-{account}".format(region=kwargs['env'].region, account=kwargs['env'].account))

    api = apigw.RestApi(self, "BizcardImageUploader",
      rest_api_name="BizcardImageUploader",
      description="This service serves uploading bizcard images into s3.",
      endpoint_types=[apigw.EndpointType.REGIONAL],
      binary_media_types=["image/png", "image/jpg"],
      deploy=True,
      deploy_options=apigw.StageOptions(stage_name="v1")
    )

    rest_api_role = aws_iam.Role(self, "ApiGatewayRoleForS3",
      role_name="ApiGatewayRoleForS3FullAccess",
      assumed_by=aws_iam.ServicePrincipal("apigateway.amazonaws.com"),
      managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")]
    )

    list_objects_responses = [apigw.IntegrationResponse(status_code="200",
        #XXX: https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_apigateway/IntegrationResponse.html#aws_cdk.aws_apigateway.IntegrationResponse.response_parameters
        # The response parameters from the backend response that API Gateway sends to the method response.
        # Use the destination as the key and the source as the value:
        #  - The destination must be an existing response parameter in the MethodResponse property.
        #  - The source must be an existing method request parameter or a static value.
        response_parameters={
          'method.response.header.Timestamp': 'integration.response.header.Date',
          'method.response.header.Content-Length': 'integration.response.header.Content-Length',
          'method.response.header.Content-Type': 'integration.response.header.Content-Type'
        }
      ),
      apigw.IntegrationResponse(status_code="400", selection_pattern="4\d{2}"),
      apigw.IntegrationResponse(status_code="500", selection_pattern="5\d{2}")
    ]

    list_objects_integration_options = apigw.IntegrationOptions(
      credentials_role=rest_api_role,
      integration_responses=list_objects_responses
    )

    get_s3_integration = apigw.AwsIntegration(service="s3",
      integration_http_method="GET",
      path='/',
      options=list_objects_integration_options
    )

    api.root.add_method("GET", get_s3_integration,
      authorization_type=apigw.AuthorizationType.IAM,
      api_key_required=False,
      method_responses=[apigw.MethodResponse(status_code="200",
          response_parameters={
            'method.response.header.Timestamp': False,
            'method.response.header.Content-Length': False,
            'method.response.header.Content-Type': False
          },
          response_models={
            'application/json': apigw.EmptyModel()
          }
        ),
        apigw.MethodResponse(status_code="400"),
        apigw.MethodResponse(status_code="500")
      ],
      request_parameters={
        'method.request.header.Content-Type': False
      }
    )

    get_s3_folder_integration_options = apigw.IntegrationOptions(
      credentials_role=rest_api_role,
      integration_responses=list_objects_responses,
      #XXX: https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_apigateway/IntegrationOptions.html#aws_cdk.aws_apigateway.IntegrationOptions.request_parameters
      # Specify request parameters as key-value pairs (string-to-string mappings), with a destination as the key and a source as the value.
      # The source must be an existing method request parameter or a static value.
      request_parameters={"integration.request.path.bucket": "method.request.path.folder"}
    )

    get_s3_folder_integration = apigw.AwsIntegration(service="s3",
      integration_http_method="GET",
      path="{bucket}",
      options=get_s3_folder_integration_options
    )

    s3_folder = api.root.add_resource('{folder}')
    s3_folder.add_method("GET", get_s3_folder_integration,
      authorization_type=apigw.AuthorizationType.IAM,
      api_key_required=False,
      method_responses=[apigw.MethodResponse(status_code="200",
          response_parameters={
            'method.response.header.Timestamp': False,
            'method.response.header.Content-Length': False,
            'method.response.header.Content-Type': False
          },
          response_models={
            'application/json': apigw.EmptyModel()
          }
        ),
        apigw.MethodResponse(status_code="400"),
        apigw.MethodResponse(status_code="500")
      ],
      request_parameters={
        'method.request.header.Content-Type': False,
        'method.request.path.folder': True
      }
    )

    get_s3_item_integration_options = apigw.IntegrationOptions(
      credentials_role=rest_api_role,
      integration_responses=list_objects_responses,
      request_parameters={
        "integration.request.path.bucket": "method.request.path.folder",
        "integration.request.path.object": "method.request.path.item"
      }
    )

    get_s3_item_integration = apigw.AwsIntegration(service="s3",
      integration_http_method="GET",
      path="{bucket}/{object}",
      options=get_s3_item_integration_options
    )

    s3_item = s3_folder.add_resource('{item}')
    s3_item.add_method("GET", get_s3_item_integration,
      authorization_type=apigw.AuthorizationType.IAM,
      api_key_required=False,
      method_responses=[apigw.MethodResponse(status_code="200",
          response_parameters={
            'method.response.header.Timestamp': False,
            'method.response.header.Content-Length': False,
            'method.response.header.Content-Type': False
          },
          response_models={
            'application/json': apigw.EmptyModel()
          }
        ),
        apigw.MethodResponse(status_code="400"),
        apigw.MethodResponse(status_code="500")
      ],
      request_parameters={
        'method.request.header.Content-Type': False,
        'method.request.path.folder': True,
        'method.request.path.item': True
      }
    )

    put_s3_item_integration_options = apigw.IntegrationOptions(
      credentials_role=rest_api_role,
      integration_responses=[apigw.IntegrationResponse(status_code="200"),
        apigw.IntegrationResponse(status_code="400", selection_pattern="4\d{2}"),
        apigw.IntegrationResponse(status_code="500", selection_pattern="5\d{2}")
      ],
      request_parameters={
        "integration.request.header.Content-Type": "method.request.header.Content-Type",
        "integration.request.path.bucket": "method.request.path.folder",
        "integration.request.path.object": "method.request.path.item"
      }
    )

    put_s3_item_integration = apigw.AwsIntegration(service="s3",
      integration_http_method="PUT",
      path="{bucket}/{object}",
      options=put_s3_item_integration_options
    )

    s3_item.add_method("PUT", put_s3_item_integration,
      authorization_type=apigw.AuthorizationType.IAM,
      api_key_required=False,
      method_responses=[apigw.MethodResponse(status_code="200",
          response_parameters={
            'method.response.header.Content-Type': False
          },
          response_models={
            'application/json': apigw.EmptyModel()
          }
        ),
        apigw.MethodResponse(status_code="400"),
        apigw.MethodResponse(status_code="500")
      ],
      request_parameters={
        'method.request.header.Content-Type': False,
        'method.request.path.folder': True,
        'method.request.path.item': True
      }
    )

    ddb_table = dynamodb.Table(self, "BizcardImageMetaInfoDdbTable",
      table_name="OctemberBizcardImgMeta",
      partition_key=dynamodb.Attribute(name="image_id", type=dynamodb.AttributeType.STRING),
      billing_mode=dynamodb.BillingMode.PROVISIONED,
      read_capacity=15,
      write_capacity=5
    )

    img_kinesis_stream = kinesis.Stream(self, "BizcardImagePath", stream_name="octember-bizcard-image")

    # create lambda function
    trigger_textract_lambda_fn = _lambda.Function(self, "TriggerTextExtractorFromImage",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="TriggerTextExtractorFromImage",
      handler="trigger_text_extract_from_s3_image.lambda_handler",
      description="Trigger to extract text from an image in S3",
      code=_lambda.Code.asset("./src/main/python/TriggerTextExtractFromS3Image"),
      environment={
        'REGION_NAME': kwargs['env'].region,
        'DDB_TABLE_NAME': ddb_table.table_name,
        'KINESIS_STREAM_NAME': img_kinesis_stream.stream_name
      },
      timeout=core.Duration.minutes(5)
    )

    ddb_table_rw_policy_statement = aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[ddb_table.table_arn],
      actions=[
        "dynamodb:BatchGetItem",
        "dynamodb:Describe*",
        "dynamodb:List*",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchWriteItem",
        "dynamodb:DeleteItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dax:Describe*",
        "dax:List*",
        "dax:GetItem",
        "dax:BatchGetItem",
        "dax:Query",
        "dax:Scan",
        "dax:BatchWriteItem",
        "dax:DeleteItem",
        "dax:PutItem",
        "dax:UpdateItem"
      ]
    )

    trigger_textract_lambda_fn.add_to_role_policy(ddb_table_rw_policy_statement)
    trigger_textract_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[img_kinesis_stream.stream_arn],
      actions=["kinesis:Get*",
        "kinesis:List*",
        "kinesis:Describe*",
        "kinesis:PutRecord",
        "kinesis:PutRecords"
      ]
    ))

    # assign notification for the s3 event type (ex: OBJECT_CREATED)
    s3_event_filter = s3.NotificationKeyFilter(prefix="bizcard-raw-img/", suffix=".jpg")
    s3_event_source = S3EventSource(s3_bucket, events=[s3.EventType.OBJECT_CREATED], filters=[s3_event_filter])
    trigger_textract_lambda_fn.add_event_source(s3_event_source)

    #XXX: https://github.com/aws/aws-cdk/issues/2240
    # To avoid to create extra Lambda Functions with names like LogRetentionaae0aa3c5b4d4f87b02d85b201efdd8a
    # if log_retention=aws_logs.RetentionDays.THREE_DAYS is added to the constructor props
    log_group = aws_logs.LogGroup(self, "TriggerTextractLogGroup",
      log_group_name="/aws/lambda/TriggerTextExtractorFromImage",
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(trigger_textract_lambda_fn)

    text_kinesis_stream = kinesis.Stream(self, "BizcardTextData", stream_name="octember-bizcard-txt")

    textract_lambda_fn = _lambda.Function(self, "GetTextFromImage",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="GetTextFromImage",
      handler="get_text_from_s3_image.lambda_handler",
      description="extract text from an image in S3",
      code=_lambda.Code.asset("./src/main/python/GetTextFromS3Image"),
      environment={
        'REGION_NAME': kwargs['env'].region,
        'DDB_TABLE_NAME': ddb_table.table_name,
        'KINESIS_STREAM_NAME': text_kinesis_stream.stream_name
      },
      timeout=core.Duration.minutes(5)
    )

    textract_lambda_fn.add_to_role_policy(ddb_table_rw_policy_statement)
    textract_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[text_kinesis_stream.stream_arn],
      actions=["kinesis:Get*",
        "kinesis:List*",
        "kinesis:Describe*",
        "kinesis:PutRecord",
        "kinesis:PutRecords"
      ]
    ))

    textract_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(**{
      "effect": aws_iam.Effect.ALLOW,
      "resources": [s3_bucket.bucket_arn, "{}/*".format(s3_bucket.bucket_arn)],
      "actions": ["s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject"]
    }))

    textract_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["textract:*"]))

    img_kinesis_event_source = KinesisEventSource(img_kinesis_stream, batch_size=100, starting_position=_lambda.StartingPosition.LATEST)
    textract_lambda_fn.add_event_source(img_kinesis_event_source)

    log_group = aws_logs.LogGroup(self, "GetTextFromImageLogGroup",
      log_group_name="/aws/lambda/GetTextFromImage",
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(textract_lambda_fn)

    sg_use_bizcard_es = aws_ec2.SecurityGroup(self, "BizcardSearchClientSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard elasticsearch client',
      security_group_name='use-octember-bizcard-es'
    )
    core.Tag.add(sg_use_bizcard_es, 'Name', 'use-octember-bizcard-es')

    sg_bizcard_es = aws_ec2.SecurityGroup(self, "BizcardSearchSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard elasticsearch',
      security_group_name='octember-bizcard-es'
    )
    core.Tag.add(sg_bizcard_es, 'Name', 'octember-bizcard-es')

    sg_bizcard_es.add_ingress_rule(peer=sg_bizcard_es, connection=aws_ec2.Port.all_tcp(), description='octember-bizcard-es')
    sg_bizcard_es.add_ingress_rule(peer=sg_use_bizcard_es, connection=aws_ec2.Port.all_tcp(), description='use-octember-bizcard-es')

    #XXX: aws cdk elastsearch example - https://github.com/aws/aws-cdk/issues/2873
    es_cfn_domain = aws_elasticsearch.CfnDomain(self, 'BizcardSearch',
      elasticsearch_cluster_config={
        "dedicatedMasterCount": 3,
        "dedicatedMasterEnabled": True,
        "dedicatedMasterType": "t2.medium.elasticsearch",
        "instanceCount": 2,
        "instanceType": "t2.medium.elasticsearch",
        "zoneAwarenessEnabled": True
      },
      ebs_options={
        "ebsEnabled": True,
        "volumeSize": 10,
        "volumeType": "gp2"
      },
      domain_name="octember-bizcard",
      elasticsearch_version="7.1",
      encryption_at_rest_options={
        "enabled": False
      },
      access_policies={
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
              "AWS": "*"
            },
            "Action": [
              "es:Describe*",
              "es:List*",
              "es:Get*",
              "es:ESHttp*"
            ],
            "Resource": self.format_arn(service="es", resource="domain", resource_name="octember-bizcard/*")
          }
        ]
      },
      snapshot_options={
        "automatedSnapshotStartHour": 17
      },
      vpc_options={
        "securityGroupIds": [sg_bizcard_es.security_group_id],
        "subnetIds": vpc.select_subnets(subnet_type=aws_ec2.SubnetType.PRIVATE).subnet_ids
      }
    )
    core.Tag.add(es_cfn_domain, 'Name', 'octember-bizcard-es')

    #XXX: https://github.com/aws/aws-cdk/issues/1342
    s3_lib_bucket = s3.Bucket.from_bucket_name(self, id, S3_BUCKET_LAMBDA_LAYER_LIB)
    es_lib_layer = _lambda.LayerVersion(self, "ESLib",
      layer_version_name="es-lib",
      compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
      code=_lambda.Code.from_bucket(s3_lib_bucket, "var/octember-es-lib.zip")
    )

    redis_lib_layer = _lambda.LayerVersion(self, "RedisLib",
      layer_version_name="redis-lib",
      compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
      code=_lambda.Code.from_bucket(s3_lib_bucket, "var/octember-redis-lib.zip")
    )

    #XXX: Deploy lambda in VPC - https://github.com/aws/aws-cdk/issues/1342
    upsert_to_es_lambda_fn = _lambda.Function(self, "UpsertBizcardToES",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="UpsertBizcardToElasticSearch",
      handler="upsert_bizcard_to_es.lambda_handler",
      description="Upsert bizcard text into elasticsearch",
      code=_lambda.Code.asset("./src/main/python/UpsertBizcardToES"),
      environment={
        'ES_HOST': es_cfn_domain.attr_domain_endpoint,
        'ES_INDEX': 'octember_bizcard',
        'ES_TYPE': 'bizcard'
      },
      timeout=core.Duration.minutes(5),
      layers=[es_lib_layer],
      security_groups=[sg_use_bizcard_es],
      vpc=vpc
    )

    text_kinesis_event_source = KinesisEventSource(text_kinesis_stream, batch_size=99, starting_position=_lambda.StartingPosition.LATEST)
    upsert_to_es_lambda_fn.add_event_source(text_kinesis_event_source)

    log_group = aws_logs.LogGroup(self, "UpsertBizcardToESLogGroup",
      log_group_name="/aws/lambda/UpsertBizcardToElasticSearch",
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(upsert_to_es_lambda_fn)

    firehose_role_policy_doc = aws_iam.PolicyDocument()
    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(**{
      "effect": aws_iam.Effect.ALLOW,
      "resources": [s3_bucket.bucket_arn, "{}/*".format(s3_bucket.bucket_arn)],
      "actions": ["s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject"]
    }))

    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["glue:GetTable",
        "glue:GetTableVersion",
        "glue:GetTableVersions"]
    ))

    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[text_kinesis_stream.stream_arn],
      actions=["kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"]
    ))

    firehose_log_group_name = "/aws/kinesisfirehose/octember-bizcard-txt-to-s3"
    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      #XXX: The ARN will be formatted as follows:
      # arn:{partition}:{service}:{region}:{account}:{resource}{sep}}{resource-name}
      resources=[self.format_arn(service="logs", resource="log-group",
        resource_name="{}:log-stream:*".format(firehose_log_group_name), sep=":")],
      actions=["logs:PutLogEvents"]
    ))

    firehose_role = aws_iam.Role(self, "FirehoseDeliveryRole",
      role_name="FirehoseDeliveryRole",
      assumed_by=aws_iam.ServicePrincipal("firehose.amazonaws.com"),
      #XXX: use inline_policies to work around https://github.com/aws/aws-cdk/issues/5221
      inline_policies={
        "firehose_role_policy": firehose_role_policy_doc
      }
    )

    bizcard_text_to_s3_delivery_stream = aws_kinesisfirehose.CfnDeliveryStream(self, "BizcardTextToS3",
      delivery_stream_name="octember-bizcard-txt-to-s3",
      delivery_stream_type="KinesisStreamAsSource",
      kinesis_stream_source_configuration={
        "kinesisStreamArn": text_kinesis_stream.stream_arn,
        "roleArn": firehose_role.role_arn
      },
      extended_s3_destination_configuration={
        "bucketArn": s3_bucket.bucket_arn,
        "bufferingHints": {
          "intervalInSeconds": 60,
          "sizeInMBs": 1
        },
        "cloudWatchLoggingOptions": {
          "enabled": True,
          "logGroupName": firehose_log_group_name,
          "logStreamName": "S3Delivery"
        },
        "compressionFormat": "GZIP",
        "prefix": "bizcard-text/",
        "roleArn": firehose_role.role_arn
      }
    )

    sg_use_bizcard_es_cache = aws_ec2.SecurityGroup(self, "BizcardSearchCacheClientSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard search query cache client',
      security_group_name='use-octember-bizcard-es-cache'
    )
    core.Tag.add(sg_use_bizcard_es_cache, 'Name', 'use-octember-bizcard-es-cache')

    sg_bizcard_es_cache = aws_ec2.SecurityGroup(self, "BizcardSearchCacheSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard search query cache',
      security_group_name='octember-bizcard-es-cache'
    )
    core.Tag.add(sg_bizcard_es_cache, 'Name', 'octember-bizcard-es-cache')

    sg_bizcard_es_cache.add_ingress_rule(peer=sg_use_bizcard_es_cache, connection=aws_ec2.Port.tcp(6379), description='use-octember-bizcard-es-cache')

    es_query_cache_subnet_group = aws_elasticache.CfnSubnetGroup(self, "QueryCacheSubnetGroup",
      description="subnet group for octember-bizcard-es-cache",
      subnet_ids=vpc.select_subnets(subnet_type=aws_ec2.SubnetType.PRIVATE).subnet_ids,
      cache_subnet_group_name='octember-bizcard-es-cache'
    )

    es_query_cache = aws_elasticache.CfnCacheCluster(self, "BizcardSearchQueryCache",
      cache_node_type="cache.t3.small",
      num_cache_nodes=1,
      engine="redis",
      engine_version="5.0.5",
      auto_minor_version_upgrade=False,
      cluster_name="octember-bizcard-es-cache",
      snapshot_retention_limit=3,
      snapshot_window="17:00-19:00",
      preferred_maintenance_window="mon:19:00-mon:20:30",
      #XXX: Do not use referece for "cache_subnet_group_name" - https://github.com/aws/aws-cdk/issues/3098
      #cache_subnet_group_name=es_query_cache_subnet_group.cache_subnet_group_name, # Redis cluster goes to wrong VPC
      cache_subnet_group_name='octember-bizcard-es-cache',
      vpc_security_group_ids=[sg_bizcard_es_cache.security_group_id]
    )

    #XXX: If you're going to launch your cluster in an Amazon VPC, you need to create a subnet group before you start creating a cluster.
    # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-elasticache-cache-cluster.html#cfn-elasticache-cachecluster-cachesubnetgroupname
    es_query_cache.add_depends_on(es_query_cache_subnet_group)

    #XXX: add more than 2 security groups
    # https://github.com/aws/aws-cdk/blob/ea10f0d141a48819ec0000cd7905feda993870a9/packages/%40aws-cdk/aws-lambda/lib/function.ts#L387
    # https://github.com/aws/aws-cdk/issues/1555
    # https://github.com/aws/aws-cdk/pull/5049
    bizcard_search_lambda_fn = _lambda.Function(self, "BizcardSearchServer",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="BizcardSearchProxy",
      handler="es_search_bizcard.lambda_handler",
      description="Proxy server to search bizcard text",
      code=_lambda.Code.asset("./src/main/python/SearchBizcard"),
      environment={
        'ES_HOST': es_cfn_domain.attr_domain_endpoint,
        'ES_INDEX': 'octember_bizcard',
        'ES_TYPE': 'bizcard',
        'ELASTICACHE_HOST': es_query_cache.attr_redis_endpoint_address
      },
      timeout=core.Duration.minutes(1),
      layers=[es_lib_layer, redis_lib_layer],
      security_groups=[sg_use_bizcard_es, sg_use_bizcard_es_cache],
      vpc=vpc
    )

    #XXX: create API Gateway + LambdaProxy
    search_api = apigw.LambdaRestApi(self, "BizcardSearchAPI",
      handler=bizcard_search_lambda_fn,
      proxy=False,
      rest_api_name="BizcardSearch",
      description="This service serves searching bizcard text.",
      endpoint_types=[apigw.EndpointType.REGIONAL],
      deploy=True,
      deploy_options=apigw.StageOptions(stage_name="v1")
    )

    bizcard_search = search_api.root.add_resource('search')
    bizcard_search.add_method("GET",
      method_responses=[apigw.MethodResponse(status_code="200",
          response_models={
            'application/json': apigw.EmptyModel()
          }
        ),
        apigw.MethodResponse(status_code="400"),
        apigw.MethodResponse(status_code="500")
      ]
    )

    sg_use_bizcard_graph_db = aws_ec2.SecurityGroup(self, "BizcardGraphDbClientSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard graph db client',
      security_group_name='use-octember-bizcard-neptune'
    )
    core.Tag.add(sg_use_bizcard_graph_db, 'Name', 'use-octember-bizcard-neptune')

    sg_bizcard_graph_db = aws_ec2.SecurityGroup(self, "BizcardGraphDbSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard graph db',
      security_group_name='octember-bizcard-neptune'
    )
    core.Tag.add(sg_bizcard_graph_db, 'Name', 'octember-bizcard-neptune')

    sg_bizcard_graph_db.add_ingress_rule(peer=sg_bizcard_graph_db, connection=aws_ec2.Port.tcp(8182), description='octember-bizcard-neptune')
    sg_bizcard_graph_db.add_ingress_rule(peer=sg_use_bizcard_graph_db, connection=aws_ec2.Port.tcp(8182), description='use-octember-bizcard-neptune')

    bizcard_graph_db_subnet_group = aws_neptune.CfnDBSubnetGroup(self, "NeptuneSubnetGroup",
      db_subnet_group_description="subnet group for octember-bizcard-neptune",
      subnet_ids=vpc.select_subnets(subnet_type=aws_ec2.SubnetType.PRIVATE).subnet_ids,
      db_subnet_group_name='octember-bizcard-neptune'
    )

    bizcard_graph_db = aws_neptune.CfnDBCluster(self, "BizcardGraphDB",
      availability_zones=vpc.availability_zones,
      db_subnet_group_name=bizcard_graph_db_subnet_group.db_subnet_group_name,
      db_cluster_identifier="octember-bizcard",
      backup_retention_period=1,
      preferred_backup_window="08:45-09:15",
      preferred_maintenance_window="sun:18:00-sun:18:30",
      vpc_security_group_ids=[sg_bizcard_graph_db.security_group_id]
    )
    bizcard_graph_db.add_depends_on(bizcard_graph_db_subnet_group)

    bizcard_graph_db_instance = aws_neptune.CfnDBInstance(self, "BizcardGraphDBInstance",
      db_instance_class="db.r5.large",
      allow_major_version_upgrade=False,
      auto_minor_version_upgrade=False,
      availability_zone=vpc.availability_zones[0],
      db_cluster_identifier=bizcard_graph_db.db_cluster_identifier,
      db_instance_identifier="octember-bizcard",
      preferred_maintenance_window="sun:18:00-sun:18:30"
    )
    bizcard_graph_db_instance.add_depends_on(bizcard_graph_db)

    bizcard_graph_db_replica_instance = aws_neptune.CfnDBInstance(self, "BizcardGraphDBReplicaInstance",
      db_instance_class="db.r5.large",
      allow_major_version_upgrade=False,
      auto_minor_version_upgrade=False,
      availability_zone=vpc.availability_zones[-1],
      db_cluster_identifier=bizcard_graph_db.db_cluster_identifier,
      db_instance_identifier="octember-bizcard-replica",
      preferred_maintenance_window="sun:18:00-sun:18:30"
    )
    bizcard_graph_db_replica_instance.add_depends_on(bizcard_graph_db)
    bizcard_graph_db_replica_instance.add_depends_on(bizcard_graph_db_instance)

    gremlinpython_lib_layer = _lambda.LayerVersion(self, "GremlinPythonLib",
      layer_version_name="gremlinpython-lib",
      compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
      code=_lambda.Code.from_bucket(s3_lib_bucket, "var/octember-gremlinpython-lib.zip")
    )

    #XXX: https://github.com/aws/aws-cdk/issues/1342
    upsert_to_neptune_lambda_fn = _lambda.Function(self, "UpsertBizcardToGraphDB",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="UpsertBizcardToNeptune",
      handler="upsert_bizcard_to_graph_db.lambda_handler",
      description="Upsert bizcard into neptune",
      code=_lambda.Code.asset("./src/main/python/UpsertBizcardToGraphDB"),
      environment={
        'REGION_NAME': kwargs['env'].region,
        'NEPTUNE_ENDPOINT': bizcard_graph_db.attr_endpoint,
        'NEPTUNE_PORT': bizcard_graph_db.attr_port
      },
      timeout=core.Duration.minutes(5),
      layers=[gremlinpython_lib_layer],
      security_groups=[sg_use_bizcard_graph_db],
      vpc=vpc
    )

    upsert_to_neptune_lambda_fn.add_event_source(text_kinesis_event_source)

    log_group = aws_logs.LogGroup(self, "UpsertBizcardToGraphDBLogGroup",
      log_group_name="/aws/lambda/UpsertBizcardToNeptune",
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(upsert_to_neptune_lambda_fn)

    sg_use_bizcard_neptune_cache = aws_ec2.SecurityGroup(self, "BizcardNeptuneCacheClientSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard recommendation query cache client',
      security_group_name='use-octember-bizcard-neptune-cache'
    )
    core.Tag.add(sg_use_bizcard_neptune_cache, 'Name', 'use-octember-bizcard-es-cache')

    sg_bizcard_neptune_cache = aws_ec2.SecurityGroup(self, "BizcardNeptuneCacheSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for octember bizcard recommendation query cache',
      security_group_name='octember-bizcard-neptune-cache'
    )
    core.Tag.add(sg_bizcard_neptune_cache, 'Name', 'octember-bizcard-neptune-cache')

    sg_bizcard_neptune_cache.add_ingress_rule(peer=sg_use_bizcard_neptune_cache, connection=aws_ec2.Port.tcp(6379), description='use-octember-bizcard-neptune-cache')

    recomm_query_cache_subnet_group = aws_elasticache.CfnSubnetGroup(self, "RecommQueryCacheSubnetGroup",
      description="subnet group for octember-bizcard-neptune-cache",
      subnet_ids=vpc.select_subnets(subnet_type=aws_ec2.SubnetType.PRIVATE).subnet_ids,
      cache_subnet_group_name='octember-bizcard-neptune-cache'
    )

    recomm_query_cache = aws_elasticache.CfnCacheCluster(self, "BizcardRecommQueryCache",
      cache_node_type="cache.t3.small",
      num_cache_nodes=1,
      engine="redis",
      engine_version="5.0.5",
      auto_minor_version_upgrade=False,
      cluster_name="octember-bizcard-neptune-cache",
      snapshot_retention_limit=3,
      snapshot_window="17:00-19:00",
      preferred_maintenance_window="mon:19:00-mon:20:30",
      #XXX: Do not use referece for "cache_subnet_group_name" - https://github.com/aws/aws-cdk/issues/3098
      #cache_subnet_group_name=recomm_query_cache_subnet_group.cache_subnet_group_name, # Redis cluster goes to wrong VPC
      cache_subnet_group_name='octember-bizcard-neptune-cache',
      vpc_security_group_ids=[sg_bizcard_neptune_cache.security_group_id]
    )

    recomm_query_cache.add_depends_on(recomm_query_cache_subnet_group)

    bizcard_recomm_lambda_fn = _lambda.Function(self, "BizcardRecommender",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="BizcardRecommender",
      handler="neptune_recommend_bizcard.lambda_handler",
      description="This service serves PYMK(People You May Know).",
      code=_lambda.Code.asset("./src/main/python/RecommendBizcard"),
      environment={
        'REGION_NAME': kwargs['env'].region,
        'NEPTUNE_ENDPOINT': bizcard_graph_db.attr_read_endpoint,
        'NEPTUNE_PORT': bizcard_graph_db.attr_port,
        'ELASTICACHE_HOST': recomm_query_cache.attr_redis_endpoint_address
      },
      timeout=core.Duration.minutes(1),
      layers=[gremlinpython_lib_layer, redis_lib_layer],
      security_groups=[sg_use_bizcard_graph_db, sg_use_bizcard_neptune_cache],
      vpc=vpc
    )

    #XXX: create API Gateway + LambdaProxy
    recomm_api = apigw.LambdaRestApi(self, "BizcardRecommendAPI",
      handler=bizcard_recomm_lambda_fn,
      proxy=False,
      rest_api_name="BizcardRecommend",
      description="This service serves PYMK(People You May Know).",
      endpoint_types=[apigw.EndpointType.REGIONAL],
      deploy=True,
      deploy_options=apigw.StageOptions(stage_name="v1")
    )

    bizcard_recomm = recomm_api.root.add_resource('pymk')
    bizcard_recomm.add_method("GET",
      method_responses=[apigw.MethodResponse(status_code="200",
          response_models={
            'application/json': apigw.EmptyModel()
          }
        ),
        apigw.MethodResponse(status_code="400"),
        apigw.MethodResponse(status_code="500")
      ]
    )
