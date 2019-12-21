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
  aws_logs
)

from aws_cdk.aws_lambda_event_sources import (
  S3EventSource,
  KinesisEventSource
)

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

