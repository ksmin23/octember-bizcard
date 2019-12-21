import os

from aws_cdk import (
  core,
  aws_ec2
)

class OctemberBizcardStack(core.Stack):

  def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
    super().__init__(scope, id, **kwargs)
    # The code that defines your stack goes here
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

