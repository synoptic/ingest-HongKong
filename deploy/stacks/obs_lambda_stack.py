from constructs import Construct
from aws_cdk.aws_ecr_assets import Platform
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    Duration,
)


class ObsLambdaStack(Stack):

    def __init__(self, scope: Construct, id: str, *, lambda_env: dict,
                 config: dict, ingest_name: str, display_name: str = None, **kwargs) -> None:
        super().__init__(scope, id,
                         description=f"{ingest_name} obs Lambda",
                         **kwargs)
        display_name = display_name or ingest_name

        # ── Networking ─────────────────────────────────────────
        vpc = ec2.Vpc.from_lookup(self, "vpc",
                                  vpc_id=config["vpc_id"],
                                  is_default=False)
        subnet_filter = ec2.SubnetFilter.by_ids(config["subnet_ids"])

        # ── IAM role ───────────────────────────────────────────
        role = iam.Role(self, "ObsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=f"{ingest_name} obs Lambda role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"),
            ],
        )

        role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:HeadObject",
                     "s3:ListBucket", "s3:DeleteObject"],
            resources=config["s3_bucket_arns"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["ec2:CreateNetworkInterface",
                     "ec2:DescribeNetworkInterfaces",
                     "ec2:DeleteNetworkInterface",
                     "ec2:CreateTags"],
            resources=["*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue",
                     "secretsmanager:DescribeSecret"],
            resources=["*"],
        ))

        # ── Lambda ─────────────────────────────────────────────
        fn = _lambda.DockerImageFunction(self, "ObsFn",
            function_name=f"{display_name}ObsLambda",
            memory_size=config["obs_memory_mb"],
            timeout=Duration.minutes(config["obs_timeout_min"]),
            reserved_concurrent_executions=config.get("obs_concurrency"),
            role=role,
            architecture=_lambda.Architecture.ARM_64,
            environment=lambda_env,
            tracing=_lambda.Tracing.ACTIVE,
            code=_lambda.DockerImageCode.from_image_asset(
                directory="../",
                file="deploy/Dockerfile",
                build_ssh="default",
                platform=Platform.LINUX_ARM64,
                exclude=["deploy/cdk.out", "deploy/.venv", "env", ".venv",
                        "dev", ".git", "__pycache__", "tests", "legacy"],
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_filters=[subnet_filter]),
            allow_public_subnet=True,
            log_group=logs.LogGroup(self, "ObsLogGroup",
                log_group_name=f"/aws/lambda/{display_name}ObsLambda",
                retention=logs.RetentionDays.ONE_MONTH,
            ),
        )

        # ── Event source ───────────────────────────────────────
        if config["obs_event_source"] == "schedule":
            rule = events.Rule(self, "ObsSchedule",
                schedule=events.Schedule.rate(
                    Duration.minutes(config["obs_schedule_minutes"])),
            )
            rule.add_target(targets.LambdaFunction(fn, retry_attempts=0))
