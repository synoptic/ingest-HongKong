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
    aws_sqs as sqs,
    aws_lambda_event_sources as event_sources,
    Duration,
)


class MetaLambdaStack(Stack):

    def __init__(self, scope: Construct, id: str, *, lambda_env: dict,
                 config: dict, ingest_name: str, display_name: str = None, **kwargs) -> None:
        super().__init__(scope, id,
                         description=f"{ingest_name} meta Lambda",
                         **kwargs)
        display_name = display_name or ingest_name

        # ── Networking ─────────────────────────────────────────
        vpc = ec2.Vpc.from_lookup(self, "vpc",
                                  vpc_id=config["vpc_id"],
                                  is_default=False)
        subnet_filter = ec2.SubnetFilter.by_ids(config["subnet_ids"])

        # ── IAM role ───────────────────────────────────────────
        role = iam.Role(self, "MetaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=f"{ingest_name} metadata Lambda role",
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
        fn = _lambda.DockerImageFunction(self, "MetaFn",
            function_name=f"{display_name}MetaLambda",
            memory_size=config["meta_memory_mb"],
            timeout=Duration.minutes(config["meta_timeout_min"]),
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
                cmd=["handlers.meta_handler.lambda_handler"],
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_filters=[subnet_filter]),
            allow_public_subnet=True,
            log_group=logs.LogGroup(self, "MetaLogGroup",
                log_group_name=f"/aws/lambda/{display_name}MetaLambda",
                retention=logs.RetentionDays.ONE_MONTH,
            ),
        )

        # ── Event source ───────────────────────────────────────
        if config["meta_event_source"] == "schedule":
            cron = config["meta_schedule_cron"]
            rule = events.Rule(self, "MetaSchedule",
                schedule=events.Schedule.cron(**cron),
            )
            rule.add_target(targets.LambdaFunction(fn, retry_attempts=0))

        elif config["meta_event_source"] == "queue":
            from aws_cdk import aws_sqs as sqs, aws_lambda_event_sources as event_sources

            queue_url = config["meta_queue_url"]
            queue = sqs.Queue.from_queue_arn(self, "MetaQueue",
                queue_arn=f"arn:aws:sqs:{config['meta_queue_region']}:"
                          f"{queue_url.split('/')[-2]}:{queue_url.split('/')[-1]}"
            )
            fn.add_event_source(event_sources.SqsEventSource(
                queue,
                batch_size=config.get("meta_queue_batch", 1),
            ))
            role.add_to_policy(iam.PolicyStatement(
                actions=["sqs:ReceiveMessage", "sqs:DeleteMessage",
                         "sqs:GetQueueAttributes"],
                resources=[queue.queue_arn],
            ))
