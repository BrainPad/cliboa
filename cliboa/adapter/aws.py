# Copyright BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
import boto3
from boto3.session import Session

from cliboa.util.exception import InvalidParameter


class S3Adapter(object):
    """
    Adapter of AWS S3
    """

    def __init__(
        self,
        access_key: str = None,
        secret_key: str = None,
        profile: str = None,
        role_arn: str = None,
        external_id: str = None,
    ):

        if (access_key and secret_key) and profile:
            raise InvalidParameter(
                "Either access_key and secret_key or profile path can be specified."
            )

        if role_arn and (access_key or secret_key or profile):
            raise InvalidParameter(
                "role_arn cannot be specified with access_key/secret_key or profile."
            )

        self._access_key = access_key
        self._secret_key = secret_key
        self._profile = profile
        self._role_arn = role_arn
        self._external_id = external_id

    def get_client(self):
        """
        Get s3 client
        """
        if self._profile:
            return Session(profile_name=self._profile).client("s3")
        elif self._access_key and self._secret_key:
            return boto3.client(
                "s3",
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            )
        elif self._role_arn:
            return self._get_cross_account_client()
        else:
            return boto3.client("s3")

    def get_resource(self):
        """
        Get s3 resource
        """
        if self._profile:
            return Session(profile_name=self._profile).resource("s3")
        elif self._access_key and self._secret_key:
            session = Session(
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            )
            return session.resource("s3")
        elif self._role_arn:
            return self._get_cross_account_resource()
        else:
            return boto3.resource("s3")

    def _get_cross_account_client(self):
        """
        Get S3 client using cross-account IAM role
        """
        sts_client = boto3.client("sts")

        assume_role_kwargs = {"RoleArn": self._role_arn, "RoleSessionName": "cliboa-session"}

        if self._external_id:
            assume_role_kwargs["ExternalId"] = self._external_id

        try:
            response = sts_client.assume_role(**assume_role_kwargs)
        except Exception as e:
            raise InvalidParameter(f"Failed to assume role {self._role_arn}: {str(e)}")

        credentials = response["Credentials"]

        return boto3.client(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

    def _get_cross_account_resource(self):
        """
        Get S3 resource using cross-account IAM role
        """
        sts_client = boto3.client("sts")

        assume_role_kwargs = {"RoleArn": self._role_arn, "RoleSessionName": "cliboa-session"}

        if self._external_id:
            assume_role_kwargs["ExternalId"] = self._external_id

        try:
            response = sts_client.assume_role(**assume_role_kwargs)
        except Exception as e:
            raise InvalidParameter(f"Failed to assume role {self._role_arn}: {str(e)}")

        credentials = response["Credentials"]

        session = Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        return session.resource("s3")
