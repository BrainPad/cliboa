#
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

from azure.storage.blob import BlobServiceClient


class BlobServiceAdapter(object):
    def get_client(self, account_url, account_access_key, connection_string):
        if connection_string:
            return BlobServiceClient.from_connection_string(connection_string)
        else:
            return BlobServiceClient(
                account_url=account_url, credential=account_access_key
            )
