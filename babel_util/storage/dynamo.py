#!/usr/bin/env python3
from botocore.exceptions import ClientError

TABLE_DEFINITION = {"hash_key": "composite_doi",
                    "range_key": "score",
                    "rec_attribute": "rec_dois"}
REC_TYPES = set(('classic', 'expert'))
DATASETS = ['wos']

class Table(object):
    """
    Wrapping a DynamoDB table for use.

    Arguments:
        connection: A dynamodb resource object, likely created via boto3.resource("dynamodb")
        table_name: The name of the table to operate on.

    """
    def __init__(self, connection, table_name):
        self.table_name = table_name
        self.hash_key = TABLE_DEFINITION["hash_key"]
        self.range_key = TABLE_DEFINITION["range_key"]
        self.rec_attribute = TABLE_DEFINITION["rec_attribute"]
        self.rec_types = REC_TYPES
        self.connection = connection
        self.table = None

    def get_table(self):
        if not self.table:
            self.table = self.connection.Table(self.table_name)
        return self.table

    def create(self, read=5, write=5):
        self.table = self.connection.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": self.hash_key,
                 "KeyType": "HASH"},
                {"AttributeName": self.range_key,
                 "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": self.hash_key,
                 "AttributeType": "S"},
                {"AttributeName": self.range_key,
                 "AttributeType": "N"},
                #{"AttributeName": self.rec_attribute,
                # "AttributeType": "SS"}
                ],
            ProvisionedThroughput={
                "ReadCapacityUnits": read,
                "WriteCapacityUnits": write})
        self.table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)

    def delete(self):
        try:
            self.get_table().delete()
            self.get_table().meta.client.get_waiter('table_not_exists').wait(TableName=self.table_name)
            self.table = None
        except ClientError:
            pass

    def get_batch_put_context(self):
        """
        Returns the context manager to use for batched put requests.

        See https://boto3.readthedocs.org/en/latest/guide/dynamodb.html for details
        """
        return self.get_table().batch_writer()

    def put_entry(self, entry):
        return self.get_table().put_item(entry)

    def update_throughput(self, read=5, write=5):
        return self.connection.meta.client.update_table(ProvisionedThroughput={'ReadCapacityUnits': read,
                                                                               'WriteCapacityUnits': write},
                                                        TableName=self.table_name)

