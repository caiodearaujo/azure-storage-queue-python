import os, uuid
from azure.storage.queue import (QueueClient,
                                 BinaryBase64EncodePolicy,
                                 BinaryBase64DecodePolicy)

QUEUE_CONNECTION_STRING = os.getenv('QUEUE_CONNECTION_STRING')

def get_queue_client_from_queue_name(queue_name):
    queue_client = QueueClient.from_connection_string(
        conn_str = QUEUE_CONNECTION_STRING, queue_name = queue_name,
        message_encode_policy = BinaryBase64EncodePolicy,
        message_decode_policy = BinaryBase64DecodePolicy)
    return queue_client

def create_queue_unique_name():
    return 'queue'+str(uuid.uuid4())

def create_queue(queue_name):
    queue_client = get_queue_client_from_queue_name(create_queue_unique_name())
    queue_client.create_queue()
    
def send_message_to_queue(queue_name, message):
    queue_client = get_queue_client_from_queue_name(queue_name)
    queue_client.send_message(message)
    
def peek_messages_from_queue(queue_name):
    queue_client = get_queue_client_from_queue_name(queue_name)
    messages = queue_client.peek_messages()
    return messages

def update_message_in_queue(queue_name, message):
    queue_client = get_queue_client_from_queue_name(queue_name)
    queue_client.update_message(message)
    
def get_queue_size(queue_name):
    queue_client = get_queue_client_from_queue_name(queue_name)
    return queue_client.get_queue_size()

def remove_message_from_queue(queue_name, message):
    queue_client = get_queue_client_from_queue_name(queue_name)
    queue_client.remove_message(message)
    
def remove_queue(queue_name):
    queue_client = get_queue_client_from_queue_name(queue_name)
    queue_client.delete_queue()