from typing import Union
from typing_extensions import Annotated
from fastapi import FastAPI, Header, Path, Query, Request, status
from fastapi.responses import Response, JSONResponse, StreamingResponse

import argparse
import collections
import logging
import os
import struct
import time
import uuid
from contextlib import asynccontextmanager
from io import BytesIO

import httpx
import bson
import hop

from archive import access_api, utility_api

parser = argparse.ArgumentParser()
utility_api.add_parser_options(parser)
access_api.add_parser_options(parser)
parser.add_argument("--hop-auth-api-root", help="Root URL for the hopauth REST API", type=str, 
                    action=utility_api.EnvDefault, envvar="HOP_AUTH_API_ROOT", required=False)
# intentionally 'parse' an empty list of arguments to set defaults
raw_config = parser.parse_args([])
if "CONFIG_FILE" in os.environ:
	utility_api.load_toml_config(os.environ["CONFIG_FILE"], raw_config)
config = raw_config.__dict__
utility_api.make_logging(config)
logging.info(config)

archiveClient = access_api.Archive_access(config)
httpClient = None

@asynccontextmanager
async def lifespan(app: FastAPI):
	logging.info("Connecting to archive")
	await archiveClient.connect()
	global httpClient
	httpClient = httpx.AsyncClient()
	logging.info("Startup complete")
	yield
	
	await httpClient.aclose()
	await archiveClient.close()

app = FastAPI(lifespan=lifespan)

def effective_topic_name_for_access(topic_name: str):
	"""
	Compute the topic name which should be used for querying access control.
	
	When a client wants to send a message to Kafka which is too large for the target
	topic, it is sent to the pseudo-topic whose name is formed by appending the offload_suffix to
	the original target topic name. Since the pseudo topic cannot actually exist in Kafka, and we
	don't want it to collide with anything which does, so it includes a character ('+') which is
	not legal in Kafka topic names. The pseudo topic is intended as an extension of the base topic,
	so its permissions are simply those of the base topic, so we rewrite its name to the base
	topic's name to perform all access queries.
	
	Args:
		topic_name: name of topic to which a user has requetsed access
		
	Returns: The effective topic name which should be used to query whether the user's access is
			 allowed
	"""
	offload_suffix = "+oversized"
	if topic_name.endswith(offload_suffix):
		effective_name = topic_name[:-len(offload_suffix)]
	else:
		effective_name = topic_name
	return effective_name

def default_not_authorized():
	return JSONResponse(status_code=401, 
		headers={"WWW-Authenticate":
		"SCRAM-SHA-512 realm=\"default@dev.hop.scimma.org\""},
		content={"message":"Not Authorized"})

async def stream_s3_response(result):
	async for chunk in result['Body']:
		yield chunk

@app.get("/msg/{msg_id}")
async def fetch_message(msg_id: Annotated[str, Path(title="The ID of message item to get")],
                        authorization: Annotated[Union[str, None], Header()] = None,
                        ):
	resp_headers={}
	
	# First, make sure the message ID supplied by user is something safe and sane
	try:
		# overwrite the variable to normalize
		msg_id = uuid.UUID(msg_id)
	except ValueError:
		return Response(status_code=400, content="Invalid Message ID")
	
	# Get the record, if any, of the message
	metadata = await archiveClient.get_metadata(msg_id)
	if not metadata:
		return Response(status_code=404, content="Message not found")
	
	logging.debug(f"message lives at key {metadata.key} in bucket {metadata.bucket} "
	              f"and was on topic {metadata.topic}")
	
	# Check whether the message is public; if so we can immediately return it to the user
	# TODO: this information is not currently stored in the DB, so we cannot implement this check
	
	# Query hopauth to find out if the user is allowed to read this message.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return default_not_authorized();

	auth_query_url = f"{config['hop_auth_api_root']}/v1/current_credential/permissions/topic/" \
		f"{effective_topic_name_for_access(metadata.topic)}"
	resp = await httpClient.get(auth_query_url, headers={"Authorization": authorization})
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		return Response(status_code=500, content="Internal Error")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!
		
	allowed_ops = resp.json()["allowed_operations"]
	if not isinstance(allowed_ops, collections.Sequence):
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	if not "Read" in allowed_ops:
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)
	
	# If the user is authorized, fetch the message from S3 and stream it back
	return StreamingResponse(stream_s3_response(await archiveClient.get_object_lazily(metadata.key)),
	                         headers=resp_headers)


async def stream_message_list(archiveClient, db_records, next_offset):
	# Time for evil!
	# We want to stream back all of the BSON blobs without having to hold all of them in memory.
	# Luckily, BSON can be embedded in BSON, so we will make up a document on the fly, streaming
	# the chunks of data when we come to where they belong.
	
	# The first thing we need to know is the total size the whole document is going to be.
	# The document we want to send looks like:
	# {
	#  "next_offset": int64
	#  "messages": {
	#               "0": document
	#               "1": document
	#               ...
	#              }
	# }
	# A document must start with a 32 bit size, and end with a NUL byte.
	total_size = 5
	# Each element in the document must have a one byte type, a NUL-terminated name, and its data.
	# The top-level `next_offset` item has an 11 byte key, and 8 bytes of data
	total_size += 1+11+1+8
	# The messages array has an 8 byte key, then a 'array' document for a value
	total_size += 1+8+1
	array_size = 5
	index = 0
	message_keys = []
	for record in db_records:
		key = str(index).encode("utf-8")
		array_size += 1+len(key)+1+record.size
		message_keys.append(key)
	total_size += array_size
	
	# Knowing the total size, we can now build the part of the document that preceds the first
	# message blob.
	header = BytesIO()
	header.write(struct.pack("<i", total_size)) # overall document size
	header.write(b"\x12") # int64 type label
	header.write(b"next_offset\x00") # e_name
	header.write(struct.pack("<q", next_offset)) # value
	header.write(b"\x04") # array type label
	header.write(b"messages\x00") # e_name
	header.write(struct.pack("<i", array_size)) # array document size
	if len(db_records) > 0:
		header.write(b"\x03") # embedded document type type label
		header.write(message_keys[0]) # e_name
		header.write(b"\x00") # e_name terminator
	
	# Send the complete header to the client
	yield header.getvalue()
	
	# Fetch messages in order and send them off
	index = 0
	while index < len(db_records):
		record = db_records[index]
		msg_result = await archiveClient.get_object_lazily(record.key)
		async for chunk in msg_result['Body']:
			yield chunk
		index += 1
		if index < len(db_records):
			# generate the header for the next element
			eheader = BytesIO()
			eheader.write(b"\x03") # embedded document type type label
			eheader.write(message_keys[index]) # e_name
			eheader.write(b"\x00") # e_name terminator
			yield eheader.getvalue()
	yield b"\x00\x00" # NUL terminators for the array and the overall document
	

@app.get("/topic/{topic_name}")
async def fetch_time_range(topic_name: Annotated[str, 
                           Path(title="The name of the topic from which to read")],
                           start: Annotated[int, Query(title="Timestamp for start of time range")],
                           end: Annotated[int, Query(title="Timestamp for end of time range")],
                           limit: Annotated[int, Query(title="Maximum number of messages to return at a tim")] = 10,
                           offset: Annotated[int, Query(title="Offset of first message to return")] = 0,
                           authorization: Annotated[Union[str, None], Header()] = None,
                           ):
	resp_headers={}
	if end<start or start<0:
		return Response(status_code=400, content="Invalid time range")
	
	if limit <= 0:
		return Response(status_code=400, content="Invalid message count limit")
	if limit > 16:
		limit = 16
	
	# Query hopauth to find out if the user is allowed to read this topic.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return default_not_authorized();
	
	auth_query_url = f"{config['hop_auth_api_root']}/v1/current_credential/permissions/topic/" \
		f"{effective_topic_name_for_access(topic_name)}"
	
	resp = await httpClient.get(auth_query_url, headers={"Authorization": authorization})
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		return Response(status_code=500, content="Internal Error")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!
		
	allowed_ops = resp.json()["allowed_operations"]
	if not isinstance(allowed_ops, collections.Sequence):
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	if not "Read" in allowed_ops:
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)
	
	# at this point we know the user is allowed to read the data, if there is any, so we must look for it
	# TODO: sanitize topic_name against SQL-injection?
	#       If we've gotten here it is a topic which actually exists, which is something
	logging.debug(f"Query time range is [{start},{end}) on topic {topic_name}")
	
	db_records = await archiveClient.get_metadata_for_time_range(topic_name, start, end, limit, offset)

	return StreamingResponse(stream_message_list(archiveClient, db_records, offset+len(db_records)),
	                         headers=resp_headers)

def _is_bytes_like(obj):
	try:
		memoryview(obj)
		return True
	except:
		return False

@app.post("/topic/{topic_name}")
async def write_message(request: Request,
                        topic_name: Annotated[str, Path(title="The name of the topic to which to write")],
                        authorization: Annotated[Union[str, None], Header()] = None):
	if config["read_only"]:
		return Response(status_code=501,
		                content="Server is configured as read-only; "
		                "write operations are not supported")
	resp_headers={}
	# Query hopauth to find out if the user is allowed to write to this topic.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return default_not_authorized();

	auth_query_url = f"{config['hop_auth_api_root']}/v1/current_credential/permissions/topic/" \
		f"{effective_topic_name_for_access(topic_name)}"
	
	resp = await httpClient.get(auth_query_url, headers={"Authorization": authorization})
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		return Response(status_code=500, content="Internal Error")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!
		
	allowed_ops = resp.json()["allowed_operations"]
	if not isinstance(allowed_ops, collections.Sequence):
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	if not "Write" in allowed_ops:
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)
	
	# at this point we know the user is allowed to write, so we process the data that was sent

	raw_data = await request.body()
	logging.debug(f"Got a request with size {len(raw_data)}")
	try:
		data = bson.loads(raw_data)
	except Exception as ex:
		logging.warning(str(ex))
		return Response(status_code=400, content="Request must be valid BSON")
	logging.debug(f"Decoded request body: {data}")
	allowed_data_keys = {"message", "headers", "key"}
	for key in data.keys():
		if key not in allowed_data_keys:
			logging.warning(f"Unsupported key in request body: {key}")
			return Response(status_code=400, content=f"Unsupported key in request body: {key}")
	if "message" not in data:
		logging.warning(f"Missing message key in request body")
		return Response(status_code=400, content=f"Missing message key in request body")
	# TODO: this silly wrapper dictionary should go away
	payload = {"content": data["message"]}
	if "headers" in data:
		for key, value in data["headers"]:
			if not _is_bytes_like(value):
				logging.warning(f"Header with key {key} is not binary data")
				return Response(status_code=400,
				                content=f"Header with key {key} is not binary data")
		headers = data["headers"]
	else:
		headers = []
	# TODO: is this always correct in terms of timezone, precision, etc?
	# Convert ns to milliseconds
	timestamp = int(time.time_ns()/1000000+0.5)
	key = b""
	if "key" in data:
		if not _is_bytes_like(data["key"]) and not isinstance(data["key"], str):
			return Response(status_code=400, content=f"Message key is not binary or a string")
		key = data["key"]
	metadata = hop.io.Metadata(topic_name, 0, 0, timestamp, key, headers, None)
	stored, reason = await archiveClient.store_message(payload, metadata)
	
	if stored:
		return Response(status_code=201, headers=resp_headers)
	else:
		logging.warning(reason)
		return Response(status_code=422, content=reason, headers=resp_headers)
