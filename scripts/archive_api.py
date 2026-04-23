from typing import Sequence, Union
from typing_extensions import Annotated
from fastapi import FastAPI, Header, Path, Query, Request, status
from fastapi.responses import Response, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

import argparse
import collections
import enum
import logging
import os
import struct
import time
import uuid
from contextlib import asynccontextmanager
from io import BytesIO
from urllib.parse import unquote, urlparse
from typing import List, Optional, Tuple

import httpx
import bson
import hop

from archive import access_api, utility_api
from archive.mview import MMView

parser = argparse.ArgumentParser()
utility_api.add_parser_options(parser)
access_api.add_parser_options(parser)
parser.add_argument("--cors-origins", help="Origin URLs to trust for CORS purposes", type=str, 
                    action=utility_api.EnvDefault, envvar="CORS_ORIGINS", required=False, nargs='*',
                    default=[])
parser.add_argument("--cors-origin-regex", help="Regex for origin URLs to trust for CORS purposes",
                    type=str, action=utility_api.EnvDefault, envvar="CORS_ORIGIN_REGEX", required=False,
                    default="")
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

app = FastAPI(lifespan=lifespan,
              title="Archive API",
              summary="REST API for the Hopskotch Archive")

app.add_middleware(
    CORSMiddleware,
    allow_origins=config["cors_origins"],
    allow_origin_regex=config["cors_origin_regex"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health_check",
         description="Used to check whether the API and its connections to upstream systems are \
                      functioning correctly.",
         responses={
             200: {
                 "description": "The API itself is functioning",
                 "content": {
                     "application/json": {
                         "schema": {
                             "type": "object",
                             "properties": {
                                 "DatabaseOK": {
                                     "type": "boolean",
                                     "description": "Metadata database connection is functioning"
                                 },
                                 "ObjectStoreOK": {
                                     "type": "boolean",
                                     "description": "Object store connection is functioning"
                                 },
                                 "HopAuthOK": {
                                     "type": "boolean",
                                     "description": "Hopskotch API connection is functioning"
                                 },
                             }
                         }
                     }
                 }
             }
         })
async def health_check():
	try:
		# we don't care if there is a message with this UUID,
		# we just want to see if we can make queries
		await archiveClient.db.uuid_in_db(uuid.UUID("00000000-0000-0000-0000-000000000000"))
		database_ok = True
	except:
		database_ok = False
	try:
		# again, the existence of the specific object is not important
		await archiveClient.store.get_object_lazily("not/a/valid/object/key")
		store_ok = True
	except:
		store_ok = False
	try:
		# only the status of the response matters
		resp = await httpClient.get(f"{config['hop_auth_api_root']}/version")
		hop_auth_ok = resp.status_code == 200
	except:
		hop_auth_ok = False
	return {"DatabaseOK": database_ok,
	        "ObjectStoreOK": store_ok,
	        "HopAuthOK": hop_auth_ok,
	       }

messageRecordSchema = {
                          "type": "object",
                          "properties": {
                              "message": {
                                  "type": "string",
                                  "format": "binary",
                                  "description": "The raw mesage body"
                              },
                              "metadata": {
                                  "type": "object",
                                  "description": "Kafka metadata for the message",
                                  "properties": {
                                      "timestamp": {
                                          "type": "number",
                                          "description": "The time at which the message was sent to the Kafka broker"
                                      },
                                      "headers": {
                                          "type": "object",
                                          "description": "Kafka message headers",
                                          "patternProperties": {
                                              ".*": {
                                                  "type": "string",
                                                  "format": "binary",
                                              }
                                          }
                                      },
                                      "key": {
                                          "type": "string",
                                          "format": "binary",
                                          "description": "Kafka message key"
                                      },
                                  }
                              },
                              "annotations": {
                                  "type": "object",
                                  "description": "Metadata added by the archive",
                                  "properties": {
                                      "con_message_crc32": {
                                          "type": "number",
                                          "description": "The CRC32 of the message data"
                                      },
                                      "con_text_uuid": {
                                          "type": "string",
                                          "format": "uuid",
                                          "description": "The message UUID as a string"
                                      },
                                  }
                              },
                          }
                      }

messageOrMetaRecordSchema = {
	"type": "object",
	"properties": {
		"message": {
			"type": "string",
			"format": "binary",
			"description": "The raw mesage body. This is not included if the `meta` parameter is set.",
		},
		"metadata": {
			"type": "object",
			"description": "Kafka metadata for the message",
			"properties": {
				"headers": {
					"type": "object",
					"description": "Kafka message headers. This is not included if the `meta` parameter is set.",
					"patternProperties": {
						".*": {
							"type": "string",
							"format": "binary",
						}
					}
				},
				"key": {
					"type": "string",
					"format": "binary",
					"description": "Kafka message key. This is not included if the `meta` parameter is set.",
				},
				"timestamp": {
					"type": "number",
					"description": "The time at which the message was sent to the Kafka broker.",
				},
				"topic": {
					"type": "string",
					"description": "The name of the Kafka topic to which the message was sent.",
				},
			},
			"required": ["timestamp", "topic"]
		},
		"annotations": {
			 "type": "object",
			 "description": "Metadata added by the archive",
			 "properties": {
				"crc32": {
					"type": "number",
					"description": "The CRC32 of the message+metadata envelope",
				},
				"con_message_crc32": {
					"type": "number",
					"description": "The CRC32 of the message data",
				},
				"con_text_uuid": {
					"type": "string",
					"format": "uuid",
					"description": "The message UUID as a string",
				},
				"public": {
					"type": "boolean",
					"description": "Whether the message is public",
				},
				"size": {
					"type": "number",
					"description": "The size of the message body in bytes",
				},
			 },
			"required": ["crc32", "con_message_crc32", "con_text_uuid", "public", "size"]
		},
	},
	"required": ["metadata", "annotations"]
}

messageListSchema = {
	"type": "object",
	"properties": {
		"messages":{
			"type": "array",
			"description": "Message or metadata records",
			"items": messageOrMetaRecordSchema,
		},
		"next":{
			"type": "string",
			"description": "Bookmark for the next page of results; omitted if there is no next page",
		},
		"prev":{
			"type": "string",
			"description": "Bookmark for the previous page of results; omitted if there is no previous page",
		},
	},
	"required": ["messages"]
}

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
		topic_name: name of topic to which a user has requested access
		
	Returns: The effective topic name which should be used to query whether the user's access is
			 allowed
	"""
	offload_suffix = "+oversized"
	if topic_name.endswith(offload_suffix):
		effective_name = topic_name[:-len(offload_suffix)]
	else:
		effective_name = topic_name
	return effective_name

def authentication_required():
	return JSONResponse(status_code=401, 
		headers={"WWW-Authenticate":
		"SCRAM-SHA-512 realm=\"default@dev.hop.scimma.org\""},
		content={"message":"Authentication required"})

async def stream_s3_response(result):
	async for chunk in result['Body']:
		yield chunk

def transform_db_record(rec):
	return {
		"metadata":{
			"topic": rec.topic,
			"timestamp": rec.timestamp,
			# not key, because it is unrelated to the key in the kafka metadata
		},
		"annotations":{
			"con_text_uuid": rec.uuid,
			"size": rec.size,
			"crc32": rec.crc32,
			"public": rec.public,
			"con_message_crc32": rec.public,
			"title": rec.title,
			"sender": rec.sender,
			"media_type": rec.media_type,
			"file_name": rec.file_name,
			"originator": rec.originator,
			"retracted": rec.retracted,
		},
	}

def has_permission(permission_set: Sequence[str], necessary_permission: str):
	"""Check whether a set of permissions contains a given permission, directly or indirectly via an
	'All' permission.
	
	Args:
		permission_set: the set of permissions which exist
		necessary_permission: the permission for which to check
	
	Returns: Whether a matching permission exists in the permission set
	"""
	return necessary_permission in permission_set or "All" in permission_set

@app.get("/msg/{msg_id}",
         description="Retrieve a single stored message from the archive, by UUID. "
                     "Authentication via SCRAM is required to fetch non-public messages. ",
         response_class=Response,
         responses={
             200: {
                 "description": "The requested message. Note that the format is BSON.",
                 "content": {
                     "application/bson": {
                         "schema": messageRecordSchema
                     }
                 }
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed message ID.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Invalid Message ID"]
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Authentication required"]
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to the requested message.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"]
                         }
                     }
                 }
             },
             404: {
                 "description": "Message not found. There is no archived message with the specified ID.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Message not found"]
                         }
                     }
                 }
             },
         })
async def fetch_message(request: Request,
                        msg_id: Annotated[str, Path(description="The ID of message item to get", 
                                                    json_schema_extra={"type": "string", "format": "uuid", "examples": ["457E145B-9F72-416F-87CA-73F0E188183D"]})],
                        authorization: Annotated[Union[str, None], Header(description="RFC 7804 SCRAM authentication")] = None,
                        ):
	resp_headers: dict[str,str]={}
	
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
	if metadata.public:
		# TODO: better error handling if get_object_lazily fails and returns None
		return StreamingResponse(stream_s3_response(await archiveClient.get_object_lazily(metadata.key)),
	                         headers=resp_headers)
	
	# Query hopauth to find out if the user is allowed to read this message.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return authentication_required();
	
	path_root=urlparse(config['hop_auth_api_root']).path
	topic_name = effective_topic_name_for_access(metadata.topic)
	resp = await httpClient.post(config['hop_auth_api_root']+"/v1/multi",
							 json={
							   "ops":{
								 "method":"get",
								 "path":f"{path_root}/v1/current_credential/permissions/topic/{topic_name}",
								 "headers":{"Authorization": "Inherit"},
							   },
							   "whoami":{
								 "method":"get",
								 "path":f"{path_root}/v1/current_user",
								 "headers":{"Authorization": "Inherit"},
							   },
							 },
							 headers={"Authorization": authorization}
							 )
	
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		return Response(status_code=500, content="Internal Error")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!
	
	hop_json = resp.json()
	if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_id = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
	
	logging.info(f"FETCH_MESSAGE request by {user_id} from {request.client.host}")
	allowed_ops = hop_json["ops"]["body"]["allowed_operations"]
	if not isinstance(allowed_ops, collections.abc.Sequence):
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	if not has_permission(allowed_ops, "Read"):
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)
	
	# If the user is authorized, fetch the message from S3 and stream it back
	return StreamingResponse(stream_s3_response(await archiveClient.get_object_lazily(metadata.key)),
	                         headers=resp_headers)


async def stream_raw_payload(s3_result):
	def anext(iterator, default=None):
		__anext__ = type(iterator).__anext__
		async def anext_impl():
			try:
				return await __anext__(iterator)
			except StopAsyncIteration:
				return default
		return anext_impl()
	
	iterator = s3_result['Body'].__aiter__()
	current_chunk = await anext(iterator, None)
	if current_chunk is None:
		return
	
	current_offset = 0
	len_remaining = 0
	
	async def read(amount: int):
		nonlocal current_chunk
		nonlocal current_offset
		nonlocal len_remaining
		value = bytearray()
		while current_chunk is not None and amount > 0:
			clen = len(current_chunk)
			if amount <= clen:
				value += current_chunk[current_offset:current_offset+amount]
				current_offset += amount
				amount = 0
			else:
				value += current_chunk[current_offset:]
				amount -= (clen - current_offset)
				current_offset = clen
			if current_offset >= clen:
				current_chunk = await anext(iterator, None)
				current_offset = 0
		if current_chunk is None and amount > 0:
			raise ValueError("read: Early end of data stream")
		len_remaining -= len(value)
		return value

	async def ignore(amount: int):
		nonlocal current_chunk
		nonlocal current_offset
		nonlocal len_remaining
		while current_chunk is not None and amount > 0:
			clen = len(current_chunk)
			if amount <= clen:
				current_offset += amount
				len_remaining -= amount
				amount = 0
			else:
				amount -= (clen - current_offset)
				len_remaining -= (clen - current_offset)
				current_offset = clen
			if current_offset >= clen:
				current_chunk = await anext(iterator, None)
				current_offset = 0
		if current_chunk is None and amount > 0:
			raise ValueError("ignore: Early end of data stream")

	async def send(amount: int):
		nonlocal current_chunk
		nonlocal current_offset
		nonlocal len_remaining
		while current_chunk is not None and amount > 0:
			clen = len(current_chunk)
			if amount <= clen:
				yield current_chunk[current_offset:current_offset+amount]
				current_offset += amount
				len_remaining -= amount
				amount = 0
			else:
				yield current_chunk[current_offset:]
				amount -= (clen - current_offset)
				len_remaining -= (clen - current_offset)
				current_offset = clen
			if current_offset >= clen:
				current_chunk = await anext(iterator, None)
				current_offset = 0
		if current_chunk is None and amount > 0:
			raise ValueError("send: Early end of data stream")
			
	async def read_cstring():
		value = bytearray()
		while True:
			c = await read(1)
			if c == b'\x00':
				return value.decode("utf-8")
			value.extend(c)
	
	def decode_int32(data):
		return struct.unpack("<i", data)[0]
	
	@enum.unique
	class BSONType(enum.Enum):
		double =      0x01
		string =      0x02
		document =    0x03
		array =       0x04
		binary =      0x05
		undefined =   0x06
		object_id =   0x07
		bool =        0x08
		datetime =    0x09
		null =        0x0A
		regex =       0x0B
		db_pointer =  0x0C
		javascript =  0x0D
		symbol =      0x0E
		scoped_code = 0x0F
		int32 =       0x10
		timestamp =   0x11
		int64 =       0x12
		decimal128 =  0x13
		min_key =     0xFF
		max_key =     0x7F

	try:
		len_remaining = decode_int32(await read(4))
# 		print("Full document length:", len_remaining)
		len_remaining -= 4
		while len_remaining > 1:  # expect one byte for end of document
			raw_e_type = await read(1)
# 			print("  Element type:", raw_e_type.hex())
			if raw_e_type[0] == 0:  # end of document
				break
			if len_remaining < 2:
				raise ValueError("Malformed document")
			try:
				e_type = BSONType(raw_e_type[0])
			except ValueError:
				logging.warning(f"Unexpected element type for BSON 1.1: 0x{raw_e_type.hex()}")
				return
			e_name = await read_cstring()
# 			print("  Element name:", e_name)
			if len_remaining < 1:
				raise ValueError("Malformed document")

			# If this is our blessed sub-object, extract it
			if e_type == BSONType.binary and e_name == "message":
				raw_dlen = await read(4)
				dlen = decode_int32(raw_dlen)
				# plus one byte for subtype
				if dlen + 1 >= len_remaining:
					raise ValueError("Malformed binary: embedded binary longer than remainder of document")
				# No one really cares what the subtype is; just extract and discard
				subtype = await read(1)
				async for chunk in send(dlen):
					yield bytes(chunk)
				# We don't really care what else was in the document; we won't send it, so skip
				# reading and parsing it
				return

			# Otherwise, this is just something to skip over
			if e_type == BSONType.double or e_type == BSONType.datetime \
					or e_type == BSONType.timestamp or e_type == BSONType.int64:
				await ignore(8)
			elif e_type == BSONType.string or e_type == BSONType.javascript \
					 or e_type == BSONType.symbol:
				slen = decode_int32(await read(4))
				if slen >= len_remaining:
					raise ValueError("Malformed document: string longer than remainder of document")
				await ignore(slen)
			elif e_type == BSONType.document or e_type == BSONType.array:
				dlen = decode_int32(await read(4))
# 				print("    Document length:", dlen)
				if dlen < 5:
					raise ValueError("Malformed document: Too small length for embedded document")
				dlen -= 4  # we already consumed the length
				if dlen >= len_remaining:
					raise ValueError("Malformed document: embedded document longer than remainder of document")
				await ignore(dlen)
# 				print("    Done skipping document, length remaining:", len_remaining)
			elif e_type == BSONType.binary:
				raw_dlen = await read(4)
				dlen = decode_int32(raw_dlen)
				# plus one byte for subtype
				if dlen + 1 >= len_remaining:
					raise ValueError("Malformed binary: embedded binary longer than remainder of document")
				await ignore(dlen + 1)
			elif e_type == BSONType.undefined or e_type == BSONType.null \
					or e_type == BSONType.min_key or e_type == BSONType.max_key:
				pass  # zero size element; nothing to do
			elif e_type == BSONType.object_id:
				await ignore(12)
			elif e_type == BSONType.bool:
				await ignore(1)
			elif e_type == BSONType.regex:
				_ = await read_cstring()
				_ = await read_cstring()
			elif e_type == BSONType.db_pointer:
				slen = decode_int32(await read(4))
				if slen >= len_remaining:
					raise ValueError("Malformed document: DB pointer string longer than remainder of document")
				await ignore(slen + 12)
			elif e_type == BSONType.scoped_code:
				clen = decode_int32(await read(4))
				if clen < 10:
					raise ValueError("Malformed document: Too small length for code with scope")
				clen -= 4  # we already consumed the length
				if clen >= len_remaining:
					raise ValueError("Malformed document: code with scope longer than remainder of document")
				await ignore(clen)
			elif e_type == BSONType.int32:
				await ignore(4)
			elif e_type == BSONType.decimal128:
				await ignore(16)
			else:
				raise ValueError(f"Internal Error: uncovered e_type: {e_type.value}")
	except ValueError as err:
		logging.warning(f"BSON decoding error: {err}")
		return
	except struct.error as err:
		logging.warning(f"BSON decoding error: {err}")
		return


@app.get("/msg/{msg_id}/raw_file/{file_name}",
         description="Retrieve the raw body of a single stored message from the archive, by UUID. "
                     "The data will be treated as if it were a file with the specified name. "
                     "Kafka headers and other metadata are not available via this mechanism. "
                     "Authentication via SCRAM is required to fetch non-public messages.",
         response_class=Response,
         responses={
             200: {
                 "description": "The requested message body.",
                 "content": {
                     "application/octet-stream": {}
                 }
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed message ID.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Invalid Message ID"],
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Authentication required"],
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to the requested message.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"],
                         }
                     }
                 }
             },
             404: {
                 "description": "Message not found. There is no archived message with the specified ID.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Message not found"],
                         }
                     }
                 }
             },
         })
async def fetch_raw_message(request: Request,
                            msg_id: Annotated[str, Path(title="The ID of message item to get")],
                            file_name: Annotated[str, Path(title="Name to treat the payload as having")],
                            authorization: Annotated[Union[str, None], Header(description="RFC 7804 SCRAM authentication")] = None,
                            ):
	resp_headers: dict[str,str]={}
	user_id = "unauthenticated user"
	
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
	if metadata.public:
		logging.info(f"FETCH_RAW_MESSAGE request by {user_id} from {request.client.host}")
		resp_headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
		return StreamingResponse(stream_raw_payload(await archiveClient.get_object_lazily(metadata.key)),
		                         headers=resp_headers)
	
	# Query hopauth to find out if the user is allowed to read this message.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return authentication_required();
	
	path_root=urlparse(config['hop_auth_api_root']).path
	topic_name = effective_topic_name_for_access(metadata.topic)
	resp = await httpClient.post(config['hop_auth_api_root']+"/v1/multi",
							 json={
							   "ops":{
								 "method":"get",
								 "path":f"{path_root}/v1/current_credential/permissions/topic/{topic_name}",
								 "headers":{"Authorization": "Inherit"},
							   },
							   "whoami":{
								 "method":"get",
								 "path":f"{path_root}/v1/current_user",
								 "headers":{"Authorization": "Inherit"},
							   },
							 },
							 headers={"Authorization": authorization}
							 )
	
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		return Response(status_code=500, content="Internal Error")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!
	
	hop_json = resp.json()
	if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_id = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
	
	logging.info(f"FETCH_RAW_MESSAGE request by {user_id} from {request.client.host}")
	allowed_ops = hop_json["ops"]["body"]["allowed_operations"]
	if not isinstance(allowed_ops, collections.abc.Sequence):
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	if not has_permission(allowed_ops, "Read"):
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)
	
	resp_headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
	
	# If the user is authorized, fetch the message from S3 and stream it back
	return StreamingResponse(stream_raw_payload(await archiveClient.get_object_lazily(metadata.key)),
	                         headers=resp_headers)


async def stream_message_list(archiveClient, db_records, *, next_offset: Optional[int]=None,
                              next_mark: Optional[str]=None, prev_mark: Optional[str]=None):
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
	if next_offset is not None:
		# The top-level `next_offset` item has an 11 byte key, and 8 bytes of data
		total_size += 1+11+1+8
	else:
		if next_mark is not None:
			next_mark = next_mark.encode("utf-8")
			# the bookmark has a 4 byte key and a string value
			total_size += 1+4+1+4+len(next_mark)+1
		else:
			# the bookmark has a 4 byte key and a null value
			total_size += 1+4+1
		if prev_mark is not None:
			prev_mark = prev_mark.encode("utf-8")
			# the bookmark has a 4 byte key and a string value
			total_size += 1+4+1+4+len(prev_mark)+1
		else:
			# the bookmark has a 4 byte key and a null value
			total_size += 1+4+1
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
	
	# Knowing the total size, we can now build the part of the document that precedes the first
	# message blob.
	header = BytesIO()
	header.write(struct.pack("<i", total_size)) # overall document size
	if next_offset is not None:
		header.write(b"\x12") # int64 type label
		header.write(b"next_offset\x00") # e_name
		header.write(struct.pack("<q", next_offset)) # value
	else:
		header.write(b"\x02" if next_mark is not None else b"\x0A") # string type or null type
		header.write(b"next\x00") # e_name
		if next_mark is not None:
			header.write(struct.pack("<i", len(next_mark)+1)) # string length
			header.write(next_mark)
			header.write(b"\x00") # NUL terminator
		
		header.write(b"\x02" if prev_mark is not None else b"\x0A") # string type or null type
		header.write(b"prev\x00") # e_name
		if prev_mark is not None:
			header.write(struct.pack("<i", len(prev_mark)+1)) # string length
			header.write(prev_mark)
			header.write(b"\x00") # NUL terminator
		
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
		if msg_result is None:
			err=f"Failed to fetch message data for key {record.key}; unable to complete request"
			logging.error(err)
			raise KeyError(err)
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
	

@app.get("/topic/{topic_name}",
         description="Fetch messages or metadata about messages which were published on a single topic. "
                     "Time ranges are specified as Kafka times (milliseconds since the unix epoch). ",
         response_class=Response,
         responses={
             200: {
                "description": "Message data. Note that the format is BSON.",
                 "content": {
                     "application/bson": {
                         "schema": messageListSchema
                     }
                 }
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed request parameter.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to read some requested data.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"],
                         }
                     }
                 }
             },
             500: {
                 "description": "Internal error.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
            })
async def fetch_from_single_topic(request: Request,
                                  topic_name: Annotated[str, Path(description="The name of the topic from which to read")],
                                  start_time: Annotated[Optional[int], Query(description="Timestamp for start of time range")] = None,
                                  end_time: Annotated[Optional[int], Query(description="Timestamp for end of time range")] = None,
                                  page: Annotated[Optional[str], Query(description="A bookmark string describing the page of results to fetch, as returned by a previous query. If not specified, the first page will be fetched.")]=None,
                                  limit: Annotated[Optional[int], Query(description="The maximum number of results to fetch. Any results of the query beyond this can be fetched in subsequent queries by specifying bookmark for the subsequent page(s). This is capped at 64 when fetching full messages, and 1024 when fetching only message metadata.")]=64,
                                  asc: Annotated[bool, Query(description="Results in ascending time order")]=True,
                                  include_retracted: Annotated[bool, Query(description="Include retracted messages in results")]=True,
                                  meta: Annotated[bool, Query(description="Return only metadata without message payloads, instead of the full messages.")]=False,
                                  authorization: Annotated[Union[str, None], Header(description="Any authentication accesed by scimma-admin, including RFC 7804 SCRAM authentication and REST tokens. If not specified, only public results will be returned.")] = None,
                                  ):
	resp_headers: dict[str,str]={}
	topic_name = unquote(topic_name)
	user_id = "unauthenticated user"
	if start_time is not None and end_time is not None and end_time < start_time:
		return Response(status_code=400, content="Invalid time range")
	
	if limit is not None and limit <= 0:
		return Response(status_code=400, content="Invalid message count limit")
	max_limit = 1024 if meta else 64
	if limit is None or limit > max_limit:
		limit = max_limit

	
	if authorization is None:
		# if no authentication is supplied, assume public-only access
		topics_full = []
		topics_public = [topic_name]
	else:
		# Query hopauth to find out if the user is allowed to read this topic.
		# This requires authenticating the user to hopauth.
		base_topic_name = effective_topic_name_for_access(topic_name)
		path_root=urlparse(config['hopauth_api_url']).path
		resp = await httpClient.post(config['hopauth_api_url']+"/v1/multi",
	                             json={
	                               "ops":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_credential/permissions/topic/{base_topic_name}",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                               "whoami":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_user",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                             },
	                             headers={"Authorization": authorization}
	                             )
		if resp.status_code == 401 and "www-authenticate" in resp.headers:
			return Response(status_code=401, content=resp.content, 
							headers={"www-authenticate": resp.headers["www-authenticate"]})
		if resp.status_code != 200:
			if resp.status_code >= 200 and resp.status_code <=499:
				# It would be nice to be more informative here, but Django makes it awkward for
				# the hopauth code to send things other than error 400 when anything goes wrong.
				return Response(status_code=400, content="Bad Request")
			return Response(status_code=500, content="Internal Error")
		if "authentication-info" in resp.headers:
			resp_headers["authentication-info"] = resp.headers["authentication-info"]
		# After this point it is important to always return a response with resp_headers
		# as the client may be expecting the authentication-info!
		
		hop_json = resp.json()
		if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_id = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
		
		allowed_ops = hop_json["ops"]["body"]["allowed_operations"]
		if not isinstance(allowed_ops, collections.abc.Sequence):
			return Response(status_code=500, content="Internal Error", headers=resp_headers)
		if not has_permission(allowed_ops, "Read"):
			topics_full = [topic_name]
			topics_public = []
		else:
			topics_full = []
			topics_public = [topic_name]
	logging.info(f"READ_SINGLE_TOPIC request by {user_id} from {request.client.host} on topic {topic_name}")
	
	try:
		mrecords, nmark, pmark = await archiveClient.get_message_records(bookmark=page, 
		                                                                 page_size=limit,
		                                                                 ascending=asc, 
		                                                                 topics_public=topics_public,
		                                                                 topics_full=topics_full,
		                                                                 start_time=start_time,
		                                                                 end_time=end_time,
		                                                                 include_retracted=include_retracted)
	except Exception as err:
		logging.error(f"Error fetching message records from database: {err}")
		# Note: error might be a bad request, e.g. due to invalid bookmark data
		return Response(status_code=500, content="Internal Error", headers=resp_headers)

	if meta:
		# if the client wants only the metadata, we can just return it directly
		return Response(content=bson.dumps({"messages":[transform_db_record(m) for m in mrecords], 
											"next":nmark, "prev":pmark}),
						headers=resp_headers)
	
	return StreamingResponse(stream_message_list(archiveClient, mrecords, next_mark=nmark, prev_mark=pmark),
	                         headers=resp_headers)

def _is_bytes_like(obj):
	try:
		memoryview(obj)
		return True
	except:
		return False

@app.post("/topic/{topic_name}",
         description="Publish a message directly to the archive. "
                     "Messages should generally be sent directly via the Kafka brokers, but large "
                     "messages and 'attachement' data can be uploaded directly via this mechanism, "
                     "and then referenced in smaller messages sent directly. "
                     "Authentication is required to publish messages, and authorization is required "
                     "for writing to the target topic.",
         response_class=Response,
         openapi_extra={
             "requestBody": {
                 "content": {
                     "application/bson": {
                         "schema": {
                             "type": "object",
                             "properties": {
                                 "message": {
                                     "type": "string",
                                     "format": "binary",
                                     "description": "The body of the message."
                                 },
                                 "headers": {
                                     "type": "array",
                                     "description": "The Kafka headers attached to the message. "
                                                    "This must be either an array of 2-tuples mapping "
                                                    "strings to binary blobs, or an equivalent dictionary/object. "
                                                    "One particularly important header is the _id header, "
                                                    "whose value should be the binary representation of a "
                                                    "UUID which the sender wishes to associate with the "
                                                    "mesasge. "
                                                    "See https://hop-client.readthedocs.io/en/stable/user/stream.html#standard-headers "
                                                    "for more information on Hopskotch message headers. ",
                                     "items": {
                                         "type": "array",
                                         "minItems": 2,
                                         "maxItems": 2,
                                         "items": {
                                             "type": "string",
                                             "format": "binary",
                                         }
                                     }
                                 },
                                 "key": {
                                     "type": "string",
                                     "format": "binary",
                                     "description": "The Kafka key for the message."
                                 }
                             },
                             "required": ["message"],
                         }
                     }
                 }
             }
         },
         status_code=201,
         responses={
             201: {
                 "description": "Message stored successfully.",
                 "content": {
                     "application/bson": {
                         "schema": {
                             "type": "object"
                         }
                     }
                 }
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed request body.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": [
                                 "Unsupported key {key} in request body",
                                 "Missing message key in request body",
                                 "Header with key {key} is not binary data",
                                 "Message key is not binary or a string",
                             ]
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to write to the target topic.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"],
                         }
                     }
                 }
             },
             422: {
                 "description": "Message could not be stored",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
         })
async def write_message(request: Request,
                        topic_name: Annotated[str, Path(description="The name of the topic to which to write")],
                        authorization: Annotated[Union[str, None], Header(description="RFC 7804 SCRAM authentication")] = None):
	if config["read_only"]:
		return Response(status_code=501,
		                content="Server is configured as read-only; "
		                "write operations are not supported")
	resp_headers: dict[str,str]={}
	user_id = "unauthenticated user"
	# Query hopauth to find out if the user is allowed to write to this topic.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return authentication_required();

	topic_name = unquote(topic_name)
	base_topic_name = effective_topic_name_for_access(topic_name)
	path_root=urlparse(config['hopauth_api_url']).path
	resp = await httpClient.post(config['hopauth_api_url']+"/v1/multi",
	                             json={
	                               "ops":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_credential/permissions/topic/{base_topic_name}",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                               "topic":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/topics/{base_topic_name}",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                               "whoami":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_user",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                             },
	                             headers={"Authorization": authorization}
	                             )
	
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		logging.info(f"WRITE_MESSAGE request by {user_id} from {request.client.host} to topic {topic_name}")
		logging.error("hop_auth API error: {resp.status_code}")
		return Response(status_code=500, content="Internal Error: hop_auth API request failed")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!

	hop_json = resp.json()
	if not isinstance(hop_json, collections.abc.Mapping) \
	  or "ops" not in hop_json or not isinstance(hop_json["ops"], collections.abc.Mapping) \
	  or "status" not in hop_json["ops"] \
	  or "topic" not in hop_json or not isinstance(hop_json["topic"], collections.abc.Mapping) \
	  or "status" not in hop_json["topic"] \
	  or "whoami" not in hop_json or not isinstance(hop_json["whoami"], collections.abc.Mapping) \
	  or "status" not in hop_json["whoami"]:
		return Response(status_code=500, content="Internal Error: Malformed response from hop_auth API", headers=resp_headers)
	
	if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_id = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
	
	if hop_json["ops"]["status"]!=200 or hop_json["topic"]["status"]!=200:
		if hop_json["ops"]["status"]>=400 and hop_json["ops"]["status"]<=499:
			return Response(status_code=hop_json["ops"]["status"], 
			                content="hop_auth API permissions sub-request failed "
			                        f"({hop_json['ops']['status']}): {hop_json['ops'].get('body')}", 
			                headers=resp_headers)
		if hop_json["topic"]["status"]>=400 and hop_json["topic"]["status"]<=499:
			return Response(status_code=hop_json["topic"]["status"], 
			                content="hop_auth API topic sub-request failed "
			                        f"({hop_json['topic']['status']}): {hop_json['topic'].get('body')}", 
			                headers=resp_headers)
		logging.info(f"WRITE_MESSAGE request by {user_id} from {request.client.host} to topic {topic_name}")
		logging.error("Error from hop_auth API: \n"
		              f"Permissions ({hop_json['ops']['status']}): {hop_json['ops'].get('body')}\n"
		              f"Topic ({hop_json['topic']['status']}): {hop_json['topic'].get('body')}"
		              )
		return Response(status_code=500, content="Internal Error: hop_auth API sub-request failed", headers=resp_headers)
	
	logging.info(f"WRITE_MESSAGE request by {user_id} from {request.client.host} to topic {topic_name}")

	if "body" not in hop_json["ops"] or not isinstance(hop_json["ops"]["body"], collections.abc.Mapping) \
	  or "allowed_operations" not in hop_json["ops"]["body"] \
	  or not isinstance(hop_json["ops"]["body"]["allowed_operations"], collections.abc.Sequence) \
	  or "body" not in hop_json["topic"] or not isinstance(hop_json["topic"]["body"], collections.abc.Mapping) \
	  or "publicly_readable" not in hop_json["topic"]["body"]:
		return Response(status_code=500, content="Internal Error: Malformed response from hop_auth API", headers=resp_headers)

	if not has_permission(hop_json["ops"]["body"]["allowed_operations"], "Write"):
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)

	message_is_public = hop_json["topic"]["body"]["publicly_readable"]
	
	# at this point we know the user is allowed to write, so we process the data that was sent
	raw_data = MMView()
	async for chunk in request.stream():
		raw_data.append(chunk)
	try:
		data = bson.loads(raw_data)
	except:
		logging.exception("Failed to decode request body as BSON")
		return Response(status_code=400, content="Request must be valid BSON", headers=resp_headers)

	allowed_data_keys = {"message", "headers", "key"}
	for key in data.keys():
		if key not in allowed_data_keys:
			logging.warning(f"Unsupported key in request body: {key}")
			return Response(status_code=400, content=f"Unsupported key in request body: {key}",
			                headers=resp_headers)
	if "message" not in data:
		logging.warning(f"Missing message key in request body")
		return Response(status_code=400, content=f"Missing message key in request body",
		                headers=resp_headers)
	payload = data["message"]
	if "headers" in data:
		headers = data["headers"]
		if isinstance(headers, collections.abc.Mapping):
			headers = list(headers.items())
		for key, value in headers:
			if not _is_bytes_like(value):
				logging.warning(f"Header with key {key} is not binary data")
				return Response(status_code=400,
				                content=f"Header with key {key} is not binary data",
				                headers=resp_headers)
	else:
		headers = []
	# TODO: is this always correct in terms of timezone, precision, etc?
	# Convert ns to milliseconds
	timestamp = int(time.time_ns()/1000000+0.5)
	key = b""
	if "key" in data:
		if not _is_bytes_like(data["key"]) and not isinstance(data["key"], str):
			return Response(status_code=400, content=f"Message key is not binary or a string",
			                headers=resp_headers)
		key = data["key"]
	metadata = hop.io.Metadata(topic_name, 0, 0, timestamp, key, headers, None)
	try:
		stored, store_meta, reason = await archiveClient.store_message(payload, metadata,
		                                                               direct_upload=True)
	except:
		logging.exception("Failed to store record to archive")
		return Response(status_code=500, content="Internal Error: Failed to store message",
		                headers=resp_headers)

	if stored:
		return Response(status_code=201, content=bson.dumps(store_meta), headers=resp_headers)
	else:
		logging.warning(reason)
		return Response(status_code=422, content=reason, headers=resp_headers)


#===================================================================================================


@app.get("/topics",
         description="Get the list of topics which can be accessed with the current authentication",
         response_class=Response,
         responses={
             200: {
                "description": "Message data. Note that the format is BSON.",
                 "content": {
                     "application/bson": {
                         "schema": {
                             "type": "object",
                             "properties": {
                                 "topics": {
                                     "type": "array",
                                     "description": "The names of topics which are accessible",
                                     "items": {
                                         "type": "string"
                                     }
                                 },
                             },
                             "required": ["topics"]
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to read some requested data.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"],
                         }
                     }
                 }
             },
             500: {
                 "description": "Internal error.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
         })
async def get_available_topics(request: Request,
                               authorization: Annotated[Optional[str], Header(description="Any authentication accesed by scimma-admin, including RFC 7804 SCRAM authentication and REST tokens. If not specified, only public results will be returned.")] = None):
	resp_headers: dict[str,str]={}
	user_id = "unauthenticated user"
	if authorization is None:
		# assume that if no authorization was sent, the request is about public data only
		topic_perms = []
	else:
		path_root=urlparse(config['hopauth_api_url']).path
		resp = await httpClient.post(config['hopauth_api_url']+"/v1/multi",
	                             json={
	                               "perms":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_user/available_permissions",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                               "whoami":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_user",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                             },
	                             headers={"Authorization": authorization}
	                             )
		
		if resp.status_code == 401 and "www-authenticate" in resp.headers:
			return Response(status_code=401, content=resp.content, 
			                headers={"www-authenticate": resp.headers["www-authenticate"]})
		if resp.status_code != 200:
			logging.error(f"Error querying {config['hopauth_api_url']}: {resp.content.decode('utf-8')}")
			return Response(status_code=500, content="Internal Error")
		if "authentication-info" in resp.headers:
			resp_headers["authentication-info"] = resp.headers["authentication-info"]
		hop_json = resp.json()
		
		if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_id = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
		topic_perms = hop_json["perms"]["body"]
	
	logging.info(f"GET_ACCESSIBLE_TOPICS request by {user_id} from {request.client.host}")
	topics = set(await archiveClient.get_topics_with_public_messages())
	for perm in topic_perms:
		if perm["operation"] == "Read" or perm["operation"] == "All":
			topics.add(perm["topic"])
	return Response(content=bson.dumps({"topics":list(topics)}), headers=resp_headers)

async def authorize_multi_topic_access(authorization, resp_headers, topics) -> Tuple[Optional[Response], str, Optional[List[str]], Optional[List[str]]]:
	"""
	Params:
		resp_headers: The headers which must be included with any subsequent response.
		              This object may be modified/updated.
	Returns a tuple of the response which _must_ be immediately sent back if not None,
	        an identifier for the user making the request,
	        the list of public topics, and the list of full/private access topics.
	"""
	user_identifier = "unauthenticated user"
	topics_public = None
	topics_full = None
	if authorization is not None:
		# treat the user as (potentially) logged-in, so all topics to which the user has full access
		# should be treated as such, while all others will have usual public-only access level
		path_root=urlparse(config['hopauth_api_url']).path
		resp = await httpClient.post(config['hopauth_api_url']+"/v1/multi",
	                             json={
	                               "perms":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_user/available_permissions",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                               "whoami":{
	                                 "method":"get",
	                                 "path":f"{path_root}/v1/current_user",
	                                 "headers":{"Authorization": "Inherit"},
	                               },
	                             },
	                             headers={"Authorization": authorization}
	                             )
		if resp.status_code == 401 and "www-authenticate" in resp.headers:
			return (Response(status_code=401, content=resp.content, 
			                 headers={"www-authenticate": resp.headers["www-authenticate"]}),
			        user_identifier, topics_public, topics_full)
		if resp.status_code != 200:
			logging.error(f"Error querying {config['hopauth_api_url']}: {resp.content.decode('utf-8')}")
			return (Response(status_code=500, content="Internal Error"), user_identifier, topics_public, topics_full)
		if "authentication-info" in resp.headers:
			resp_headers["authentication-info"] = resp.headers["authentication-info"]
		
		hop_json = resp.json()
		if not isinstance(hop_json, collections.abc.Mapping) \
		  or "perms" not in hop_json or not isinstance(hop_json["perms"]["body"], collections.abc.Sequence) \
		  or "whoami" not in hop_json or not isinstance(hop_json["whoami"]["body"], collections.abc.Mapping) \
		  or "status" not in hop_json["perms"] or "status" not in hop_json["whoami"] \
		  or hop_json["perms"]["status"]!=200 or hop_json["whoami"]["status"]!=200:
			return (Response(status_code=500, content="Internal Error: Malformed/failed response from hop_auth API", headers=resp_headers),
			        user_identifier, topics_public, topics_full)
		
		topic_perms = hop_json["perms"]["body"]
		
		allowed_topics_full = set()
		for perm in topic_perms:
			if perm["operation"] == "Read" or perm["operation"] == "All":
				allowed_topics_full.add(perm["topic"])
		
		if len(topics) == 0:
			# if the user made no topic selection, select _all_ accessible topics
			topics_full = list(allowed_topics_full)
			topics_with_public = set(await archiveClient.get_topics_with_public_messages())
			topics_public = list(topics_with_public-allowed_topics_full)
		else:
			# otherwise, apply the selection requested by the user
			topics_full = [t for t in topics if t in allowed_topics_full]
			topics_public = [t for t in topics if t not in allowed_topics_full]
		if len(topics_full) == 0:
			topics_full = None
		if len(topics_public) == 0:
			topics_public = None
		
		if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_identifier = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
	else:
		# with no authN data, the user cannot be logged in, so all access is necessarily at the
		# public-only level
		if len(topics) != 0:
			topics_public = topics
		# the database layer will automatically assume public data on any topic if no topics are
		# specified
	return (None, user_identifier, topics_public, topics_full)

@app.get("/messages",
         description="Fetch messages or metadata about messages.",
         response_class=Response,
         responses={
             200: {
                "description": "Message data. Note that the format is BSON.",
                 "content": {
                     "application/bson": {
                         "schema": messageListSchema
                     }
                 }
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed request parameter.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to read some requested data.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"],
                         }
                     }
                 }
             },
             500: {
                 "description": "Internal error.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
            })
async def get_messages(request: Request,
                       topic: Annotated[list[str], Query(description="The topic(s) from which to count messages. If not specified, the count will be across all accessible topics. Note that this parameter can be specified repeatedly to select multiple topics.")]=[],
                       start_time: Annotated[Optional[int], Query(description="Timestamp for start of time range. Kafka times are represented as milliseconds since the unix epoch. Leave unspecified for no constraint.")]=None,
                       end_time: Annotated[Optional[int], Query(description="Timestamp for end of time range. Kafka times are represented as milliseconds since the unix epoch. Leave unspecified for no constraint.")]=None,
                       search_query: Annotated[Optional[str], Query(description="If specified, a general text-search query to select messages")]=None,
                       page: Annotated[Optional[str], Query(description="A bookmark string describing the page of results to fetch, as returned by a previous query. If not specified, the first page will be fetched.")]=None,
                       limit: Annotated[Optional[int], Query(description="The maximum number of results to fetch. Any results of the query beyond this can be fetched in subsequent queries by specifying bookmark for the subsequent page(s). This is capped at 64 when fetching full messages, and 1024 when fetching only message metadata.")]=64,
                       asc: Annotated[bool, Query(description="Results in ascending time order")]=True,
                       include_retracted: Annotated[bool, Query(description="Include retracted messages in results")]=True,
                       meta: Annotated[bool, Query(description="Return only metadata without message payloads, instead of the full messages.")]=False,
                       authorization: Annotated[Optional[str], Header(description="Any authentication accesed by scimma-admin, including RFC 7804 SCRAM authentication and REST tokens. If not specified, only public results will be returned.")] = None):
	resp_headers: dict[str,str]={}
	request_start=time.time()
	
	max_limit = 1024 if meta else 64
	if limit is None or limit > max_limit:
		limit = max_limit
	
	required_resp, user_id, topics_public, topics_full = \
		await authorize_multi_topic_access(authorization, resp_headers, topic)
	logging.info(f"READ_FROM_TOPICS request by {user_id} from {request.client.host} on topics {topic}")
	if required_resp is not None:
		return required_resp
	
	try:
		if search_query is None:
			mrecords, nmark, pmark = await archiveClient.get_message_records(bookmark=page, 
			                                                                 page_size=limit,
			                                                                 ascending=asc, 
			                                                                 topics_public=topics_public,
			                                                                 topics_full=topics_full,
			                                                                 start_time=start_time,
			                                                                 end_time=end_time,
			                                                                 include_retracted=include_retracted)
		else:
			mrecords, nmark, pmark = await archiveClient.search_message_text(search_query,
			                                                                 bookmark=page, 
			                                                                 page_size=limit,
			                                                                 ascending=asc, 
			                                                                 topics_public=topics_public,
			                                                                 topics_full=topics_full,
			                                                                 start_time=start_time,
			                                                                 end_time=end_time,
			                                                                 include_retracted=include_retracted)
	except Exception as err:
		logging.error(f"Error fetching message records from database: {err}")
		# Note: error might be a bad request, e.g. due to invalid bookmark data
		return Response(status_code=500, content="Internal Error", headers=resp_headers)

	request_end=time.time()
	print(f"Request took {request_end-request_start} seconds")
	if meta:
		# if the client wants only the metadata, we can just return it directly
		return Response(content=bson.dumps({"messages":[transform_db_record(m) for m in mrecords], 
		                                    "next":nmark, "prev":pmark}),
		                headers=resp_headers)
	
	return StreamingResponse(stream_message_list(archiveClient, mrecords, next_mark=nmark, prev_mark=pmark),
	                         headers=resp_headers)

messageCountSchema = {
	"type": "object",
	"properties": {
		"count": {
			"type": "integer",
			"description": "The number of matching messages"
		},
	},
}

@app.get("/messages/count",
         description="Get a count of messages matching particular criteria",
         response_class=Response,
         responses={
             200: {
                "description": "The count of messages. Note that the format is BSON.",
                 "content": {
                     "application/bson": {
                         "schema": messageCountSchema
                     }
                 }
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed request parameter.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to read some requested data.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"],
                         }
                     }
                 }
             },
             500: {
                 "description": "Internal error.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string"
                         }
                     }
                 }
             },
         }
         )
async def count_messages(request: Request,
                         topic: Annotated[list[str], Query(description="The topic(s) from which to count messages. If not specified, the count will be across all accessible topics. Note that this parameter can be specified repeatedly to select multiple topics.")]=[],
                         start_time: Annotated[Optional[int], Query(description="Timestamp for start of time range. Kafka times are represented as milliseconds since the unix epoch. Leave unspecified for no constraint.")]=None,
                         end_time: Annotated[Optional[int], Query(description="Timestamp for end of time range. Kafka times are represented as milliseconds since the unix epoch. Leave unspecified for no constraint.")]=None,
                         search_query: Annotated[Optional[str], Query(description="If specified, a general text-search query to select messages")]=None,
                         include_retracted: Annotated[bool, Query(description="Include retracted messages in results")]=True,
                         authorization: Annotated[Optional[str], Header(description="Any authentication accesed by scimma-admin, including RFC 7804 SCRAM authentication and REST tokens. If not specified, only public results will be returned.")] = None):
	resp_headers: dict[str,str]={}
	required_resp, user_id, topics_public, topics_full = \
		await authorize_multi_topic_access(authorization, resp_headers, topic)
	logging.info(f"COUNT_FROM_TOPICS request by {user_id} from {request.client.host} on topics {topic}")
	if required_resp is not None:
		return required_resp
	
	try:
		if search_query is None:
			n = await archiveClient.count_message_records(topics_public=topics_public,
			                                              topics_full=topics_full,
			                                              start_time=start_time,
			                                              end_time=end_time,
			                                              include_retracted=include_retracted)
		else:
			n = await archiveClient.count_text_search_results(search_query,
			                                                  topics_public=topics_public,
			                                                  topics_full=topics_full,
			                                                  start_time=start_time,
			                                                  end_time=end_time,
			                                                  include_retracted=include_retracted)
	except Exception as err:
		logging.error(f"Error counting message records in database: {err}")
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	
	resp_headers["content-type"] = "application/bson"
	return Response(content=bson.dumps({"count":n}), headers=resp_headers)


@app.put("/msg/{msg_id}/retraction",
         description="Set a message's retraction status",
         response_class=Response,
         responses={
             200: {
                 "description": "Success",
             },
             400: {
                 "description": "Bad request. This may be caused by an ill-formed message ID.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Invalid Message ID"]
                         }
                     }
                 }
             },
             401: {
                 "description": "Authentication required.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Authentication required"]
                         }
                     }
                 }
             },
             403: {
                 "description": "Not authorized. The authenticated user does not have access to the requested message.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Operation not permitted"]
                         }
                     }
                 }
             },
             404: {
                 "description": "Message not found. There is no archived message with the specified ID.",
                 "content": {
                     "text/plain": {
                         "schema": {
                             "type": "string",
                             "examples": ["Message not found"]
                         }
                     }
                 }
             },
         })
async def retract_message(request: Request,
                          msg_id: Annotated[str, Path(description="The ID of the target message", 
                                                      json_schema_extra={"type": "string", "format": "uuid", "examples": ["457E145B-9F72-416F-87CA-73F0E188183D"]})],
                          retracted: Annotated[bool, Query(description="Whether the message should be considered retracted")],
                          authorization: Annotated[Union[str, None], Header(description="RFC 7804 SCRAM authentication")] = None,
                          ):
	resp_headers: dict[str,str]={}
	
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
	
	# Query hopauth to find out if the user is allowed to manipulate this message.
	# This requires authenticating the user to hopauth.
	if authorization is None:
		return authentication_required();
	
	path_root=urlparse(config['hopauth_api_url']).path
	topic_name = effective_topic_name_for_access(metadata.topic)
	resp = await httpClient.post(config['hopauth_api_url']+"/v1/multi",
							 json={
							   "roles":{
								 "method":"get",
								 "path":f"{path_root}/v1/current_user/memberships",
								 "headers":{"Authorization": "Inherit"},
							   },
							   "topic":{
								 "method":"get",
								 "path":f"{path_root}/v1/topics/{topic_name}",
								 "headers":{"Authorization": "Inherit"},
							   },
							   "whoami":{
								 "method":"get",
								 "path":f"{path_root}/v1/current_user",
								 "headers":{"Authorization": "Inherit"},
							   },
							 },
							 headers={"Authorization": authorization}
							 )
	
	if resp.status_code == 401 and "www-authenticate" in resp.headers:
		return Response(status_code=401, content=resp.content, 
						headers={"www-authenticate": resp.headers["www-authenticate"]})
	if resp.status_code != 200:
		return Response(status_code=500, content="Internal Error")
	if "authentication-info" in resp.headers:
		resp_headers["authentication-info"] = resp.headers["authentication-info"]
	# After this point it is important to always return a response with resp_headers
	# as the client may be expecting the authentication-info!
	
	hop_json = resp.json()
	if hop_json["whoami"]["status"]==200 and "body" in hop_json["whoami"] \
		  and "username" in hop_json["whoami"]["body"] and "email" in hop_json["whoami"]["body"]:
			user_id = f"user {hop_json['whoami']['body']['username']} ({hop_json['whoami']['body']['email']})"
	
	logging.info(f"RETRACT_MESSAGE request by {user_id} from {request.client.host}")
	
	# figure out which group owns the topic
	if hop_json["topic"]["status"]==200 and "body" in hop_json["topic"] \
		  and "owning_group" in hop_json["topic"]["body"]:
		owning_group = hop_json["topic"]["body"]["owning_group"]
	else:
		if hop_json["topic"]["status"]!=200:
			return Response(status_code=hop_json["topic"]["status"],
			                content="Operation not permitted" if hop_json["topic"]["status"]<500 else "Internal Error",
			                headers=resp_headers)
		else:
			return Response(status_code=500, content="Internal Error", headers=resp_headers)
	
	# figure out whether the user has authority within the owning group
	is_owner = False
	if hop_json["roles"]["status"]!=200 or "body" not in hop_json["roles"] \
		  or not isinstance(hop_json["roles"]["body"], collections.abc.Sequence):
		return Response(status_code=500, content="Internal Error", headers=resp_headers)
	for membership in hop_json["roles"]["body"]:
		if "group" in membership and membership["group"]==owning_group \
		        and "status" in membership and membership["status"]=="Owner":
		    is_owner = True
		    break
	
	if not is_owner:
		return Response(status_code=403, content="Operation not permitted", headers=resp_headers)
	
	await archiveClient.mark_message_retracted(msg_id=msg_id, retracted=retracted)
	return Response(status_code=200, content=bson.dumps({}), headers=resp_headers)