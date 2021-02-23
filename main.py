#!/usr/bin/env python

import argparse
import hashlib
import mimetypes
import os

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings

mimetypes.init()

parser = argparse.ArgumentParser()
parser.add_argument("SOURCE", help="Source folder")
parser.add_argument("DEST", help="Blob storage destination")
parser.add_argument(
    "--sync", action="store_true", help="Remove items from the destination that are not present in the source"
)
parser.add_argument("--container", default="$web", help="Blob storage container")
args = parser.parse_args()

mimetypes.types_map[".map"] = "application/octet-stream"
mimetypes.types_map[".ttf"] = "application/octet-stream"

connection_string = os.environ["CONNECTION_STRING"]


def md5(data: bytes) -> bytes:
    """
    Calculate file's MD5 hash

    :param data: File contents
    :return: MD5 digest in bytes not hex form
    """
    md5hash = hashlib.md5()
    md5hash.update(data)
    return md5hash.digest()


with ContainerClient.from_connection_string(connection_string, args.container) as client:  # type: ContainerClient
    files_to_keep = set()
    dest = args.DEST.lstrip("/")

    # Walk over files in source directory tree
    for root, _, files in os.walk(args.SOURCE):
        for file in files:

            path = os.path.join(root, file)
            dest_path = os.path.join(dest, os.path.relpath(path, args.SOURCE))
            files_to_keep.add(dest_path)
            print(f"Uploading {path} ⭢ {dest_path}... ", end="", flush=True)

            with open(path, "rb") as fp:
                # Get blob client, may or may not exist in destination
                blob = client.get_blob_client(blob=dest_path)

                # Read all of file, yolo they shouldn't be too big
                file_data = fp.read()
                md5_hash = md5(file_data)

                # Opportunistically get MD5 from Azure Blob Storage, else empty
                try:
                    blob_settings = blob.get_blob_properties()
                    dest_md5 = blob_settings.content_settings.content_md5
                except ResourceNotFoundError:
                    dest_md5 = b""

                # Guess Content-Type as if its not set, Blob Storage Static Site will not set the Content-Type header
                # and the browser will download the page
                content_type, _ = mimetypes.guess_type(path)
                if not content_type:
                    content_type = "application/octet-stream"
                content_settings = ContentSettings(content_type=content_type)

                try:
                    # Upload if changed
                    if md5_hash != dest_md5:
                        blob.upload_blob(file_data, overwrite=True, content_settings=content_settings)
                        print("done")
                    else:
                        print("skipped")
                except Exception as err:
                    print(f"failed, error: {err}")

    if args.sync:
        # Go over files in the destination and delete any which shouldn't exist
        for file in client.list_blobs(name_starts_with=dest):
            if file.name not in files_to_keep:
                print(f"Deleting {file.name}... ", end="", flush=True)
                blob = client.get_blob_client(blob=file.name)
                try:
                    blob.delete_blob()
                    print("done")
                except Exception as err:
                    print(f"failed, error: {err}")
