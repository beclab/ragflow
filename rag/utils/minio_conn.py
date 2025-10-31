#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import os
import logging
import time
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from io import BytesIO
from rag import settings
from rag.utils import singleton


@singleton
class RAGFlowMinio:
    def __init__(self):
        self.conn = None
        self.__open__()

    def _with_prefix(self, bucket: str) -> str:
        prefix = settings.MINIO_BUCKET_PREFIX or ""
        return f"{prefix}-{bucket}" if prefix else bucket

    def __open__(self):
        try:
            if self.conn:
                self.__close__()
        except Exception:
            pass

        try:
            self.conn = Minio(settings.MINIO["host"],
                              access_key=settings.MINIO["user"],
                              secret_key=settings.MINIO["password"],
                              secure=False
                              )
        except Exception:
            logging.exception(
                "Fail to connect %s " % settings.MINIO["host"])

    def __close__(self):
        del self.conn
        self.conn = None

    def health(self):
        bucket, fnm, binary = os.getenv('MINIO_HEALTH_BUCKET', ''), "txtxtxtxt1", b"_t@@@1"
        pbucket = self._with_prefix(bucket)

        if not self.conn.bucket_exists(pbucket):
            self.conn.make_bucket(pbucket)
        r = self.conn.put_object(pbucket, fnm,
                                 BytesIO(binary),
                                 len(binary)
                                 )
        return r

    def put(self, bucket, fnm, binary, tenant_id=None):
        for _ in range(3):
            try:
                pbucket = self._with_prefix(bucket)
                if not self.conn.bucket_exists(pbucket):
                    self.conn.make_bucket(pbucket)

                r = self.conn.put_object(pbucket, fnm,
                                         BytesIO(binary),
                                         len(binary)
                                         )
                return r
            except Exception:
                logging.exception(f"Fail to put {bucket}/{fnm}:")
                self.__open__()
                time.sleep(1)

    def rm(self, bucket, fnm, tenant_id=None):
        try:
            pbucket = self._with_prefix(bucket)
            self.conn.remove_object(pbucket, fnm)
        except Exception:
            logging.exception(f"Fail to remove {bucket}/{fnm}:")

    def get(self, bucket, filename, tenant_id=None):
        for _ in range(1):
            try:
                pbucket = self._with_prefix(bucket)
                r = self.conn.get_object(pbucket, filename)
                return r.read()
            except Exception:
                logging.exception(f"Fail to get {pbucket}/{filename}")
                self.__open__()
                time.sleep(1)
        return

    def obj_exist(self, bucket, filename, tenant_id=None):
        try:
            pbucket = self._with_prefix(bucket)
            if not self.conn.bucket_exists(pbucket):
                return False
            if self.conn.stat_object(pbucket, filename):
                return True
            else:
                return False
        except S3Error as e:
            if e.code in ["NoSuchKey", "NoSuchBucket", "ResourceNotFound"]:
                return False
        except Exception:
            logging.exception(f"obj_exist {pbucket}/{filename} got exception")
            return False

    def bucket_exists(self, bucket):
        try:
            pbucket = self._with_prefix(bucket)
            if not self.conn.bucket_exists(pbucket):
                return False
            else:
                return True
        except S3Error as e:
            if e.code in ["NoSuchKey", "NoSuchBucket", "ResourceNotFound"]:
                return False
        except Exception:
            logging.exception(f"bucket_exist {pbucket} got exception")
            return False

    def get_presigned_url(self, bucket, fnm, expires, tenant_id=None):
        for _ in range(10):
            try:
                pbucket = self._with_prefix(bucket)
                return self.conn.get_presigned_url("GET", pbucket, fnm, expires)
            except Exception:
                logging.exception(f"Fail to get_presigned {pbucket}/{fnm}:")
                self.__open__()
                time.sleep(1)
        return

    def remove_bucket(self, bucket):
        try:
            pbucket = self._with_prefix(bucket)
            if self.conn.bucket_exists(pbucket):
                objects_to_delete = self.conn.list_objects(pbucket, recursive=True)
                for obj in objects_to_delete:
                    self.conn.remove_object(pbucket, obj.object_name)
                self.conn.remove_bucket(pbucket)
        except Exception:
            logging.exception(f"Fail to remove bucket {pbucket}")

    def copy(self, src_bucket, src_path, dest_bucket, dest_path):
        try:
            psrc_bucket = self._with_prefix(src_bucket)
            pdest_bucket = self._with_prefix(dest_bucket)
            if not self.conn.bucket_exists(pdest_bucket):
                self.conn.make_bucket(pdest_bucket)

            try:
                self.conn.stat_object(psrc_bucket, src_path)
            except Exception as e:
                logging.exception(f"Source object not found: {psrc_bucket}/{src_path}, {e}")
                return False

            self.conn.copy_object(
                pdest_bucket,
                dest_path,
                CopySource(psrc_bucket, src_path),
            )
            return True

        except Exception:
            logging.exception(f"Fail to copy {src_bucket}/{src_path} -> {dest_bucket}/{dest_path}")
            return False

    def move(self, src_bucket, src_path, dest_bucket, dest_path):
        try:
            psrc_bucket = self._with_prefix(src_bucket)
            pdest_bucket = self._with_prefix(dest_bucket)
            if self.copy(psrc_bucket, src_path, pdest_bucket, dest_path):
                self.rm(psrc_bucket, src_path)
                return True
            else:
                logging.error(f"Copy failed, move aborted: {psrc_bucket}/{src_path}")
                return False
        except Exception:
            logging.exception(f"Fail to move {psrc_bucket}/{src_path} -> {pdest_bucket}/{dest_path}")
            return False
