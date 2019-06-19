#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
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
import pickle as c_pickle
from arch.api import StoreType
from arch.api.utils import cloudpickle as f_pickle, cache_utils, file_utils
from arch.api.utils.core import string_to_bytes, bytes_to_string
from heapq import heapify, heappop, heapreplace
from typing import Iterable
import uuid
from concurrent.futures import ProcessPoolExecutor as Executor
import lmdb
from cachetools import cached
import numpy as np
from functools import partial
from operator import is_not
import hashlib
import fnmatch
import shutil
from arch.api import WorkMode
from arch.api.pyspark.rddtable import _RDDTable

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel 


class Standalone:
    __instance = None

    def __init__(self, job_id=None):
        self.data_dir = os.path.join(file_utils.get_project_base_directory(), 'data')
        self.job_id = str(uuid.uuid1()) if job_id is None else "{}".format(job_id)
        self.meta_table = _DTable('__META__', '__META__', 'fragments', 10)
                
        self.sc = SparkContext()
               
        Standalone.__instance = self        

    def table(self, name, namespace, partition=1, create_if_missing=True, error_if_exist=False, persistent=True, use_serialize=True):
        _dtable = self.dtable(name, namespace, partition, create_if_missing, error_if_exist, persistent, use_serialize)
        return _RDDTable(dtable=_dtable, mode_instance=self)
    
    def dtable(self, name, namespace, partition=1, create_if_missing=True, error_if_exist=False, persistent=True, use_serialize=True):
        __type = StoreType.LMDB.value if persistent else StoreType.IN_MEMORY.value
        _table_key = ".".join([__type, namespace, name])
        self.meta_table.put_if_absent(_table_key, partition)
        partition = self.meta_table.get(_table_key)
        _dtable = _DTable(__type, namespace, name, partition, use_serialize=use_serialize)
        
        return _dtable
    
    def parallelize(self, data: Iterable, include_key=False, name=None, partition=1, namespace=None,
                    create_if_missing=True,
                    error_if_exist=False,
                    persistent=False, chunk_size=100000):
        
        _iter = data if include_key else enumerate(data)
        
        if name is None:
            name = str(uuid.uuid1())
        if namespace is None:
            namespace = self.job_id
            
        #pyspark rdd;
        rdd = self.sc.parallelize(_iter, partition).persist(StorageLevel.MEMORY_AND_DISK)  
        rdd.count()
        rdd_inst = _RDDTable(rdd=rdd, _partitions=partition, mode_instance=self)
        
        return rdd_inst
    
    def cleanup(self, name, namespace, persistent):
        if not namespace or not name:
            raise ValueError("neither name nor namespace can be blank")

        _type = StoreType.LMDB.value if persistent else StoreType.IN_MEMORY.value
        _base_dir = os.sep.join([Standalone.get_instance().data_dir, _type])
        if not os.path.isdir(_base_dir):
            raise EnvironmentError("illegal datadir set for eggroll")

        _namespace_dir = os.sep.join([_base_dir, namespace])
        if not os.path.isdir(_namespace_dir):
            raise EnvironmentError("namespace does not exist")

        _tables_to_delete = fnmatch.filter(os.listdir(_namespace_dir), name)
        for table in _tables_to_delete:
            shutil.rmtree(os.sep.join([_namespace_dir, table]))
            
    @staticmethod
    def get_instance():
        if Standalone.__instance is None:
            raise EnvironmentError("eggroll should initialize before use")
        return Standalone.__instance


def serialize(_obj):
    return c_pickle.dumps(_obj)


def _evict(_, env):
    env.close()


@cached(cache=cache_utils.EvictLRUCache(maxsize=64, evict=_evict))
def _open_env(path, write=False):
    os.makedirs(path, exist_ok=True)
    return lmdb.open(path, create=True, max_dbs=1, max_readers=1024, lock=write, sync=True, map_size=10_737_418_240)


def _get_db_path(*args):
    data_dir = os.path.join(file_utils.get_project_base_directory(), 'data')
    return os.sep.join([data_dir, *args])


def _get_env(*args, write=False):
    _path = _get_db_path(*args)
    return _open_env(_path, write=write)


def _hash_key_to_partition(key, partitions):
    _key = hashlib.sha1(key).digest()
    if isinstance(_key, bytes):
        _key = int.from_bytes(_key, byteorder='little', signed=False)
    if partitions < 1:
        raise ValueError('partitions must be a positive number')
    b, j = -1, 0
    while j < partitions:
        b = int(j)
        _key = ((_key * 2862933555777941757) + 1) & 0xffffffffffffffff
        j = float(b + 1) * (float(1 << 31) / float((_key >> 33) + 1))
    return int(b)


class _DTable(object):
    def __init__(self, _type, namespace, name, partitions, use_serialize=True):
        self._type = _type
        self._namespace = namespace
        self._name = name
        self._partitions = partitions
        self.schema = {}
        self.use_serialize = use_serialize
        
    def __str__(self):
        return "type: {}, namespace: {}, name: {}, partitions: {}".format(self._type, self._namespace, self._name,
                                                                          self._partitions)

    def _get_env_for_partition(self, p: int, write=False):
        return _get_env(self._type, self._namespace, self._name, str(p), write=write)

    def kv_to_bytes(self, **kwargs):        
        # can not use is None
        if "k" in kwargs and "v" in kwargs:
            k, v = kwargs["k"], kwargs["v"]
            return (c_pickle.dumps(k), c_pickle.dumps(v)) if self.use_serialize \
                else (string_to_bytes(k), string_to_bytes(v))
        elif "k" in kwargs:
            k = kwargs["k"]
            return c_pickle.dumps(k) if self.use_serialize else string_to_bytes(k)
        elif "v" in kwargs:
            v = kwargs["v"]
            return c_pickle.dumps(v) if self.use_serialize else string_to_bytes(v)

    def put(self, k, v):
        k_bytes, v_bytes = self.kv_to_bytes(k=k, v=v)
        p = _hash_key_to_partition(k_bytes, self._partitions)
        env = self._get_env_for_partition(p, write=True)
        with env.begin(write=True) as txn:
            return txn.put(k_bytes, v_bytes)
        return False

    def count(self):
        cnt = 0
        for p in range(self._partitions):
            env = self._get_env_for_partition(p)
            cnt += env.stat()['entries']
        return cnt

    def delete(self, k):
        k_bytes = self.kv_to_bytes(k=k)
        p = _hash_key_to_partition(k_bytes, self._partitions)
        env = self._get_env_for_partition(p, write=True)
        with env.begin(write=True) as txn:
            old_value_bytes = txn.get(k_bytes)
            if txn.delete(k_bytes):
                return None if old_value_bytes is None else (c_pickle.loads(old_value_bytes) if self.use_serialize else old_value_bytes)
            return None

    def put_if_absent(self, k, v):
        k_bytes = self.kv_to_bytes(k=k)
        p = _hash_key_to_partition(k_bytes, self._partitions)
        env = self._get_env_for_partition(p, write=True)
        with env.begin(write=True) as txn:
            old_value_bytes = txn.get(k_bytes)
            if old_value_bytes is None:
                v_bytes = self.kv_to_bytes(v=v)
                txn.put(k_bytes, v_bytes)
                return None
            return c_pickle.loads(old_value_bytes) if self.use_serialize else old_value_bytes

    def put_all(self, kv_list: Iterable, chunk_size=100000):
        txn_map = {}
        _succ = True
        for p in range(self._partitions):
            env = self._get_env_for_partition(p, write=True)
            txn = env.begin(write=True)
            txn_map[p] = env, txn
        for k, v in kv_list:
            try:
                k_bytes, v_bytes = self.kv_to_bytes(k=k, v=v)
                p = _hash_key_to_partition(k_bytes, self._partitions)
                _succ = _succ and txn_map[p][1].put(k_bytes, v_bytes)
            except Exception as e:
                _succ = False
                break
        for p, (env, txn) in txn_map.items():
            txn.commit() if _succ else txn.abort()
        
        return _succ

    def get(self, k):
        k_bytes = self.kv_to_bytes(k=k)
        p = _hash_key_to_partition(k_bytes, self._partitions)
        env = self._get_env_for_partition(p)
        with env.begin(write=True) as txn:
            old_value_bytes = txn.get(k_bytes)
            return None if old_value_bytes is None else (c_pickle.loads(old_value_bytes) if self.use_serialize else old_value_bytes)

    def destroy(self):
        for p in range(self._partitions):
            env = self._get_env_for_partition(p, write=True)
            db = env.open_db()
            with env.begin(write=True) as txn:
                txn.drop(db)
        _table_key = ".".join([self._type, self._namespace, self._name])
        Standalone.get_instance().meta_table.delete(_table_key)
        _path = _get_db_path(self._type, self._namespace, self._name)
        import shutil
        shutil.rmtree(_path)

    def collect(self):
        iterators = []
        for p in range(self._partitions):
            env = self._get_env_for_partition(p)
            txn = env.begin()
            iterators.append(txn.cursor())
        return self._merge(iterators, self.use_serialize)

    def save_as(self, name, namespace, partition=None, use_serialize=True):
        if partition is None:
            partition = self._partitions
        dup = Standalone.get_instance().dtable(name, namespace, partition, persistent=True, use_serialize=use_serialize)
        dup.put_all(self.collect())
        
        return dup

    @staticmethod
    def _merge(cursors, use_serialize=True):
        ''' Merge sorted iterators. '''
        entries = []
        for _id, it in enumerate(cursors):
            if it.next():
                key, value = it.item()
                entries.append([key, value, _id, it])
            else:
                it.close()
        heapify(entries)
        while entries:
            key, value, _, it = entry = entries[0]
            if use_serialize:
                yield c_pickle.loads(key), c_pickle.loads(value)
            else:
                yield bytes_to_string(key), value
            if it.next():
                entry[0], entry[1] = it.item()
                heapreplace(entries, entry)
            else:
                _, _, _, it = heappop(entries)
                it.close()
