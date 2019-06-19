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
import numpy as np
import hashlib
import fnmatch
import shutil
from typing import Iterable
import uuid

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel 


class _RDDTable(object):
    def __init__(self, rdd=None, dtable=None, _partitions=1, schema={}, mode_instance=None):
        """
        """
        self.rdd = rdd
        self.dtable = dtable
        self._partitions = _partitions
        self.schema = schema
        self.mode_instance = mode_instance
        
        if dtable is not None and mode_instance is not None:            
            table_iterator = dtable.collect()
            self.rdd = self.mode_instance.sc.parallelize(table_iterator,
                                                    dtable._partitions)\
                                                    .persist(StorageLevel.MEMORY_AND_DISK)
            self.rdd.count()       
   
    def map(self, func):
        rtn_rdd = self.rdd.map(lambda x: func(x[0], x[1]), 
                               preservesPartitioning=True)\
                               .persist(StorageLevel.MEMORY_AND_DISK)     
        rtn_rdd.count() 
        return _RDDTable(rdd=rtn_rdd, 
                         _partitions=rtn_rdd.getNumPartitions(), 
                         mode_instance=self.mode_instance)
        
    def mapValues(self, func):
        rtn_rdd = self.rdd.mapValues(func).persist(StorageLevel.MEMORY_AND_DISK)   
        rtn_rdd.count()          
        return _RDDTable(rdd=rtn_rdd, 
                         _partitions=rtn_rdd.getNumPartitions(), 
                         mode_instance=self.mode_instance)
        
    def mapPartitions(self, func):
        rtn_rdd = self.rdd.mapPartitions(lambda x: [[str(uuid.uuid1()), func(x)]], 
                                         preservesPartitioning=True)\
                                         .persist(StorageLevel.MEMORY_AND_DISK)   
#         rtn_rdd = rtn_rdd.zipWithUniqueId()
#         rtn_rdd = rtn_rdd.map(lambda x: (x[1], x[0])).persist(StorageLevel.MEMORY_AND_DISK)    
        rtn_rdd.count() 
        return _RDDTable(rdd=rtn_rdd, 
                         _partitions=rtn_rdd.getNumPartitions(), 
                         mode_instance=self.mode_instance)
       
    def reduce(self, func):
        rtn = self.rdd.values()
        rtn = rtn.reduce(func)         
        return rtn
    
    def join(self, other, func=None):
        _partitions = max(self._partitions, other._partitions)
        rtn_rdd = self.rdd.join(other.rdd, numPartitions=_partitions) 
        if func is not None:
            rtn_rdd = rtn_rdd.mapValues(lambda x: func(x[0], x[1]))\
                             .persist(StorageLevel.MEMORY_AND_DISK)    
        rtn_rdd.count()    
        return _RDDTable(rdd=rtn_rdd,
                         _partitions=rtn_rdd.getNumPartitions(), 
                         mode_instance=self.mode_instance)
    
    def filter(self, func):
        rtn_rdd = self.rdd.filter(func).persist(StorageLevel.MEMORY_AND_DISK)  
        rtn_rdd.count()          
        return _RDDTable(rdd=rtn_rdd, 
                         _partitions=rtn_rdd.getNumPartitions(), 
                         mode_instance=self.mode_instance)
    
    def count(self):
        num = self.rdd.count()        
        return num
           
    def collect(self):
        rtn_iterator = self.rdd.toLocalIterator()        
        return rtn_iterator
    
    def save_as(self, name, namespace, partition=None, use_serialize=True, persistent=True):
        if partition is None:
            partition = self._partitions
        
        partition = min(partition, 50)
        dup = self.mode_instance.dtable(name, namespace, partition, 
                                        persistent=persistent, use_serialize=use_serialize)
        
        res = self.rdd.mapPartitions(lambda x: (1, dup.put_all(x)))
        res.count()
        
        return dup
    
    def put(self, k, v):
        if self.dtable is not None:
            return self.dtable.put(k, v)
        return False

    def delete(self, k):
        if self.dtable is not None:
            return self.dtable.delete(k)
        return None   
        
    def put_if_absent(self, k, v):
        if self.dtable is not None:
            return self.dtable.put_if_absent(k, v)
        return None
    
    def put_all(self, kv_list: Iterable, chunk_size=100000):
        if self.dtable is not None:
            return self.dtable.put_all(kv_list, chunk_size)
        return None
        
    def get(self, k):
        if self.dtable is not None:
            return self.dtable.get(k)
        
        return None            
        
    def destroy(self):
        if self.dtable is not None:
            self.dtable.destroy()
            self.dtable = None
   