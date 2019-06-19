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
import random
import gmpy2

from arch.api import federation
from federatedml.secureprotol.fixedpoint import FixedPointNumber
from federatedml.secureprotol.encrypt import PaillierEncrypt
from federatedml.util.transfer_variable import SecretShareTransferVariable


class SPDZ(object):
    def __init__(self, role=None):
        self.Q = FixedPointNumber.Q
        self.transfer_variable = SecretShareTransferVariable()
        self.role = role
        self.work_role_list = ["guest", "host"]
        
    def __share_strategy(self, secret, n_workers=2):
        random_shares = [random.randrange(self.Q) for i in range(n_workers - 1)]    
        shares = []
        for i in range(n_workers):
            if i == 0:
                share = random_shares[i]
            elif i < n_workers - 1:
                share = (random_shares[i] - random_shares[i - 1]) % self.Q
            else:
                share = (secret - random_shares[i - 1]) % self.Q
            shares.append(share)
        
        return shares   
            
    def share(self, secret):
        """ role: guest, work 0
            role: host, work 1             
        """
        secret_shares = secret.mapValues(self.__share_strategy())
        secret_shares_li = []
        secret_shares_li.append(secret_shares.mapValues(lambda shares: shares[0]))
        secret_shares_li.append(secret_shares.mapValues(lambda shares: shares[1]))
        
        for i in range(2):
            work_role = self.work_role_list[i]
            if work_role == self.role:
                self.owner_share = secret_shares_li[i]
            else:
                federation.remote(obj=secret_shares_li[i],
                          name=self.transfer_variable.share.name,
                          tag=self.transfer_variable.generate_transferid(
                                            self.transfer_variable.share),
                          role=work_role,
                          idx=0)
    
    def __recv_share_from_works(self):
        shares = federation.get(name=self.transfer_variable.share.name,
                                 tag=self.transfer_variable.generate_transferid(self.transfer_variable.share),
                                 idx=0)
        
    def reconstruct(self, shares):        
        return sum(shares) % self.Q
    
    def truncate(self, x):
        if interface.get_party() == 0:
            return (x / BASE ** amount) % mod
        return (mod - ((mod - x) / BASE ** amount)) % mod

    def public_add(x, y, interface):
        if interface.get_party() == 0:
            return x + y
        elif interface.get_party() == 1:
            return x    
    
    def spdz_add(self, x, y):
        z = x + y
        return z % self.Q    
    
    def spdz_neg(self, x):
        return (self.Q - x) % self.Q    
    
    def spdz_mul(x, y, workers):
        if x.get_shape() != y.get_shape():
            raise ValueError("Shapes must be identical in order to multiply them")
        shape = x.get_shape()
        triple = generate_mul_triple_communication(shape, workers)
        a, b, c = triple
    
        d = (x - a) % mod
        e = (y - b) % mod
    
        delta = d.child.sum_get() % mod
        epsilon = e.child.sum_get() % mod
    
        epsilon_delta = epsilon * delta
    
        delta = delta.broadcast(workers)
        epsilon = epsilon.broadcast(workers)
    
        z = (c + (delta * b) % mod + (epsilon * a) % mod) % mod
    
        z.child.public_add_(epsilon_delta)
    
        return z


    def generate_mul_triple(self):
        pass
    