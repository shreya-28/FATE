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
from federatedml.secureprotol.fixedpoint import FixedPointNumber
from federatedml.secureprotol.spdz import SPDZ

class SharedVariable:
    def __init__(self, secret=None, n_workers=2, role=None):
        en_secret = FixedPointNumber.encode(secret)
        SPDZ(role=role).share(en_secret, n_workers)        

    def __neg__(self):
        return self.neg()

    def __add__(self, other):
        return self.add(other)

    def __sub__(self, other):
        return self.sub(other)

    def __mul__(self, other):
        return self.mul(other)

    def __matmul__(self, other):
        return self.matmul(other)

    def sigmoid(self):
        return SharedVariable(
            SharedSigmoid.apply(self.var, self.interface), self.interface
        )

    def neg(self):
        return SharedVariable(SharedNeg.apply(self.var), self.interface)

    def add(self, other):
        return SharedVariable(
            SharedAdd.apply(self.var, other.var), self.interface, self.requires_grad
        )

    def sub(self, other):
        return SharedVariable(SharedSub.apply(self.var, other.var), self.interface)

    def mul(self, other):
        return SharedVariable(
            SharedMult.apply(self.var, other.var, self.interface), self.interface
        )

    def matmul(self, other):
        return SharedVariable(
            SharedMatmul.apply(self.var, other.var, self.interface), self.interface
        )

    @property
    def grad(self):
        return self.var.grad

    @property
    def data(self):
        return self.var.data

    def backward(self, grad):
        return self.var.backward(grad)

    def t_(self):
        self.var = self.var.t_()

    def __repr__(self):
        return self.var.__repr__()

    def type(self):
        return "SharedVariable"