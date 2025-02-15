/*
 * Copyright 2019 The FATE Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.ai.eggroll.core.retry.impl.predicate;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.webank.ai.eggroll.core.retry.AttemptContext;

import javax.annotation.Nullable;

public final class ExceptionClassPredicate<T> implements Predicate<AttemptContext<T>> {
    private Class<? extends Throwable> exceptionClass;

    public ExceptionClassPredicate(Class<? extends Throwable> exceptionClass) {
        Preconditions.checkNotNull(exceptionClass, "exception class cannot be null");
        this.exceptionClass = exceptionClass;
    }

    @Override
    public boolean apply(@Nullable AttemptContext<T> attemptContext) {
        if (!attemptContext.hasException()) {
            return false;
        }
        return exceptionClass.isAssignableFrom(attemptContext.getExceptionCause().getClass());
    }
}
