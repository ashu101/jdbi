/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jdbi.v3.core.argument;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.jdbi.v3.core.argument.internal.StatementBinder;
import org.jdbi.v3.core.argument.internal.strategies.LoggableBinderArgument;
import org.jdbi.v3.core.config.ConfigRegistry;

import static org.jdbi.v3.core.generic.GenericTypes.getErasedType;

abstract class DelegatingArgumentFactory implements ArgumentFactory.Preparable {
    private final Map<Class<?>, Function<Object, Argument>> preparableBuilders = new IdentityHashMap<>();
    private final Map<Class<?>, BiFunction<Object, ConfigRegistry, Argument>> hotBuilders = new IdentityHashMap<>();

    @Override
    public Optional<Function<Object, Argument>> prepare(Type type, ConfigRegistry config) {
        return Optional.ofNullable(preparableBuilders.get(getErasedType(type)));
    }

    @Override
    public Optional<Argument> build(Type expectedType, Object value, ConfigRegistry config) {
        Class<?> expectedClass = getErasedType(expectedType);

        if (value != null && expectedClass == Object.class) {
            expectedClass = value.getClass();
        }

        Optional<Argument> prepared = Optional.ofNullable(preparableBuilders.get(expectedClass)).map(r -> r.apply(value));

        return prepared.isPresent()
            ? prepared
            : Optional.ofNullable(hotBuilders.get(expectedClass)).map(r -> r.apply(value, config));
    }

    @Override
    public Collection<? extends Type> prePreparedTypes() {
        return preparableBuilders.keySet();
    }

    @SuppressWarnings("unchecked")
    <T> void registerPreparable(Class<T> klass, int sqlType, StatementBinder<T> binder) {
        preparableBuilders.put(klass, value -> value == null ? new NullArgument(sqlType) : new LoggableBinderArgument<>((T) value, binder));
    }

    @SuppressWarnings("unchecked")
    <T> void registerHot(Class<T> klass, int sqlType, StatementBinder<T> binder) {
        hotBuilders.put(klass, (value, config) -> {
            if (value == null) {
                if (klass.isPrimitive() && !config.get(Arguments.class).isBindingNullToPrimitivesPermitted()) {
                    throw new IllegalArgumentException(String.format(
                        "binding null to a primitive %s is forbidden by configuration, declare a boxed type instead", klass.getSimpleName()
                    ));
                }

                return new NullArgument(sqlType);
            }

            return new LoggableBinderArgument<>((T) value, binder);
        });
    }
}
