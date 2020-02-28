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

import java.sql.PreparedStatement;
import java.sql.Types;

class PrimitivesArgumentFactory extends DelegatingArgumentFactory {
    PrimitivesArgumentFactory() {
        registerPreparable(boolean.class, Types.BOOLEAN, PreparedStatement::setBoolean);
        registerPreparable(byte.class, Types.TINYINT, PreparedStatement::setByte);
        registerPreparable(char.class, Types.CHAR, new ToStringBinder<>(PreparedStatement::setString));
        registerPreparable(short.class, Types.SMALLINT, PreparedStatement::setShort);
        registerPreparable(int.class, Types.INTEGER, PreparedStatement::setInt);
        registerPreparable(long.class, Types.INTEGER, PreparedStatement::setLong);
        registerPreparable(float.class, Types.FLOAT, PreparedStatement::setFloat);
        registerPreparable(double.class, Types.DOUBLE, PreparedStatement::setDouble);
    }
}
