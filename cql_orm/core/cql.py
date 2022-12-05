from cql_orm.core.columns import PartitionKeyColumn, ClusteringKeyColumn, Ordering
from cql_orm.core.meta import UserDefinedType
from cassandra.cluster import Session
from cassandra.cqlengine.query import BatchQuery


class InitStatements:
    @staticmethod
    def __define_columns(table):
        partition_keys = []
        clustering_keys = []
        udts = []
        defs = ""
        for col_name, col in table.columns.items():
            if isinstance(col, PartitionKeyColumn):
                partition_keys.append(col_name)
            elif isinstance(col, ClusteringKeyColumn):
                clustering_keys.append(col_name)
            if isinstance(col.datatype, UserDefinedType):
                udts.append(col.datatype)
            t = col.datatype.typename()
            t = f"{col_name} {t} static" if col.static else f"{col_name} {t}"
            defs += (f", {t}" if defs else t)
        return defs, partition_keys, clustering_keys, udts

    @staticmethod
    def __define_primary_key(partition_keys, clustering_keys):
        p = ", ".join(partition_keys)
        p = f"({p})" if len(partition_keys) > 1 else p
        c = ", ".join(clustering_keys)
        return f"PRIMARY KEY ({p}, {c})" if c else f"PRIMARY KEY ({p})"

    @staticmethod
    def __define_ordering(table, clustering_keys):
        last_order = None
        last_col_name = None
        ordering = ""
        for col_name in clustering_keys:
            order = table.columns[col_name].ordering
            if last_order == Ordering.Undefined and order != Ordering.Undefined:
                err = f"""Column {col_name} of table {table.name} can't be used in clustering ordering because previous column {last_col_name} ordering is undefined. Add an ordering for {last_col_name} or swap the columns initialization order in {table.name}"""
                raise Exception(err)
            elif order != Ordering.Undefined:
                ordering += f", {col_name} {order.value}" if ordering else f"{col_name} {order.value}"
            last_order = order
            last_col_name = col_name
        return f" WITH CLUSTERING ORDER BY ({ordering})" if ordering else ""

    @staticmethod
    def __define_udts(keyspace, udts):
        unique_udts = {type(udt): udt for udt in udts}
        unique_udts = [unique_udts[k]
                       for k in UserDefinedType._types.keys() if k in unique_udts]
        statements = []
        for udt in unique_udts:
            statements.append(
                f"CREATE TYPE IF NOT EXISTS {keyspace}.{udt.definition()};")
        return statements

    @staticmethod
    def create_if_not_exists(keyspace: str, tables, session: Session):
        found_udts = []
        table_statements = []
        for table in tables:
            defs, part_keys, clust_keys, udts = InitStatements.__define_columns(
                table)
            found_udts.extend(udts)
            if not part_keys:
                raise Exception(
                    f"Table {table.name} doesn't have a partition key")
            pk = InitStatements.__define_primary_key(part_keys, clust_keys)
            ordering = InitStatements.__define_ordering(table, clust_keys)
            table_statements.append(
                f"CREATE TABLE IF NOT EXISTS {keyspace}.{table.name}({defs}) {pk}{ordering};")
        statements = InitStatements.__define_udts(keyspace, found_udts)
        statements.extend(table_statements)
        for statement in statements:
            session.execute(statement)
