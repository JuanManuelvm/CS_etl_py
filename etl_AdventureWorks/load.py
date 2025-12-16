import pandas as pd
from sqlalchemy.engine import Engine


def load_dataframe(df: pd.DataFrame,
                   table_name: str,
                   engine: Engine,
                   schema: str = "public",
                   if_exists: str = "replace") -> None:
    """
    Cargamos un DataFrame a la base destino usando to_sql.
    ----------------------------------
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False
    )


def load_dimensions(dimensions: dict[str, pd.DataFrame],
                    engine: Engine,
                    schema: str = "public") -> None:
    """
    Cargamoa todas las dimensiones al DW.
    """
    for name, df in dimensions.items():
        # opcional: asegurar minúsculas
        table_name = name.lower()
        load_dataframe(df, table_name, engine, schema=schema)


def load_facts(facts: dict[str, pd.DataFrame],
               engine: Engine,
               schema: str = "public") -> None:
    """
    Cargamoa todas las tablas de hechos al DW.
    """
    for name, df in facts.items():
        table_name = name.lower()
        load_dataframe(df, table_name, engine, schema=schema)
