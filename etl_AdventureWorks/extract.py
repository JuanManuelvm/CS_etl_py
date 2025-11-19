import pandas as pd
from sqlalchemy import create_engine
import yaml
from sqlalchemy.engine import Engine


CONFIG_PATH = 'C:/Users/WILSON/OneDrive/Escritorio/Septimo Semestre/CIENCIA DE DATOS/REPO_MONITOR/CS_etl_py/config.yml'  # Ajusta la ruta si es necesario


def get_engines() -> tuple[Engine, Engine]:
    """
    Crea y retorna los engines de conexión:
    - co_sa: origen (AdventureWorks2022)
    - etl_Adventure: destino (bodega / DW)
    """
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
        config_co = config['CO_SA']
        config_etl = config['ETL_PRO']

    url_co = (
        f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}"
        f"@{config_co['host']}:{config_co['port']}/{config_co['dbname']}"
    )

    url_etl = (
        f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}"
        f"@{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}"
    )

    co_sa = create_engine(url_co)
    etl_Adventure = create_engine(url_etl)

    return co_sa, etl_Adventure


def load_table(schema: str, table: str, engine: Engine) -> pd.DataFrame:
    """
    Carga una tabla completa desde un schema y la retorna como DataFrame.
    """
    query = f"SELECT * FROM {schema}.{table}"
    return pd.read_sql(query, engine)


def extract_raw_data(source_engine: Engine) -> dict[str, pd.DataFrame]:
    """
    Extrae todas las tablas necesarias desde AdventureWorks2022
    y las retorna en un diccionario.
    """
    raw = {}

    # Schema sales
    raw["salesorderheader"] = load_table("sales", "salesorderheader", source_engine)
    raw["salesorderdetail"] = load_table("sales", "salesorderdetail", source_engine)
    raw["customer"] = load_table("sales", "customer", source_engine)
    raw["personcreditcard"] = load_table("sales", "personcreditcard", source_engine)
    raw["salesperson"] = load_table("sales", "salesperson", source_engine)
    raw["store"] = load_table("sales", "store", source_engine)
    raw["specialoffer"] = load_table("sales", "specialoffer", source_engine)
    raw["specialofferproduct"] = load_table("sales", "specialofferproduct", source_engine)
    raw["salesterritory"] = load_table("sales", "salesterritory", source_engine)
    raw["currency"] = load_table("sales", "currency", source_engine)
    raw["currencyrate"] = load_table("sales", "currencyrate", source_engine)

    # Schema person
    raw["person"] = load_table("person", "person", source_engine)
    raw["address"] = load_table("person", "address", source_engine)
    raw["businessentity"] = load_table("person", "businessentity", source_engine)
    raw["businessentityaddress"] = load_table("person", "businessentityaddress", source_engine)
    raw["stateprovince"] = load_table("person", "stateprovince", source_engine)
    raw["countryregion"] = load_table("person", "countryregion", source_engine)

    # Schema production
    raw["product"] = load_table("production", "product", source_engine)
    raw["subcategory"] = load_table("production", "productsubcategory", source_engine)
    raw["category"] = load_table("production", "productcategory", source_engine)

    # Schema humanresources
    raw["employee"] = load_table("humanresources", "employee", source_engine)
    raw["emp_history"] = load_table("humanresources", "employeedepartmenthistory", source_engine)
    raw["department"] = load_table("humanresources", "department", source_engine)

    return raw
