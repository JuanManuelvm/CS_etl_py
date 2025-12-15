import pandas as pd


# --------- DIMENSIÓN FECHA ---------

def build_dimdate_from_header(salesorderheader: pd.DataFrame) -> pd.DataFrame:
    """
    contruimos una sola dimensión de fechas (dimdate).
    """
    # Unimos todas las columnas de fecha
    all_dates = pd.concat([
        salesorderheader["orderdate"],
        salesorderheader["duedate"],
        salesorderheader["shipdate"]
    ])

    date_series = pd.to_datetime(all_dates.dropna().unique())

    dimdate = pd.DataFrame()
    dimdate["date"] = date_series
    dimdate["datekey"] = dimdate["date"].dt.strftime("%Y%m%d").astype(int)
    dimdate["year"] = dimdate["date"].dt.year
    dimdate["month"] = dimdate["date"].dt.month
    dimdate["day"] = dimdate["date"].dt.day
    dimdate["quarter"] = dimdate["date"].dt.quarter
    dimdate["is_weekend"] = (dimdate["date"].dt.weekday >= 5).astype(int)
    dimdate = dimdate.sort_values("datekey").reset_index(drop=True)

    return dimdate


# --------- HELPER SURROGATE KEYS ---------

def add_surrogate_key(df: pd.DataFrame, key_name: str) -> pd.DataFrame:
    """
    Agrega una surrogate key incremental a un DataFrame de dimensión.
    """
    df = df.drop_duplicates().reset_index(drop=True).copy()
    df[key_name] = df.index + 1  # surrogate key empieza en 1
    # Ponemos la surrogate key como primera columna
    cols = [key_name] + [c for c in df.columns if c != key_name]
    return df[cols]


# --------- DIMENSIONES ---------

def build_dimensions(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Construye todas las DIMENSIONES a partir de los DataFrames crudos.
    """
    salesorderheader = raw["salesorderheader"]
    customer = raw["customer"].copy()
    person = raw["person"].copy()
    address = raw["address"].copy()
    businessentityaddress = raw["businessentityaddress"].copy()
    stateprovince = raw["stateprovince"].copy()
    store = raw["store"].copy()
    employee = raw["employee"].copy()
    emp_history = raw["emp_history"].copy()
    department = raw["department"].copy()
    salesperson = raw["salesperson"].copy()
    product = raw["product"].copy()
    subcategory = raw["subcategory"].copy()
    category = raw["category"].copy()
    specialoffer = raw["specialoffer"].copy()
    specialofferproduct = raw["specialofferproduct"].copy()
    salesterritory = raw["salesterritory"].copy()

    # --- FECHAS ---
    dimdate = build_dimdate_from_header(salesorderheader)

  
    # --- CUSTOMER (INTERNET) ---
    customer = customer.rename(columns={'personid': 'businessentityid'})
    customer = customer.drop(columns=['rowguid', 'modifieddate', 'accountnumber'])
    internet_customers = customer[customer["storeid"].isna()].copy()

    cust = (
        internet_customers
        .merge(person, on="businessentityid", how="left")
    )
    cust = cust.rename(columns={"territoryid": "territoryidCustomer"})
    cust = cust[['customerid', 'businessentityid', 'territoryidCustomer', 'firstname', 'lastname']]

    businessentityaddress = businessentityaddress[['businessentityid', 'addressid']]
    address = address[['addressid', 'addressline1', 'stateprovinceid', 'city']]
    stateprovince = stateprovince[['stateprovinceid', 'countryregioncode', 'territoryid']]

    cust = cust.merge(businessentityaddress, on="businessentityid", how="left")
    cust = cust[['customerid', 'businessentityid', 'firstname', 'lastname', 'addressid']]
    cust = cust.merge(address, on="addressid", how="left")
    cust = cust.merge(stateprovince, on="stateprovinceid", how= "left")

    dimcustomer = cust[[
        "customerid",
        "firstname",
        "lastname",
        "addressline1",
        "city",
        "stateprovinceid",
        "countryregioncode",
        "territoryid"
    ]]
    dimcustomer = add_surrogate_key(dimcustomer, "customer_key")

    # --- REVENDEDORES ---
    reseller = customer[customer["storeid"].notnull()].copy()
    storeRevendedoreid = store.rename(columns={'businessentityid': 'storeid'})

    reseller = reseller.merge(storeRevendedoreid, on="storeid", how="left")

    dimreseller = reseller[[
        "customerid",
        "storeid",
        "name",
        "businessentityid"
    ]]
    dimreseller = add_surrogate_key(dimreseller, "reseller_key")

    # --- EMPLEADO ---
    employeeMerge = employee.drop(columns={'rowguid', 'modifieddate'})
    dimemployee = employeeMerge.merge(emp_history, on="businessentityid", how="left")
    dimemployee = dimemployee.drop(columns={'modifieddate'})
    dimemployee = dimemployee[['businessentityid', 'jobtitle', 'loginid', 'departmentid']]

    departmentMerge = department[['departmentid', 'name', 'groupname']]
    dimemployee = dimemployee.merge(departmentMerge, on="departmentid", how="left")

    salespersonMerge = salesperson[['businessentityid', 'territoryid']]
    personMerge = person[['businessentityid', 'firstname', 'lastname']]

    dimemployee = dimemployee.merge(salespersonMerge, on="businessentityid", how="left")
    dimemployee = dimemployee.merge(personMerge, on="businessentityid", how="left")
    # businessentityid lo renombramos como clave de negocio del empleado
    dimemployee = dimemployee.rename(columns={"businessentityid": "employeekey", 'name': 'departmentname'})

    dimemployee = add_surrogate_key(dimemployee, "employee_key")

    # --- PRODUCTO ---
    subcategoryMerge = subcategory.rename(columns={'name': 'productsubcategoryname'})
    dimproduct = product.merge(subcategoryMerge, on="productsubcategoryid", how="left")

    categoryMerge = category.rename(columns={'name': 'productcategoryname'})
    dimproduct = dimproduct.merge(categoryMerge, on="productcategoryid", how="left")

    dimproduct = dimproduct[[
        "productid",
        "name",
        "color",
        "size",
        "productsubcategoryid",
        "productsubcategoryname",
        "productcategoryid",
        "productcategoryname",
        "listprice"
    ]]
    dimproduct = add_surrogate_key(dimproduct, "product_key")

    # --- PROMOCIÓN ---
    dimpromotion = (
        specialoffer
        .merge(specialofferproduct, on="specialofferid", how="left")
    )[[
        "specialofferid",
        "description",
        "discountpct",
        "startdate",
        "enddate",
        "productid",
        "category",
        "type",
        "minqty",
        "maxqty"
    ]].drop_duplicates()

    dimpromotion = add_surrogate_key(dimpromotion, "promotion_key")

    # --- SALES TERRITORY ---
    dimsalesterritory = salesterritory[[
        "territoryid",
        "name",
        "countryregioncode",
        "group"
    ]]
    dimsalesterritory = add_surrogate_key(dimsalesterritory, "territory_key")

    dimensions = {
        "dimdate": dimdate,
        "dimcustomer": dimcustomer,
        "dimreseller": dimreseller,
        "dimemployee": dimemployee,
        "dimproduct": dimproduct,
        "dimpromotion": dimpromotion,
        "dimsalesterritory": dimsalesterritory,
    }

    return dimensions


# --------- HECHOS ---------

def build_facts(raw: dict[str, pd.DataFrame],
                dims: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Construimos las tablas de HECHO para:
    - Ventas por internet
    - Ventas por revendedores
    Usando surrogate keys de las dimensiones.
    """
    salesorderheader = raw["salesorderheader"].copy()
    salesorderdetail = raw["salesorderdetail"].copy()
    customer = raw["customer"].copy()
    salesperson = raw["salesperson"].copy()

    dimproduct = dims["dimproduct"]
    dimcustomer = dims["dimcustomer"]
    dimreseller = dims["dimreseller"]
    dimemployee = dims["dimemployee"]
    dimsalesterritory = dims["dimsalesterritory"]
    dimpromotion = dims["dimpromotion"]  

    # ---- DataFrames auxiliares solo con claves----
    
    # Producto: 1 fila por productid
    dimproduct_keys = (
        dimproduct[["productid", "product_key"]]
        .drop_duplicates(subset="productid")
    )

    # Cliente: 1 fila por customerid
    dimcustomer_keys = (
        dimcustomer[["customerid", "customer_key"]]
        .drop_duplicates(subset="customerid")
    )

    # Reseller: 1 fila por storeid
    dimreseller_keys = (
        dimreseller[["storeid", "reseller_key"]]
        .drop_duplicates(subset="storeid")
    )

    # Empleado: 1 fila por employeekey (businessentityid)
    dimemployee_keys = (
        dimemployee[["employeekey", "employee_key"]]
        .drop_duplicates(subset="employeekey")
    )

    # Territorio: 1 fila por territoryid
    dimterritory_keys = (
        dimsalesterritory[["territoryid", "territory_key"]]
        .drop_duplicates(subset="territoryid")
    )

    # Promoción: 1 fila por combinación (specialofferid, productid)
    dimpromotion_keys = (
        dimpromotion[["specialofferid", "productid", "promotion_key"]]
        .drop_duplicates(subset=["specialofferid", "productid"])
    )


    # ---------- FACT VENTAS POR INTERNET ----------
    
    internet_sales = salesorderheader[salesorderheader["onlineorderflag"] == True].copy()
   
    factInternet = internet_sales.merge(salesorderdetail, on="salesorderid")
    
    factInternet = factInternet.drop_duplicates(subset=["salesorderdetailid"]).copy()
  
    # Producto (productid -> product_key)
    factInternet = factInternet.merge(dimproduct_keys, on="productid", how="left")

    # Cliente (customerid -> customer_key)
    factInternet = factInternet.merge(dimcustomer_keys, on="customerid", how="left")

    # Territorio (territoryid -> territory_key)
    factInternet = factInternet.merge(dimterritory_keys, on="territoryid", how="left")
    #promotions
    factInternet = factInternet.merge(
        dimpromotion_keys,
        on=["specialofferid", "productid"],
        how="left"
    )
    # Claves de fecha
    factInternet["orderdatekey"] = pd.to_datetime(factInternet["orderdate"]).dt.strftime("%Y%m%d").astype(int)
    factInternet["duedatekey"] = pd.to_datetime(factInternet["duedate"]).dt.strftime("%Y%m%d").astype(int)
    factInternet["shipdatekey"] = pd.to_datetime(factInternet["shipdate"]).dt.strftime("%Y%m%d").astype(int)

    factInternet = factInternet[[
        "salesorderid",
        "salesorderdetailid",
        "product_key",
        "customer_key",
        "territory_key",
        "promotion_key",
        "orderqty",
        "unitprice",
        "unitpricediscount",
        "freight",
        "taxamt",
        "totaldue",
        "orderdatekey",
        "duedatekey",
        "shipdatekey"
    ]]

    # ---------- FACT VENTAS POR REVENDEDORES ----------
    customerMergeRevendedores = customer[['customerid', 'storeid']].copy()
    salespersonCopiaTerritory = salesperson[['territoryid', 'businessentityid']].copy()

    reseller_sales = salesorderheader[salesorderheader["onlineorderflag"] == False].copy()
    reseller_sales = reseller_sales.merge(customerMergeRevendedores, on="customerid", how="left")
    reseller_sales = reseller_sales.merge(salespersonCopiaTerritory, on="territoryid", how="left")
    reseller_sales = reseller_sales[reseller_sales["storeid"].notnull()]

    # Header + detail
    factReseller = reseller_sales.merge(salesorderdetail, on="salesorderid")

    factReseller = factReseller.drop_duplicates(subset=["salesorderdetailid"]).copy()


    # Producto
    factReseller = factReseller.merge(dimproduct_keys, on="productid", how="left")

    # Empleado 
    factReseller = factReseller.merge(
        dimemployee_keys,
        left_on="businessentityid",
        right_on="employeekey",
        how="left"
    )

    # Reseller (storeid -> reseller_key)
    factReseller = factReseller.merge(dimreseller_keys, on="storeid", how="left")

    # Territorio
    factReseller = factReseller.merge(dimterritory_keys, on="territoryid", how="left")

    # Promoción
    factReseller = factReseller.merge(
        dimpromotion_keys,
        on=["specialofferid", "productid"],
        how="left"
    )


    # Claves de fecha
    factReseller["orderdatekey"] = pd.to_datetime(factReseller["orderdate"]).dt.strftime("%Y%m%d").astype(int)
    factReseller["duedatekey"] = pd.to_datetime(factReseller["duedate"]).dt.strftime("%Y%m%d").astype(int)
    factReseller["shipdatekey"] = pd.to_datetime(factReseller["shipdate"]).dt.strftime("%Y%m%d").astype(int)

    factReseller = factReseller[[
        "salesorderid",
        "salesorderdetailid",
        "product_key",
        "reseller_key",
        "employee_key",
        "territory_key",
        "promotion_key",   
        "orderqty",
        "unitprice",
        "unitpricediscount",
        "freight",
        "taxamt",
        "totaldue",
        "orderdatekey",
        "duedatekey",
        "shipdatekey"
    ]]

    facts = {
        "fact_internet_sales": factInternet,
        "fact_reseller_sales": factReseller
    }

    return facts
