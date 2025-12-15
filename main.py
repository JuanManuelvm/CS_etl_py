from etl_AdventureWorks import extract,transform,load

def main():
    # 1. Conexiones
    source_engine, target_engine = extract.get_engines()

    # 2. EXTRACT
    raw_data = extract.extract_raw_data(source_engine)

    # 3. TRANSFORM
    dimensions = transform.build_dimensions(raw_data)
    facts = transform.build_facts(raw_data, dimensions)

    # 4. LOAD (DATAMARTS: internet y revendedores comparten dimensiones)
    load.load_dimensions(dimensions, target_engine, schema="public")
    load.load_facts(facts, target_engine, schema="public")
     # 5. Validación rápida
    print("---- VALIDACIÓN Filas ----")

    dimensions = transform.build_dimensions(raw_data)
    facts = transform.build_facts(raw_data, dimensions)
    suma = 0
    print("---- DIMENSIONES ----")
    for name, df in dimensions.items():
        suma+=len(df)
        print(name, len(df))
    suma2 = 0
    print("---- HECHOS ----")
    for name, df in facts.items():
        suma2+=len(df)
        print(name, len(df))
    print("Total filas:", suma+suma2)

if __name__ == "__main__":
    main()

