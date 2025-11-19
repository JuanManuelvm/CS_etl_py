from etlAdventure import extract,transform,load

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


if __name__ == "__main__":
    main()

