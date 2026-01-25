from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG('json_extract_revina', schedule_interval = timedelta(minutes=30),
          start_date = datetime(2025,1,1), 
          catchup = False, ) as dag:
    history = PostgresOperator(
        task_id = 'json_extract_revina',
        postgres_conn_id = 'demo_db', 
        sql="""
        create table if not exists extract_data_revina.data_from_json as 
        select 
            post.value->>'name' as name,
            post.value->>'species' as species,
            post.value->>'favFoods' as favFoods,
            post.value->>'birthYear' as birthYear,
            post.value->>'photo' as photo
        from extract_data_revina.json_content, jsonb_array_elements(json_data->'pets') as post(value);
        """)

with DAG('xml_extract_revina', schedule_interval = timedelta(minutes=30),
          start_date = datetime(2025,1,1), 
          catchup = False, ) as dag:
    history = PostgresOperator(
        task_id = 'xml_extract_revina',
        postgres_conn_id = 'demo_db', 
        sql="""
        create table if not exists extract_data_revina.data_from_xml as 
        with xml_extract as (
	        select xml_data from extract_data_revina.xml_content
	    )
        select 
            unnest(xpath('/nutrition/food/name/text()', xml_data))::text as name,
            unnest(xpath('/nutrition/food/mfr/text()', xml_data))::text as mfr,
            unnest(xpath('/nutrition/food/serving/text()', xml_data))::text || ' ' || unnest(xpath('/nutrition/food/serving/@units', xml_data))::text as serving,
            unnest(xpath('/nutrition/food/calories/@total', xml_data))::text::int as calories_total,
            unnest(xpath('/nutrition/food/calories/@fat', xml_data))::text::int as calories_fat,
            unnest(xpath('/nutrition/food/total-fat/text()', xml_data))::text::numeric(10,2) as total_fat,
            unnest(xpath('/nutrition/food/saturated-fat/text()', xml_data))::text::numeric(10,2) as saturated_fat,
            unnest(xpath('/nutrition/food/cholesterol/text()', xml_data))::text::numeric(10,2) as cholesterol,
            unnest(xpath('/nutrition/food/sodium/text()', xml_data))::text::numeric(10,2) as sodium,
            unnest(xpath('/nutrition/food/carb/text()', xml_data))::text::numeric(10,2) as carb,
            unnest(xpath('/nutrition/food/fiber/text()', xml_data))::text::numeric(10,2) as fiber,
            unnest(xpath('/nutrition/food/protein/text()', xml_data))::text::numeric(10,2) as protein,
            unnest(xpath('/nutrition/food/vitamins/a/text()', xml_data))::text::numeric(10,2) as vitamin_a,
            unnest(xpath('/nutrition/food/vitamins/c/text()', xml_data))::text::numeric(10,2) as vitamin_c,
            unnest(xpath('/nutrition/food/minerals/ca/text()', xml_data))::text::numeric(10,2) as minerals_ca,
            unnest(xpath('/nutrition/food/minerals/fe/text()', xml_data))::text::numeric(10,2) as minerals_fe
        from xml_extract;
        """)
    