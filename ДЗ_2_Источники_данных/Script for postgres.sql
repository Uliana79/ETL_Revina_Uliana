create schema  if not exists extract_data_revina;

create table extract_data_revina.json_content (id serial primary key, json_data jsonb);
insert into extract_data_revina.json_content (id, json_data) values (
	1,
	'{
	  "pets": [
	    {
	      "name" : "Purrsloud",
	      "species" : "Cat",
	      "favFoods" : ["wet food", "dry food", "<strong>any</strong> food"],
	      "birthYear" : 2016,
	      "photo" : "https://learnwebcode.github.io/json-example/images/cat-2.jpg"
	    },
	    {
	      "name" : "Barksalot",
	      "species" : "Dog",
	      "birthYear" : 2008,
	      "photo" : "https://learnwebcode.github.io/json-example/images/dog-1.jpg"
	    },
	    {
	      "name" : "Meowsalot",
	      "species" : "Cat",
	      "favFoods" : ["tuna", "catnip", "celery"],
	      "birthYear" : 2012,
	      "photo" : "https://learnwebcode.github.io/json-example/images/cat-1.jpg"
	    }
	  ]
}'
);

create table extract_data_revina.xml_content (id serial primary key, xml_data xml);
insert into extract_data_revina.xml_content (id, xml_data) values (
	1,
	(select xmlparse(document convert_from(pg_read_binary_file('/data/nutrition.xml'), 'UTF8'))));


select * from extract_data_revina.data_from_json;
select * from extract_data_revina.data_from_xml;
