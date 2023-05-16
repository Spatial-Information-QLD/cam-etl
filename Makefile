run:
	OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES python main.py

download-driver:
	curl -o postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

clean:
	rm output/*.ttl

compound:
	python compound.py