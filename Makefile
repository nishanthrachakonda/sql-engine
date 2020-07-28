run: 
	@echo "Starting engine."
	python src/engine.py

docker_build:
	@echo "Build sql-engine in docker."
	docker build -t sql-engine:latest .

docker_run:
	@echo "Run sql-engine in docker."
	docker run -it sql-engine:latest

docker_clean:
	@echo "Clean docker images."
	docker rmi -f sql-engine:latest