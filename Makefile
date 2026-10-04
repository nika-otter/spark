COMPOSE := docker compose
SUBMIT  := $(COMPOSE) exec spark-master /opt/spark/bin/spark-submit

.PHONY: up down status logs submit smoke-generate smoke history-check

up:            ## Start master, 3 workers and History Server
	$(COMPOSE) up -d
	@echo "Master UI:  http://localhost:8080"
	@echo "History UI: http://localhost:18080"

down:          ## Stop the cluster (named volumes with data and event logs are kept)
	$(COMPOSE) down

status:        ## Show containers and their resource usage
	$(COMPOSE) ps
	docker stats --no-stream spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-history

logs:          ## Follow cluster logs
	$(COMPOSE) logs -f

submit:        ## Submit a PySpark job: make submit JOB=jobs/x.py ARGS="..."
	$(SUBMIT) /opt/project/$(JOB) $(ARGS)

smoke-generate: ## Write the smoke-test tables (10M + 1M rows) as Parquet
	$(MAKE) submit JOB=jobs/smoke_generate.py

smoke:         ## Run the smoke join with AQE off and on
	$(MAKE) submit JOB=jobs/smoke_join.py ARGS="--aqe false"
	$(MAKE) submit JOB=jobs/smoke_join.py ARGS="--aqe true"

history-check: ## List applications known to the History Server
	@curl -s http://localhost:18080/api/v1/applications | python3 -m json.tool
