.PHONY: up down logs test dbt-build backfill
up:
	docker compose up -d
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
test:
	cd producer && pytest
dbt-build:
	docker compose --profile tools run --rm dbt build
backfill:
	python scripts/backfill_weather.py
