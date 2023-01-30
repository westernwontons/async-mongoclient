alias r := run

run:
	pipenv run app

db:
	docker compose -f docker-compose.dev.yml up -d