FROM docker-dev-artifactory.workday.com/library/python:3-alpine

LABEL MAINTAINER="goku.support@workday.com"

RUN apk add --no-cache gcc python3-dev musl-dev linux-headers bash
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --upgrade acryl-datahub pandas

WORKDIR /app
COPY ./data/* /app/

RUN adduser -D goku

USER goku

ENTRYPOINT ["/app/load.sh"]
