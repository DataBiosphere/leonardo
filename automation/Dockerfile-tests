FROM hseeberger/scala-sbt

RUN mkdir /app

COPY src /app/src
COPY test.sh /app
COPY project /app/project
COPY build.sbt /app

WORKDIR /app

ENTRYPOINT ["bash", "test.sh"]