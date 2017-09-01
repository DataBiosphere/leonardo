FROM openjdk:8

EXPOSE 8080
EXPOSE 5050

ENV GIT_HASH $GIT_HASH

RUN mkdir /leonardo
COPY ./leonardo*.jar /leonardo

# Add Leonardo as a service (it will start when the container starts)
CMD java $JAVA_OPTS -jar $(find /leonardo -name 'leonardo*.jar')