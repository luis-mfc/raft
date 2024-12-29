FROM openjdk:11-jdk-slim

COPY . /app
WORKDIR /app/src

RUN javac *.java
