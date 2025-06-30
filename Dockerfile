FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

RUN mkdir -p /app/config

COPY target/coresh-2.1.jar /app/coresh-2.1.jar

EXPOSE 5600

ENTRYPOINT ["java", "-jar", "coresh-2.1.jar"]