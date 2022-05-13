# Market Data Technical Analysis

Technical Components
- Microservices
- Java (8 & 11)
- Spring Boot
- RESTful API
- Kafka
- MongoDB
- Docker

Microservice Components
- md-main: RESTful controller to provide entry point 

- md-config: Configuration Server 

- md-registry: Eureka Server

- md-file-parser: Module to convert data from Yahoo CSV/JSON files into JSON objects

- md-enricher: Service to add various kinds of technical indicators (configurable) to EOD data, consolidate them based on different criterions 

- md-retreiver: RESTful consumer to retrieve stock EOD prices, stock detail information from Yahoo Finance API 

- md-storage: Component to access MongoDB
