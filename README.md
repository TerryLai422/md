# Market Data Technical/Quantitative Analysis

Technical Components
- Microservices
- Java
- Spring Boot
- RESTful API
- Kafka
- MongoDB
- Docker

Microservice Components
- md-main: RESTful controller to provide entry point for the whole application
- md-file-parser: Component to convert data from CSV/JSON files into JSON objects
- md-enricher: Application to add various kinds of technical indicators to EOD data, consolidate them based on different criterions 
- md-retreiver: RESTful consumer to retrieve stock EOD prices, stock detail information from Yahoo API (via CSV or JSON payload)
- md-storage: Component to access MongoDB
