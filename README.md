# Niomon Foundation Mock
Implementation of Niomon microservice runtime for use in tests of different microservices. Redis and Kafka are mocked,
which makes the tests fairly fast. For the actual implementation, see [foundation](../foundation/README.md).

# Development
The most interesting class is [NioMicroserviceMock](src/main/scala/com/ubirch/niomon/base/NioMicroserviceMock.scala).
Refer to ScalaDoc there.