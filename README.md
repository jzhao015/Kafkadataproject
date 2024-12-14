# Kafkadataproject
Create an EC2 Instance: Log in to your AWS Management Console and navigate to the EC2 service. Launch a new EC2 instance with your desired configuration, such as an Amazon Linux 2 or Ubuntu Server image.
Install Apache Kafka on the EC2 Instance:Connect to your EC2 instance using SSH. Download the latest Apache Kafka distribution by running the following command then Extract the downloaded kafka: wget https://downloads.apache.org/kafka/3.9.0 and tar -xzf kafka_-3.9.0.tgz
Install Java on the EC2 Instance: Install Java using the following command (for Amazon Linux 2): sudo yum install java-1.8.0-openjdk-devel. For other Linux distributions, use the appropriate package manager command to install Java.
Start the ZooKeeper service: bin/zookeeper-server-start.sh config/zookeeper.properties
Start the Kafka Server: bin/kafka-server-start.sh config/server.properties
Create Kafka Topics: Open a new terminal session and navigate to the Kafka installation directory .Create the necessary topics for your data streaming: bin/kafka-topics.sh --create --topic <topic-name> --bootstrap-server localhost:9092
