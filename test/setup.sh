sudo docker exec -it scylla1 cqlsh -e "create keyspace if not exists keyspace1 with replication={'class':'SimpleStrategy', 'replication_factor':3}"
sudo docker exec -it scylla1 cqlsh -e "create table if not exists keyspace1.person(id text primary key, name text)"
sudo docker exec -it scylla1 cqlsh -e "insert into keyspace1.person(id, name) VALUES ('1', 'person1')"
sudo docker exec -it scylla1 cqlsh -e "insert into keyspace1.person(id, name) VALUES ('2', 'person2')"
