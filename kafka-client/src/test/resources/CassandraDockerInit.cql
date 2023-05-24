CREATE KEYSPACE IF NOT EXISTS responsive_clients
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };