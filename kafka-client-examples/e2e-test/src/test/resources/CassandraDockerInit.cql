CREATE KEYSPACE IF NOT EXISTS responsive_itests
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };